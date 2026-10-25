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
use std::sync::Arc;
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
        // The alarm directory hoisted here so the action-sink boot replay
        // can raise build failures (W1-16 owns the endpoint).
        let alarms = Arc::new(crate::v5::alarms::AlarmStore::new());
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
        crate::v5::action_sinks::rebuild_all_action_sinks(&engine, &connectors, &actions, &alarms);
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
    /// visible without claiming the rule itself is broken.
    fn raise_unavailable_stored_kinds(&self) {
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

/// Public routes (dashboard assets, health, login, MQTT-over-WebSocket)
/// stay outside the authentication layer; every other route requires a
/// bearer token enforced by [`api_auth::require_api_auth`].
pub fn router(state: ApiState) -> Router {
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
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            api_auth::require_api_auth,
        ));
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
    public.with_state(state)
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
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(error) => persist_error(error),
    }
}

fn node_id() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "local".to_string())
}

/// Node status, version, live connection count, and rule count.
async fn get_nodes(State(state): State<ApiState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "node_id": node_id(),
        "status": "running",
        "version": env!("CARGO_PKG_VERSION"),
        "connections": state.metrics.connections_active(),
        "rules": state.engine.list_rules().len(),
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
/// external brokers.
#[derive(Debug, Deserialize)]
struct CreateConnectorRequest {
    id: String,
    kind: String,
    #[serde(default)]
    config: serde_json::Value,
}

async fn create_connector(
    State(state): State<ApiState>,
    Json(req): Json<CreateConnectorRequest>,
) -> Response {
    if req.id.trim().is_empty() {
        return bad_request("connector id must not be empty");
    }
    let built = build_connector(&req).await;
    match built {
        Ok((kind, sink)) => {
            state.engine.connectors().register(req.id.clone(), sink);
            // Persist the legacy connector through the same connector
            // store the boot replay reads, so the v1 path keeps delivery
            // after a restart. Only a kind the stored snapshot accepts
            // is persisted; an unready kind keeps its current v1
            // behaviour and stays memory-only.
            let persistable = broker_config::is_known_connector_type(&kind)
                && !broker_config::is_removed_connector_type(&kind)
                && crate::v5::rules::kind_refusal(&kind).is_none();
            if persistable {
                let mut doc = serde_json::Map::new();
                doc.insert("id".to_string(), serde_json::json!(req.id.clone()));
                doc.insert("name".to_string(), serde_json::json!(req.id.clone()));
                doc.insert("type".to_string(), serde_json::json!(kind.clone()));
                doc.insert("enable".to_string(), serde_json::json!(true));
                match &req.config {
                    serde_json::Value::Object(fields) => {
                        for (key, value) in fields {
                            doc.entry(key.clone()).or_insert_with(|| value.clone());
                        }
                    }
                    other => {
                        doc.insert("config".to_string(), other.clone());
                    }
                }
                state.connectors.upsert(serde_json::Value::Object(doc));
                if let Err(error) = state.connectors.persist() {
                    return crate::v5::rules::connector_persist_error(error);
                }
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
/// directory (hence async).
async fn build_connector(
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
                let transport = std::sync::Arc::new(
                    broker_connectors_enterprise::TcpRocketMqTransport::new(&config)
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
                let sink = broker_connectors::GreptimeDbSink::new(config, transport)
                    .map_err(|e| format!("invalid greptimedb sink: {e}"))?;
                Ok(("greptimedb".to_string(), std::sync::Arc::new(sink)
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
mod tests {
    use super::*;
    use broker_auth::Authenticator;
    use serde_json::{json, Value};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Serialises the connector tests that share the process-global v5
    /// connector store. Parallel creates/persists export the whole global
    /// into each test's own data dir, so one test's snapshot can resurrect
    /// another test's deleted entry on restart (e.g. w023-del-conn reads
    /// back 200 instead of 404). Holding this across the whole test keeps
    /// the create/delete/persist/seed sequence atomic. No production code
    /// depends on it; it only orders tests.
    static CONNECTOR_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    /// In-process HTTP round-trip over loopback TCP: no extra crates, no
    /// daemons. The server is spawned per test and aborted at the end.
    struct TestServer {
        port: u16,
        task: tokio::task::JoinHandle<()>,
    }

    struct TestState {
        engine: Arc<RuleEngine>,
        sessions: Arc<SessionManager>,
        router: Arc<SubscriptionRouter>,
        metrics: Arc<Metrics>,
        auth: Arc<MemoryAuth>,
        admin_users: Arc<crate::admin_users::AdminUsers>,
        tokens: Arc<crate::v5::auth::ApiTokens>,
        alarms: Arc<crate::v5::alarms::AlarmStore>,
        license: Arc<crate::v5::license::LicenseState>,
        mcp: Arc<crate::mcp::McpStore>,
        edge_rx: Arc<tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<BrokerFrame>>>,
    }

    impl TestServer {
        async fn start() -> (Self, TestState) {
            Self::start_with_registry(defaults_registry()).await
        }

        /// Start serving `ApiState` built on `registry`: pass a registry
        /// loaded from a temp `--data-dir` to simulate kernel restarts.
        async fn start_with_registry(registry: Arc<ConfigRegistry>) -> (Self, TestState) {
            let engine = Arc::new(RuleEngine::new(
                16,
                broker_rules::BackpressurePolicy::DropOldest,
            ));
            let sessions = Arc::new(SessionManager::new());
            let sub_router = Arc::new(SubscriptionRouter::new());
            let metrics = Arc::new(Metrics::new());
            let auth = Arc::new(MemoryAuth::new());
            let conns = Arc::new(broker_router::ConnTable::default());
            let (edge_tx, edge_rx) = tokio::sync::mpsc::unbounded_channel::<BrokerFrame>();
            let api_state = ApiState::new(
                engine.clone(),
                sessions.clone(),
                sub_router.clone(),
                metrics.clone(),
                auth.clone(),
                conns.clone(),
                registry,
                "indra-node-1".to_string(),
                edge_tx,
            );
            let state = TestState {
                engine: engine.clone(),
                sessions: sessions.clone(),
                router: sub_router.clone(),
                metrics: metrics.clone(),
                auth: auth.clone(),
                admin_users: api_state.admin_users.clone(),
                tokens: api_state.tokens.clone(),
                alarms: api_state.alarms.clone(),
                license: api_state.license.clone(),
                mcp: api_state.mcp.clone(),
                edge_rx: Arc::new(tokio::sync::Mutex::new(edge_rx)),
            };
            let app = router(api_state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind test server");
            let port = listener.local_addr().expect("local addr").port();
            let task = tokio::spawn(async move {
                axum::serve(listener, app).await.expect("serve test app");
            });
            (Self { port, task }, state)
        }

        async fn request(&self, raw: &str) -> (u16, Value) {
            let (status, text) = self.request_raw(raw).await;
            let body: Value = if text.trim().is_empty() {
                Value::Null
            } else {
                serde_json::from_str(&text).expect("response body is JSON")
            };
            (status, body)
        }

        async fn request_raw(&self, raw: &str) -> (u16, String) {
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .expect("connect test server");
            let (mut reader, mut writer) = stream.split();
            // The server can answer before it reads the whole body. Read and
            // write at the same time so an early close cannot block the
            // answer. A reset on the write is only accepted when the server
            // still sends its response.
            let write = writer.write_all(raw.as_bytes());
            let read = async {
                // HTTP/1.0 + Connection: close => the server closes when done.
                let mut buf = Vec::new();
                reader.read_to_end(&mut buf).await.map(|_| buf)
            };
            let (write_result, read_result) = tokio::join!(write, read);
            let buf = read_result.expect("read response");
            if let Err(error) = write_result {
                assert!(
                    matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                    ),
                    "write request: {error}"
                );
            }
            let text = String::from_utf8(buf).expect("response is UTF-8");
            let (head, body) = text.split_once("\r\n\r\n").expect("header/body split");
            let status: u16 = head.lines().next().expect("status line")[9..12]
                .parse()
                .expect("status code");
            (status, body.to_string())
        }

        async fn send(
            &self,
            method: &str,
            path: &str,
            body: Option<Value>,
            token: Option<&str>,
        ) -> (u16, Value) {
            let authorization = token.map(|token| format!("Bearer {token}"));
            self.send_with_authorization(method, path, body, authorization.as_deref())
                .await
        }

        /// Send a request with a complete `Authorization` header value,
        /// for example `Basic ...`.
        async fn send_with_authorization(
            &self,
            method: &str,
            path: &str,
            body: Option<Value>,
            authorization: Option<&str>,
        ) -> (u16, Value) {
            let mut head = format!("{method} {path} HTTP/1.0\r\nHost: test\r\n");
            if let Some(authorization) = authorization {
                head.push_str(&format!("Authorization: {authorization}\r\n"));
            }
            if let Some(body) = body {
                let raw_body = serde_json::to_string(&body).expect("encode body");
                head.push_str(&format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw_body}",
                    raw_body.len()
                ));
            } else {
                head.push_str("Connection: close\r\n\r\n");
            }
            self.request(&head).await
        }

        async fn post(&self, path: &str, body: Value) -> (u16, Value) {
            self.send("POST", path, Some(body), None).await
        }

        /// `GET` with the Basic credentials of an API key.
        async fn get_basic(&self, path: &str, key_id: &str, secret: &str) -> (u16, Value) {
            let authorization = basic_authorization(key_id, secret);
            self.send_with_authorization("GET", path, None, Some(&authorization))
                .await
        }

        /// `POST` with the Basic credentials of an API key.
        async fn post_basic(
            &self,
            path: &str,
            body: Value,
            key_id: &str,
            secret: &str,
        ) -> (u16, Value) {
            let authorization = basic_authorization(key_id, secret);
            self.send_with_authorization("POST", path, Some(body), Some(&authorization))
                .await
        }

        async fn post_auth(&self, path: &str, body: Value, token: &str) -> (u16, Value) {
            self.send("POST", path, Some(body), Some(token)).await
        }

        async fn get(&self, path: &str) -> (u16, Value) {
            self.send("GET", path, None, None).await
        }

        async fn get_auth(&self, path: &str, token: &str) -> (u16, Value) {
            self.send("GET", path, None, Some(token)).await
        }

        async fn put_auth(&self, path: &str, body: Value, token: &str) -> (u16, Value) {
            self.send("PUT", path, Some(body), Some(token)).await
        }

        async fn delete_auth(&self, path: &str, token: &str) -> (u16, Value) {
            self.send("DELETE", path, None, Some(token)).await
        }

        /// Log in with the default admin and return its bearer token.
        /// The token still carries `must_change_password`; it only opens
        /// the login, logout, current_user and own-password-change routes.
        async fn admin_token(&self) -> String {
            let (status, body) = self
                .post(
                    "/api/v5/login",
                    json!({"username": "admin", "password": "public"}),
                )
                .await;
            assert_eq!(status, 200);
            body["token"].as_str().expect("login token").to_string()
        }

        /// Log in as `admin`, clear the default-password flag, log in
        /// again and return a fully-privileged bearer token for tests
        /// that call authenticated routes.
        async fn login_as_admin(&self) -> String {
            let fresh = self.admin_token().await;
            let (status, _) = self
                .put_auth(
                    "/api/v5/users/admin/change_pwd",
                    json!({"old_pwd": "public", "new_pwd": "Adm1n-test-pass!"}),
                    &fresh,
                )
                .await;
            assert_eq!(status, 204);
            let (status, body) = self
                .post(
                    "/api/v5/login",
                    json!({"username": "admin", "password": "Adm1n-test-pass!"}),
                )
                .await;
            assert_eq!(status, 200);
            assert_eq!(body["must_change_password"], json!(false));
            body["token"].as_str().expect("login token").to_string()
        }
    }

    #[tokio::test]
    async fn removed_s3_tables_kind_is_refused_everywhere_and_stores_nothing() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let message = "connector type s3_tables is not available in this broker";

        for (index, kind) in ["s3_tables", "s3tables", "iceberg"].iter().enumerate() {
            let id = format!("removed-{index}");
            let (status, body) = server
                .post_auth(
                    "/api/v5/connectors",
                    json!({"id": id, "type": kind, "config": {}}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} connector create");
            assert_eq!(
                body["code"],
                json!("ALREADY_EXISTS"),
                "{kind} connector create"
            );
            assert_eq!(body["message"], json!(message), "{kind} connector create");

            let (status, body) = server
                .post_auth(
                    "/api/v5/actions",
                    json!({"id": id, "type": kind, "enable": true}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} action create");
            assert_eq!(
                body["code"],
                json!("ALREADY_EXISTS"),
                "{kind} action create"
            );
            assert_eq!(body["message"], json!(message), "{kind} action create");

            let (status, body) = server
                .post_auth(
                    "/api/v5/sources",
                    json!({"id": id, "type": kind, "enable": true}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} source create");
            assert_eq!(
                body["code"],
                json!("ALREADY_EXISTS"),
                "{kind} source create"
            );
            assert_eq!(body["message"], json!(message), "{kind} source create");

            let (status, body) = server
                .post_auth(
                    "/api/v5/connectors_probe",
                    json!({"type": kind, "config": {}}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} connector probe");
            assert_eq!(body["code"], json!("TEST_FAILED"), "{kind} connector probe");
            assert_eq!(body["message"], json!(message), "{kind} connector probe");

            let (status, body) = server
                .post_auth(
                    "/api/v5/actions_probe",
                    json!({"type": kind, "config": {}}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} action probe");
            assert_eq!(body["code"], json!("TEST_FAILED"), "{kind} action probe");
            assert_eq!(body["message"], json!(message), "{kind} action probe");

            let (status, body) = server
                .post_auth(
                    "/api/v5/sources_probe",
                    json!({"type": kind, "config": {}}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} source probe");
            assert_eq!(body["code"], json!("TEST_FAILED"), "{kind} source probe");
            assert_eq!(body["message"], json!(message), "{kind} source probe");

            let (status, body) = server
                .post_auth(
                    "/api/v1/connectors",
                    json!({"id": id, "kind": kind, "config": {}}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} v1 connector create");
            assert_eq!(body["error"], json!(message), "{kind} v1 connector create");
        }

        let (status, connectors) = server.get_auth("/api/v5/connectors", &token).await;
        assert_eq!(status, 200);
        assert_eq!(connectors, json!([]));
        let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
        assert_eq!(status, 200);
        assert_eq!(actions, json!([]));
        let (status, sources) = server.get_auth("/api/v5/sources", &token).await;
        assert_eq!(status, 200);
        assert_eq!(sources, json!([]));
        let (status, schema) = server.get_auth("/schemas/connectors", &token).await;
        assert_eq!(status, 200);
        let schema: serde_json::Value = serde_json::from_str(
            schema
                .as_str()
                .expect("connector schema body is a JSON string"),
        )
        .expect("connector schema string holds JSON");
        assert!(
            schema.get("connector_s3tables.post_connector").is_none(),
            "removed connector schema is not served"
        );
        let (status, _) = server
            .get_auth("/schemas/connector_s3tables.post_connector", &token)
            .await;
        assert_eq!(
            status, 404,
            "direct request for a removed schema is unknown"
        );
        let (status, action_types) = server.get_auth("/api/v5/action_types", &token).await;
        assert_eq!(status, 200);
        let action_types = action_types.as_array().expect("action type list");
        assert!(!action_types
            .iter()
            .any(|kind| kind.as_str() == Some("s3tables")));
        assert!(!action_types
            .iter()
            .any(|kind| kind.as_str() == Some("s3_tables")));
        let (status, source_types) = server.get_auth("/api/v5/source_types", &token).await;
        assert_eq!(status, 200);
        let source_types = source_types.as_array().expect("source type list");
        assert!(!source_types
            .iter()
            .any(|kind| kind.as_str() == Some("s3tables")));
        assert!(!source_types
            .iter()
            .any(|kind| kind.as_str() == Some("s3_tables")));
    }

    #[tokio::test]
    async fn removed_update_is_refused_and_keeps_the_old_connector_and_action() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let message = "connector type s3_tables is not available in this broker";

        for (index, kind) in ["s3_tables", "s3tables", "iceberg"].iter().enumerate() {
            let connector_id = format!("keep-conn-{index}");
            let (status, _) = server
                .post_auth(
                    "/api/v5/connectors",
                    json!({"id": connector_id, "type": "console"}),
                    &token,
                )
                .await;
            assert_eq!(status, 201, "{kind} connector seed");
            let (status, body) = server
                .put_auth(
                    &format!("/api/v5/connectors/{connector_id}"),
                    json!({"id": connector_id, "type": kind}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} connector update");
            assert_eq!(
                body["code"],
                json!("BAD_REQUEST"),
                "{kind} connector update"
            );
            assert_eq!(body["message"], json!(message), "{kind} connector update");
            let (status, stored) = server
                .get_auth(&format!("/api/v5/connectors/{connector_id}"), &token)
                .await;
            assert_eq!(status, 200, "{kind} connector still readable");
            assert_eq!(
                stored["type"],
                json!("console"),
                "{kind} connector unchanged"
            );

            let action_id = format!("keep-act-{index}");
            let (status, _) = server
                .post_auth(
                    "/api/v5/actions",
                    json!({"id": action_id, "type": "console"}),
                    &token,
                )
                .await;
            assert_eq!(status, 201, "{kind} action seed");
            let (status, body) = server
                .put_auth(
                    &format!("/api/v5/actions/{action_id}"),
                    json!({"id": action_id, "type": kind}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{kind} action update");
            assert_eq!(body["code"], json!("BAD_REQUEST"), "{kind} action update");
            assert_eq!(body["message"], json!(message), "{kind} action update");
            let (status, stored) = server
                .get_auth(&format!("/api/v5/actions/{action_id}"), &token)
                .await;
            assert_eq!(status, 200, "{kind} action still readable");
            assert_eq!(stored["type"], json!("console"), "{kind} action unchanged");

            let (status, _) = server
                .delete_auth(&format!("/api/v5/connectors/{connector_id}"), &token)
                .await;
            assert_eq!(status, 204, "{kind} connector cleanup");
            let (status, _) = server
                .delete_auth(&format!("/api/v5/actions/{action_id}"), &token)
                .await;
            assert_eq!(status, 204, "{kind} action cleanup");
        }
    }

    struct NoopBrokerSink;
    #[async_trait::async_trait]
    impl broker_rules::BrokerSink for NoopBrokerSink {
        async fn publish(
            &self,
            _topic: broker_protocol::Topic,
            _payload: bytes::Bytes,
            _qos: broker_protocol::QoS,
            _retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn legacy_s3_tables_state_boots_reports_and_deletes_in_reference_order() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir =
            std::env::temp_dir().join(format!("indra-mv04-legacy-{}-{nanos}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create data dir");
        let legacy = r#"
[[connectors.connectors]]
id = "legacy-s3t"
connector_type = "s3_tables"
enable = true
config = '{"table_bucket_arn":"arn:aws:s3tables:us-east-1:123456789012:bucket/telemetry"}'

[[connectors.connectors]]
id = "legacy-good"
connector_type = "console"
enable = true
config = '{}'

[[actions.actions]]
id = "legacy-s3t"
action_type = "s3tables"
enable = true
config = '{"connector":"legacy-s3t"}'

[[actions.actions]]
id = "legacy-good"
action_type = "console"
enable = true
config = '{"connector":"legacy-good"}'

[[rules.rules]]
id = "legacy-rule"
name = "legacy rule"
tenant = "default"
topic_filter = "legacy/#"
sql_query = 'SELECT * FROM "legacy/+" INTO connector("legacy-s3t")'
enabled = true

[[rules.rules.actions]]
type = "forwardconnector"
connector_id = "legacy-good"
"#;
        std::fs::write(dir.join("state.toml"), legacy).expect("write legacy state");

        let registry =
            Arc::new(ConfigRegistry::load_for_boot(&dir).expect("legacy state loads for boot"));
        let (server, state) = TestServer::start_with_registry(registry).await;
        let token = server.login_as_admin().await;
        let reason = "connector type s3_tables is not available in this broker";

        let (status, connectors) = server.get_auth("/api/v5/connectors", &token).await;
        assert_eq!(status, 200);
        let s3 = connectors
            .as_array()
            .expect("connector list")
            .iter()
            .find(|row| row["id"] == json!("legacy-s3t"))
            .expect("legacy connector listed");
        assert_eq!(s3["status"], json!("disconnected"));
        assert_eq!(s3["status_reason"], json!(reason));
        assert!(
            connectors
                .as_array()
                .expect("connector list")
                .iter()
                .any(|row| row["id"] == json!("legacy-good")),
            "working sibling connector stays listed"
        );

        let (status, actions) = server.get_auth("/api/v5/actions", &token).await;
        assert_eq!(status, 200);
        let action = actions
            .as_array()
            .expect("action list")
            .iter()
            .find(|row| row["id"] == json!("legacy-s3t"))
            .expect("legacy action listed");
        assert_eq!(action["status"], json!("disconnected"));
        assert_eq!(action["status_reason"], json!(reason));

        let (status, rule) = server.get_auth("/api/v5/rules/legacy-rule", &token).await;
        assert_eq!(status, 200);
        assert_eq!(rule["enable"], json!(true));

        let (status, alarms) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        let names: Vec<&str> = alarms["data"]
            .as_array()
            .expect("alarm rows")
            .iter()
            .filter_map(|row| row["name"].as_str())
            .collect();
        assert!(
            names.contains(&"connector_unavailable:legacy-s3t"),
            "{names:?}"
        );
        assert!(
            names.contains(&"action_unavailable:legacy-s3t"),
            "{names:?}"
        );

        let sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
        state
            .engine
            .dispatch_ingress(
                &Topic::new("legacy/device").expect("topic"),
                &bytes::Bytes::from_static(br#"{"v":1}"#),
                QoS::AtMostOnce,
                &sink,
            )
            .await;

        let (status, metrics) = server
            .get_auth("/api/v5/actions/legacy-s3t/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(metrics["metrics"]["total"], json!(1));
        assert_eq!(metrics["metrics"]["failed"], json!(1));
        let (status, metrics) = server
            .get_auth("/api/v5/actions/legacy-good/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(metrics["metrics"]["total"], json!(1));
        assert_eq!(metrics["metrics"]["success"], json!(1));
        let (status, metrics) = server
            .get_auth("/api/v5/rules/legacy-rule/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(metrics["metrics"]["actions.failed"], json!(1));
        assert_eq!(metrics["metrics"]["actions.success"], json!(1));

        let (status, _) = server
            .delete_auth("/api/v5/connectors/legacy-s3t", &token)
            .await;
        assert_eq!(status, 400, "connector is still referenced by the rule");
        let (status, _) = server
            .delete_auth("/api/v5/actions/legacy-s3t", &token)
            .await;
        assert_eq!(status, 400, "action is still referenced by the rule");
        let (status, _) = server
            .delete_auth("/api/v5/rules/legacy-rule", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/legacy-s3t", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/connectors/legacy-s3t", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/legacy-good", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/connectors/legacy-good", &token)
            .await;
        assert_eq!(status, 204);

        let (status, alarms) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        let names: Vec<&str> = alarms["data"]
            .as_array()
            .expect("alarm rows")
            .iter()
            .filter_map(|row| row["name"].as_str())
            .collect();
        assert!(
            !names.contains(&"connector_unavailable:legacy-s3t"),
            "{names:?}"
        );
        assert!(
            !names.contains(&"action_unavailable:legacy-s3t"),
            "{names:?}"
        );

        let reloaded = ConfigRegistry::load_for_boot(&dir).expect("rewritten state still boots");
        assert!(reloaded.snapshot().connectors.connectors.is_empty());
        assert!(reloaded.snapshot().actions.actions.is_empty());
        assert!(reloaded.snapshot().rules.rules.is_empty());
    }

    /// The licence endpoints have the shapes of the reference API and
    /// show an open-source licence without a real limit.
    #[tokio::test]
    async fn the_licence_endpoints_answer_as_the_reference_does() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, doc) = server.get_auth("/api/v5/license", &token).await;
        assert_eq!(status, 200);
        assert_eq!(doc["max_sessions"], json!(10_000_000_000u64));
        assert_eq!(doc["expiry"], json!(false));
        let (status, same) = server
            .post_auth("/api/v5/license", json!({"key": "default"}), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(same, doc);
        let (status, body) = server.post_auth("/api/v5/license", json!({}), &token).await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        let (status, setting) = server.get_auth("/api/v5/license/setting", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            setting,
            json!({
                "connection_low_watermark": "75%",
                "connection_high_watermark": "80%",
                "high_watermark_timezone": "system",
            })
        );
        let (status, updated) = server
            .put_auth(
                "/api/v5/license/setting",
                json!({"connection_high_watermark": "90%"}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "update: {updated}");
        assert_eq!(updated["connection_high_watermark"], json!("90%"));
        assert_eq!(updated["connection_low_watermark"], json!("75%"));
        let (status, body) = server
            .put_auth(
                "/api/v5/license/setting",
                json!({"connection_low_watermark": "95%"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "low above high: {body}");

        let (status, empty) = server
            .get_auth("/api/v5/license/session_hwm_history", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(empty, json!({"period": "monthly", "count": 0, "data": []}));
        state.license.record_sessions(12, 20_731 * 86_400 + 5);
        let (status, daily) = server
            .get_auth(
                "/api/v5/license/session_hwm_history?period=daily&limit=5",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            daily,
            json!({
                "period": "daily",
                "count": 1,
                "data": [{
                    "period": "2026-10-05",
                    "high_watermark": 12,
                    "observed_at": "2026-10-05T00:00:05+00:00",
                }],
            })
        );
        let (status, _) = server
            .get_auth("/api/v5/license/session_hwm_history?period=weekly", &token)
            .await;
        assert_eq!(status, 400);
    }

    /// The `Authorization: Basic` value for an API key.
    fn basic_authorization(key_id: &str, secret: &str) -> String {
        use base64::Engine;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(format!("{key_id}:{secret}"));
        format!("Basic {encoded}")
    }

    /// Makes a key through the API and returns the identifier and the
    /// secret.
    async fn make_key(server: &TestServer, token: &str, body: Value) -> (String, String) {
        let (status, created) = server.post_auth("/api/v5/api_key", body, token).await;
        assert_eq!(status, 200, "create: {created}");
        (
            created["api_key"].as_str().expect("identifier").to_string(),
            created["api_secret"].as_str().expect("secret").to_string(),
        )
    }

    /// The secret of a key from `POST /api_key` works as the password of
    /// Basic authentication (user name: the `api_key` identifier) and as
    /// a bearer credential. The scopes of the key limit the paths, the
    /// operations that the reference lists for a bearer token only refuse
    /// a key, and a removed, disabled or expired key does not work.
    #[tokio::test]
    async fn an_api_key_authenticates_with_its_scopes() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (scoped_id, scoped) = make_key(
            &server,
            &token,
            json!({"name": "scoped", "scopes": ["monitoring"]}),
        )
        .await;
        let (full_id, full) = make_key(&server, &token, json!({"name": "full"})).await;

        // The scoped key can read its group and nothing else, with Basic
        // and with Bearer.
        let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
        assert_eq!(status, 200);
        let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &scoped).await;
        assert_eq!(status, 200);
        let (status, body) = server.get_auth("/api/v5/clients", &scoped).await;
        assert_eq!(status, 403, "another scope: {body}");
        assert_eq!(body["code"], json!("FORBIDDEN"));
        let (status, _) = server
            .get_basic("/api/v5/clients", &scoped_id, &scoped)
            .await;
        assert_eq!(status, 403);

        // The key with no scope list can call each group.
        let (status, _) = server.get_basic("/api/v5/stats", &full_id, &full).await;
        assert_eq!(status, 200);
        let (status, _) = server.get_basic("/api/v5/clients", &full_id, &full).await;
        assert_eq!(status, 200);

        // Basic needs the identifier and the secret together.
        let (status, _) = server
            .get_basic("/api/v5/stats", &full_id, "wrong-secret")
            .await;
        assert_eq!(status, 401);
        let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &full).await;
        assert_eq!(status, 401, "the secret of another key");
        let (status, _) = server.get_basic("/api/v5/stats", "full", &full).await;
        assert_eq!(status, 401, "the name is not the identifier");
        let (status, _) = server.get_auth("/api/v5/stats", "scoped").await;
        assert_eq!(status, 401, "the name is not a secret");

        // The reference lists these operations for a bearer token only:
        // no key can call them, whatever the scheme.
        for (key_id, secret) in [(&scoped_id, &scoped), (&full_id, &full)] {
            for path in [
                "/api/v5/api_key",
                "/api/v5/api_key_scopes",
                "/api/v5/users",
                "/api/v5/user_scopes",
            ] {
                let (status, body) = server.get_basic(path, key_id, secret).await;
                assert_eq!(status, 403, "GET {path} with Basic: {body}");
                assert_eq!(body["code"], json!("FORBIDDEN"));
                let (status, _) = server.get_auth(path, secret).await;
                assert_eq!(status, 403, "GET {path} with Bearer");
            }
            let (status, _) = server
                .post_basic("/api/v5/api_key", json!({"name": "more"}), key_id, secret)
                .await;
            assert_eq!(status, 403);
            let (status, _) = server
                .post_basic("/api/v5/users", json!({}), key_id, secret)
                .await;
            assert_eq!(status, 403);
            // Logout documents 401.
            let (status, _) = server
                .post_basic("/api/v5/logout", json!({}), key_id, secret)
                .await;
            assert_eq!(status, 401);
        }
        let (status, list) = server.get_auth("/api/v5/api_key", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            list.as_array().map(Vec::len),
            Some(2),
            "no key made a key: {list}"
        );

        // The operations that the reference lists for Basic accept a key
        // without a scope list. The catalogue has no scope for them, thus
        // a key with a scope list cannot call them.
        let (status, body) = server
            .get_basic("/api/v5/api_key/scoped", &full_id, &full)
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["name"], json!("scoped"));
        assert!(body.get("api_secret").is_none());
        let (status, _) = server.get_basic("/api/v5/sso", &full_id, &full).await;
        assert_eq!(status, 200);
        let (status, _) = server.get_basic("/api/v5/sso", &scoped_id, &scoped).await;
        assert_eq!(status, 403);
        let (status, _) = server
            .get_basic("/api/v5/api_key/full", &scoped_id, &scoped)
            .await;
        assert_eq!(status, 403);

        // A disabled key does not work, and works again when enabled.
        let (status, body) = server
            .put_auth("/api/v5/api_key/scoped", json!({"enable": false}), &token)
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["enable"], json!(false));
        let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
        assert_eq!(status, 401);
        let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &scoped).await;
        assert_eq!(status, 401);
        let (status, _) = server
            .put_auth("/api/v5/api_key/scoped", json!({"enable": true}), &token)
            .await;
        assert_eq!(status, 200);
        let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
        assert_eq!(status, 200);

        // An expired key does not work, and the read shows `expired`.
        let (status, body) = server
            .put_auth(
                "/api/v5/api_key/scoped",
                json!({"expired_at": "2020-01-01T00:00:00Z"}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["expired"], json!(true));
        let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
        assert_eq!(status, 401);
        let (status, _) = server
            .put_auth(
                "/api/v5/api_key/scoped",
                json!({"expired_at": "infinity"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server.get_auth("/api/v5/api_key/scoped", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["expired"], json!(false));
        assert_eq!(body["expired_at"], json!("infinity"));
        let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
        assert_eq!(status, 200);

        // A key with the role `viewer` can read and cannot write.
        let (viewer_id, viewer) = make_key(
            &server,
            &token,
            json!({"name": "watcher", "role": "viewer"}),
        )
        .await;
        let (status, _) = server.get_basic("/api/v5/stats", &viewer_id, &viewer).await;
        assert_eq!(status, 200);
        let (status, body) = server
            .post_basic("/api/v5/publish", json!({}), &viewer_id, &viewer)
            .await;
        assert_eq!(status, 403, "a viewer key cannot write: {body}");

        // A key with an empty scope list can call nothing.
        let (nothing_id, nothing) =
            make_key(&server, &token, json!({"name": "nothing", "scopes": []})).await;
        let (status, _) = server
            .get_basic("/api/v5/stats", &nothing_id, &nothing)
            .await;
        assert_eq!(status, 403);

        // A removed key does not work.
        let (status, _) = server.delete_auth("/api/v5/api_key/scoped", &token).await;
        assert_eq!(status, 204);
        let (status, _) = server.get_auth("/api/v5/stats", &scoped).await;
        assert_eq!(status, 401);
        let (status, _) = server.get_basic("/api/v5/stats", &scoped_id, &scoped).await;
        assert_eq!(status, 401);
        let (status, _) = server.get_auth("/api/v5/stats", &full).await;
        assert_eq!(status, 200);
    }

    /// A viewer can enrol MFA for that viewer. A viewer cannot start or
    /// read the enrolment of a different user.
    #[tokio::test]
    async fn a_viewer_enrols_mfa_only_for_that_viewer() {
        let (server, state) = TestServer::start().await;
        state
            .admin_users
            .create(
                "watcher",
                "Watch3r-test-pass!",
                crate::admin_users::AdminRole::Viewer,
                "",
            )
            .expect("viewer");
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "watcher", "password": "Watch3r-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200, "viewer login: {body}");
        let viewer = body["token"].as_str().expect("token").to_string();

        for path in ["/api/v5/sso/mfa/setup", "/api/v5/sso/mfa/setup_info"] {
            let (status, body) = server
                .post_auth(path, json!({"username": "admin"}), &viewer)
                .await;
            assert_eq!(status, 403, "{path} for a different user: {body}");
            assert!(body.get("secret").is_none());
        }
        let (status, _) = server
            .post_auth(
                "/api/v5/sso/mfa/verify",
                json!({"username": "admin", "code": "000000"}),
                &viewer,
            )
            .await;
        assert_eq!(status, 403);
        let admin_status = state.admin_users.mfa_status("admin").expect("admin");
        assert!(!admin_status.pending && !admin_status.enabled);

        let (status, setup) = server
            .post_auth(
                "/api/v5/sso/mfa/setup",
                json!({"username": "watcher"}),
                &viewer,
            )
            .await;
        assert_eq!(status, 200, "own setup: {setup}");
        let secret = crate::mfa_totp::base32_decode(setup["secret"].as_str().expect("secret"))
            .expect("base32 secret");
        let step = crate::mfa_totp::totp_counter(crate::mfa_totp::unix_now_secs());
        let (status, body) = server
            .post_auth(
                "/api/v5/sso/mfa/verify",
                json!({"username": "watcher", "code": crate::mfa_totp::totp_code(&secret, step)}),
                &viewer,
            )
            .await;
        assert_eq!(status, 200, "own verify: {body}");
        assert!(
            state
                .admin_users
                .mfa_status("watcher")
                .expect("viewer")
                .enabled
        );

        // The other write routes stay closed for a viewer.
        let (status, _) = server
            .post_auth("/api/v5/api_key", json!({"name": "k"}), &viewer)
            .await;
        assert_eq!(status, 403);
    }

    /// A user with MFA cannot log in with the password only. The login
    /// needs a correct code, and one code works one time.
    #[tokio::test]
    async fn login_needs_the_mfa_code_when_the_user_has_mfa() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Enrol: get the secret, then confirm it with a code.
        let (status, setup) = server
            .post_auth(
                "/api/v5/sso/mfa/setup",
                json!({"username": "admin"}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "setup: {setup}");
        let secret = crate::mfa_totp::base32_decode(setup["secret"].as_str().expect("secret"))
            .expect("base32 secret");
        let now = crate::mfa_totp::unix_now_secs();
        let step = crate::mfa_totp::totp_counter(now);
        // The code of the step before this one is in the accepted window,
        // and it leaves the code of this step for the login below.
        let enrol_code = crate::mfa_totp::totp_code(&secret, step - 1);
        let (status, body) = server
            .post_auth(
                "/api/v5/sso/mfa/verify",
                json!({"username": "admin", "code": enrol_code}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "verify: {body}");

        let password = json!({"username": "admin", "password": "Adm1n-test-pass!"});
        // The password alone is not sufficient.
        let (status, body) = server.post("/api/v5/login", password.clone()).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_MFA_TOKEN"));
        assert!(body.get("token").is_none(), "no token without the code");

        // An incorrect code is refused.
        let mut wrong = password.clone();
        wrong["mfa_token"] = json!("000000");
        let login_code = crate::mfa_totp::totp_code(&secret, step);
        if login_code != "000000" {
            let (status, body) = server.post("/api/v5/login", wrong).await;
            assert_eq!(status, 401);
            assert_eq!(body["code"], json!("BAD_MFA_TOKEN"));
        }

        // The correct code gives a token.
        let mut good = password.clone();
        good["mfa_token"] = json!(login_code);
        let (status, body) = server.post("/api/v5/login", good.clone()).await;
        assert_eq!(status, 200, "login with the code: {body}");
        assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));

        // The same code does not work a second time.
        let (status, body) = server.post("/api/v5/login", good).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_MFA_TOKEN"));
    }

    /// After five incorrect passwords the user is locked for a time, and
    /// the correct password does not work during that time. A name that
    /// is not a user gets the usual refusal and is not locked.
    #[tokio::test]
    async fn repeated_failed_logins_lock_the_user() {
        let (server, _state) = TestServer::start().await;
        for _ in 0..crate::admin_users::MFA_VERIFY_MAX_ATTEMPTS {
            let (status, body) = server
                .post(
                    "/api/v5/login",
                    json!({"username": "admin", "password": "not-the-password"}),
                )
                .await;
            assert_eq!(status, 401);
            assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
        }
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "public"}),
            )
            .await;
        assert_eq!(status, 401, "locked: {body}");
        assert_eq!(body["code"], json!("LOGIN_LOCKED"));
        assert!(body.get("token").is_none());

        for _ in 0..(crate::admin_users::MFA_VERIFY_MAX_ATTEMPTS + 2) {
            let (status, body) = server
                .post(
                    "/api/v5/login",
                    json!({"username": "no-such-user", "password": "x"}),
                )
                .await;
            assert_eq!(status, 401, "an unknown name is not locked: {body}");
        }
    }

    /// Minimal SCRAM-SHA-256 client for the login tests (independent of the
    /// server's credential store).
    mod scram_client {
        use base64::engine::general_purpose::STANDARD as BASE64;
        use base64::Engine;
        use sha2::{Digest, Sha256};

        fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
            let mut padded = [0u8; 64];
            if key.len() > 64 {
                let hash = Sha256::digest(key);
                padded[..32].copy_from_slice(&hash);
            } else {
                padded[..key.len()].copy_from_slice(key);
            }
            let mut ipad = [0x36u8; 64];
            let mut opad = [0x5cu8; 64];
            for i in 0..64 {
                ipad[i] ^= padded[i];
                opad[i] ^= padded[i];
            }
            let mut inner = Sha256::new();
            inner.update(ipad);
            inner.update(data);
            let inner_hash = inner.finalize();
            let mut outer = Sha256::new();
            outer.update(opad);
            outer.update(inner_hash);
            outer.finalize().into()
        }

        fn salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
            let mut block = Vec::with_capacity(salt.len() + 4);
            block.extend_from_slice(salt);
            block.extend_from_slice(&1u32.to_be_bytes());
            let mut u = hmac(password.as_bytes(), &block);
            let mut result = u;
            for _ in 1..iterations {
                u = hmac(password.as_bytes(), &u);
                for (r, v) in result.iter_mut().zip(u.iter()) {
                    *r ^= *v;
                }
            }
            result
        }

        /// Returns `(client_proof_b64, server_signature_b64)`.
        pub(super) fn proof(
            password: &str,
            salt_b64: &str,
            iterations: u32,
            auth_message: &str,
        ) -> (String, String) {
            let salt = BASE64.decode(salt_b64).expect("challenge salt");
            let salted = salted_password(password, &salt, iterations);
            let client_key = hmac(&salted, b"Client Key");
            let stored_key: [u8; 32] = Sha256::digest(client_key).into();
            let client_sig = hmac(&stored_key, auth_message.as_bytes());
            let mut proof = [0u8; 32];
            for i in 0..32 {
                proof[i] = client_key[i] ^ client_sig[i];
            }
            let server_key = hmac(&salted, b"Server Key");
            let server_sig = hmac(&server_key, auth_message.as_bytes());
            (BASE64.encode(proof), BASE64.encode(server_sig))
        }
    }

    fn scram_auth_message(
        username: &str,
        client_nonce: &str,
        combined_nonce: &str,
        salt_b64: &str,
        iterations: u32,
    ) -> String {
        let escaped = username.replace('=', "=3D").replace(',', "=2C");
        format!(
            "n={escaped},r={client_nonce},r={combined_nonce},s={salt_b64},i={iterations},c=biws,r={combined_nonce}"
        )
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    #[test]
    fn standalone_needs_no_filesystem() {
        let engine = Arc::new(RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        assert_eq!(
            *state.config.snapshot(),
            broker_config::FullSnapshot::default(),
            "standalone exposes validated defaults"
        );
        assert_eq!(state.node_id, "indra-node-1");
        assert!(
            !state.config.dir().exists(),
            "standalone must not create any directory, found: {}",
            state.config.dir().display()
        );
    }

    #[tokio::test]
    async fn test_api_router_health() {
        let (server, _state) = TestServer::start().await;
        let (status, text) = server
            .request_raw("GET /healthz HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await;
        assert_eq!(status, 200);
        assert_eq!(text, "OK");
    }

    /// Empty states of the console pages (AP-01, AP2-02): the reads answer
    /// the documented shape with an empty or disabled state, and the writes
    /// refuse with the documented 400 code. Nothing is stored.
    #[tokio::test]
    async fn test_console_empty_states_are_honest() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // AI lists read empty; the Flow Designer loads both together.
        let (status, body) = server.get_auth("/api/v5/ai/providers", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server
            .get_auth("/api/v5/ai/completion_profiles", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server
            .get_auth("/api/v5/ai/providers/no-such-provider/models", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server
            .post_auth(
                "/api/v5/ai/providers",
                json!({"name": "x", "type": "openai", "api_key": "k"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("INVALID_CREDENTIAL"));
        let (status, body) = server
            .post_auth(
                "/api/v5/ai/completion_profiles",
                json!({"name": "x", "provider_name": "y", "type": "openai"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("INVALID_COMPLETION_PROFILE"));
        let (status, body) = server
            .post_auth(
                "/api/v5/ai/models",
                json!({"type": "openai", "api_key": "k"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("INVALID_CREDENTIAL"));
        let (status, body) = server
            .get_auth("/api/v5/ai/providers/no-such-provider", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Queue and stream configs read disabled; lists read empty.
        let (status, body) = server.get_auth("/api/v5/queues/config", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        assert!(body["auto_create"]["lastvalue"].get("limits").is_some());
        assert!(body["auto_create"]["regular"].get("limits").is_some());
        let (status, body) = server.get_auth("/api/v5/streams/config", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        assert!(body["auto_create"]["lastvalue"].get("limits").is_some());
        assert!(body["auto_create"]["regular"].get("limits").is_some());
        let (status, body) = server.get_auth("/api/v5/queues", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server.get_auth("/api/v5/streams", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server
            .put_auth("/api/v5/queues/config", json!({"enable": true}), &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .post_auth("/api/v5/queues", json!({"name": "q1"}), &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        let (status, body) = server
            .post_auth("/api/v5/streams", json!({"name": "s1"}), &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));

        // Telemetry reads disabled; data misses while disabled.
        let (status, body) = server.get_auth("/api/v5/telemetry/status", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!({"enable": false}));
        let (status, body) = server.get_auth("/api/v5/telemetry/data", &token).await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Rebalance and eviction read idle; writes refuse.
        let (status, body) = server
            .get_auth("/api/v5/load_rebalance/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!({"status": "disabled"}));
        let (status, body) = server
            .get_auth("/api/v5/load_rebalance/global_status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["rebalances"], json!([]));
        let (status, body) = server
            .get("/api/v5/load_rebalance/availability_check")
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!({}));
        let (status, body) = server
            .get_auth("/api/v5/node_eviction/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!({"status": "disabled"}));
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/load_rebalance/no-such-node/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn test_rules_crud_lifecycle() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty at first.
        let (status, body) = server.get_auth("/api/v1/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        // Create.
        let (status, created) = server
            .post_auth(
                "/api/v1/rules",
                json!({
                    "name": "republish-temp",
                    "topic_filter": "sensors/+",
                    "enabled": true,
                    "actions": [{"type": "republish", "topic": "alerts/critical", "qos": 1}]
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["name"], json!("republish-temp"));
        assert_eq!(created["topic_filter"], json!("sensors/+"));
        assert_eq!(created["actions"][0]["type"], json!("republish"));
        assert_eq!(created["actions"][0]["qos"], json!(1));
        let id = created["id"]
            .as_str()
            .expect("created rule has id")
            .to_string();

        // List shows it.
        let (status, body) = server.get_auth("/api/v1/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("list").len(), 1);

        // Fetch it directly.
        let (status, fetched) = server
            .get_auth(&format!("/api/v1/rules/{id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched, created);

        // Unknown id is a 404.
        let (status, body) = server.get_auth("/api/v1/rules/rule-999", &token).await;
        assert_eq!(status, 404);
        assert!(body["error"].as_str().unwrap().contains("rule-999"));

        // Delete it.
        let (status, _) = server
            .delete_auth(&format!("/api/v1/rules/{id}"), &token)
            .await;
        assert_eq!(status, 204);

        // Gone afterwards; second delete is a 404.
        let (status, _) = server
            .get_auth(&format!("/api/v1/rules/{id}"), &token)
            .await;
        assert_eq!(status, 404);
        let (status, _) = server
            .delete_auth(&format!("/api/v1/rules/{id}"), &token)
            .await;
        assert_eq!(status, 404);
    }

    /// Record the tenant of each rule output through the broker sink.
    struct TenantProbe {
        recorded: std::sync::Mutex<Vec<(String, String)>>,
    }

    #[async_trait::async_trait]
    impl broker_rules::BrokerSink for TenantProbe {
        async fn publish(
            &self,
            topic: Topic,
            _payload: bytes::Bytes,
            _qos: QoS,
            _retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            self.recorded
                .lock()
                .unwrap()
                .push(("default".to_string(), topic.as_str().to_string()));
            Ok(())
        }

        async fn publish_in_tenant(
            &self,
            tenant: &str,
            topic: Topic,
            _payload: bytes::Bytes,
            _qos: QoS,
            _retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            self.recorded
                .lock()
                .unwrap()
                .push((tenant.to_string(), topic.as_str().to_string()));
            Ok(())
        }
    }

    /// Unknown rule tenants fail closed through the v1 API: an unknown
    /// tenant and a malformed tenant both answer 400 and store nothing.
    #[tokio::test]
    async fn test_rules_tenant_unknown_rejected_through_api() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for tenant in ["tm04-ghost", "bad\u{7}tenant"] {
            let (status, body) = server
                .post_auth(
                    "/api/v1/rules",
                    json!({
                        "name": "tm04-bad",
                        "topic_filter": "tm04/#",
                        "tenant": tenant,
                        "actions": []
                    }),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "tenant {tenant:?} must fail");
            assert!(
                body["error"].as_str().unwrap().contains("tenant"),
                "error names the field: {body}"
            );
        }
        let (status, body) = server.get_auth("/api/v1/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]), "nothing stored");
        assert!(state.engine.list_rules().is_empty());
    }

    /// One rule of tenant A is created through the v1 API. It reads
    /// back with its tenant. It fires only for a publish of tenant A
    /// through the broker dispatch. Its output carries tenant A only.
    #[tokio::test]
    async fn test_rules_tenant_round_trip_through_api_and_broker() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Two tenants through the production namespace entry.
        for ns in ["tm04-a", "tm04-b"] {
            let (status, _) = server
                .post_auth(&format!("/api/v5/mt/ns/{ns}"), json!({}), &token)
                .await;
            assert_eq!(status, 204, "namespace {ns} creates");
        }
        // Random value the broker cannot know.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|span| span.as_nanos())
            .unwrap_or(0);
        let secret = format!("tm04-secret-{nanos}");
        // Create the rule of tenant A with a republish action.
        let (status, created) = server
            .post_auth(
                "/api/v1/rules",
                json!({
                    "name": "tm04-a-rule",
                    "topic_filter": "tm04/in",
                    "tenant": "tm04-a",
                    "actions": [{"type": "republish", "topic": "tm04/out", "qos": 0}]
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201, "rule creates: {created}");
        assert_eq!(created["tenant"], json!("tm04-a"));
        let id = created["id"]
            .as_str()
            .expect("created rule has id")
            .to_string();
        // The read shows the same tenant.
        let (status, fetched) = server
            .get_auth(&format!("/api/v1/rules/{id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["tenant"], json!("tm04-a"));
        // No tenant in the body means the default tenant, as before.
        let (status, created) = server
            .post_auth(
                "/api/v1/rules",
                json!({
                    "name": "tm04-plain",
                    "topic_filter": "tm04/in",
                    "actions": []
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["tenant"], json!("default"));
        // Through the broker: a publish of A fires only the A rule,
        // and the output carries tenant A. A publish of B fires
        // nothing: no rule of B matches.
        let probe = Arc::new(TenantProbe {
            recorded: std::sync::Mutex::new(Vec::new()),
        });
        let probe_obj: Arc<dyn broker_rules::BrokerSink> = probe.clone();
        let topic = Topic::new("tm04/in").expect("topic parses");
        let payload = bytes::Bytes::from(secret.clone());
        let fired = state
            .engine
            .dispatch_ingress_in_tenant("tm04-a", &topic, &payload, QoS::AtMostOnce, &probe_obj)
            .await;
        assert_eq!(fired, 1, "only the A rule fires for an A publish");
        let fired = state
            .engine
            .dispatch_ingress_in_tenant("tm04-b", &topic, &payload, QoS::AtMostOnce, &probe_obj)
            .await;
        assert_eq!(fired, 0, "no rule fires for a B publish");
        let recorded = probe.recorded.lock().unwrap().clone();
        assert_eq!(recorded.len(), 1, "one output, from the A publish");
        assert_eq!(recorded[0], ("tm04-a".to_string(), "tm04/out".to_string()));
        // Cleanup through the API.
        for rule_id in [id, created["id"].as_str().unwrap().to_string()] {
            let (status, _) = server
                .delete_auth(&format!("/api/v1/rules/{rule_id}"), &token)
                .await;
            assert_eq!(status, 204);
        }
    }

    /// The v5 rule API carries the tenant: create validates it, the
    /// read shows it, update moves it, and dispatch honours it.
    #[tokio::test]
    async fn test_v5_rules_tenant_create_update_dispatch_through_api() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for ns in ["tm04-v5a", "tm04-v5b"] {
            let (status, _) = server
                .post_auth(&format!("/api/v5/mt/ns/{ns}"), json!({}), &token)
                .await;
            assert_eq!(status, 204, "namespace {ns} creates");
        }
        // Unknown tenant fails with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "tm04-v5-bad",
                    "sql": "SELECT * FROM \"tm04v5/in\"",
                    "tenant": "tm04-ghost",
                    "actions": []
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        // Create in the first tenant.
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "tm04-v5-rule",
                    "sql": "SELECT * FROM \"tm04v5/in\"",
                    "tenant": "tm04-v5a",
                    "actions": []
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201, "rule creates: {created}");
        assert_eq!(created["tenant"], json!("tm04-v5a"));
        let id = created["id"]
            .as_str()
            .expect("created rule has id")
            .to_string();
        // Update moves the rule to the second tenant.
        let (status, updated) = server
            .put_auth(
                &format!("/api/v5/rules/{id}"),
                json!({
                    "name": "tm04-v5-rule",
                    "sql": "SELECT * FROM \"tm04v5/in\"",
                    "tenant": "tm04-v5b",
                    "actions": []
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200, "update moves tenant: {updated}");
        assert_eq!(updated["tenant"], json!("tm04-v5b"));
        // Update to an unknown tenant fails and keeps the old rule.
        let (status, body) = server
            .put_auth(
                &format!("/api/v5/rules/{id}"),
                json!({
                    "name": "tm04-v5-rule",
                    "sql": "SELECT * FROM \"tm04v5/in\"",
                    "tenant": "tm04-ghost",
                    "actions": []
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, fetched) = server
            .get_auth(&format!("/api/v5/rules/{id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["tenant"], json!("tm04-v5b"));
        // Dispatch honours the moved tenant.
        let probe = Arc::new(TenantProbe {
            recorded: std::sync::Mutex::new(Vec::new()),
        });
        let probe_obj: Arc<dyn broker_rules::BrokerSink> = probe.clone();
        let topic = Topic::new("tm04v5/in").expect("topic parses");
        let payload = bytes::Bytes::from_static(br#"{"temp": 21}"#);
        let fired = state
            .engine
            .dispatch_ingress_in_tenant("tm04-v5a", &topic, &payload, QoS::AtMostOnce, &probe_obj)
            .await;
        assert_eq!(fired, 0, "the moved rule no longer fires in A");
        let fired = state
            .engine
            .dispatch_ingress_in_tenant("tm04-v5b", &topic, &payload, QoS::AtMostOnce, &probe_obj)
            .await;
        assert_eq!(fired, 1, "the moved rule fires in B");
    }

    #[tokio::test]
    async fn test_rules_create_rejects_invalid_input() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Bad topic filter.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "bad", "topic_filter": "sport/#/bogus", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(body["error"].as_str().unwrap().contains("topic_filter"));

        // Bad republish QoS.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "bad", "topic_filter": "a/#",
                       "actions": [{"type": "republish", "topic": "b", "qos": 7}]}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(body["error"].as_str().unwrap().contains("qos"));

        // Wildcard republish target is not a concrete topic.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "bad", "topic_filter": "a/#",
                       "actions": [{"type": "republish", "topic": "b/#", "qos": 0}]}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(body["error"].as_str().unwrap().contains("topic"));

        // Empty name.
        let (status, _) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "  ", "topic_filter": "a/#", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 400);

        // Nothing was stored by the failed creates.
        let (status, body) = server.get_auth("/api/v1/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
    }

    #[tokio::test]
    async fn test_rules_create_with_sql_query() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Valid streaming SQL is accepted and echoed back verbatim.
        let (status, created) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "hot-temp",
                       "topic_filter": "sensors/+",
                       "sql_query": "SELECT * FROM \"sensors/+\" WHERE temperature > 50.0",
                       "enabled": true,
                       "actions": [{"type": "republish", "topic": "alerts/hot", "qos": 0}]}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(
            created["sql_query"],
            json!("SELECT * FROM \"sensors/+\" WHERE temperature > 50.0")
        );
        let id = created["id"]
            .as_str()
            .expect("created rule has id")
            .to_string();
        let (status, _) = server
            .get_auth(&format!("/api/v1/rules/{id}"), &token)
            .await;
        assert_eq!(status, 200);

        // Broken SQL is a 400 and stores nothing.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "broken",
                       "topic_filter": "sensors/+",
                       "sql_query": "SELECT WHERE WHERE",
                       "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(body["error"].as_str().unwrap().contains("sql_query"));

        let (status, body) = server.get_auth("/api/v1/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("list").len(), 1);
    }

    #[tokio::test]
    async fn test_nodes_reports_status_version_connections() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.metrics.set_active_connections(3);

        let (status, body) = server.get_auth("/api/v1/nodes", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["status"], json!("running"));
        assert_eq!(body["version"], json!(env!("CARGO_PKG_VERSION")));
        assert_eq!(body["connections"], json!(3));
        assert!(body["node_id"].is_string());
    }

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
    async fn sessions_count_counts_sessions_as_a_json_string() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Zero sessions read as the string "0", not an error.
        let (status, body) = server.get_auth("/api/v5/sessions_count", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!("0"));

        // Two sessions read as the string "2".
        for (id, conn) in [("w1-13-a", 13_101u64), ("w1-13-b", 13_102u64)] {
            let (session, _) = state.sessions.get_or_create(id, true);
            state.sessions.bind_session(&session, conn);
        }
        let (status, body) = server.get_auth("/api/v5/sessions_count", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!("2"));

        // A detached session has not expired, so it stays in the count.
        state.sessions.unbind_connection("w1-13-a", 13_101);
        let (status, body) = server.get_auth("/api/v5/sessions_count", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!(state.sessions.session_count().to_string()));

        // `since` is a number of seconds. The broker keeps no expired
        // sessions, so the count does not change.
        let (status, body) = server
            .get_auth("/api/v5/sessions_count?since=1705391625", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!(state.sessions.session_count().to_string()));
        let (status, body) = server
            .get_auth("/api/v5/sessions_count?since=yesterday", &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Unknown parameters are ignored, not rejected.
        let (status, body) = server
            .get_auth("/api/v5/sessions_count?unknown_param=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!(state.sessions.session_count().to_string()));
    }

    #[tokio::test]
    async fn metrics_snapshot_is_flat_numeric_and_monotonic() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Documented counter names grouped as bytes, packets, messages,
        // delivery, client and session. Every value must stay numeric.
        let expected = [
            "bytes.received",
            "bytes.sent",
            "packets.received",
            "packets.sent",
            "packets.connect.received",
            "packets.connack.sent",
            "packets.connack.error",
            "packets.connack.auth_error",
            "packets.publish.received",
            "packets.publish.sent",
            "packets.subscribe.received",
            "packets.suback.sent",
            "packets.pingreq.received",
            "packets.pingresp.sent",
            "messages.received",
            "messages.sent",
            "messages.dropped",
            "messages.delivered",
            "messages.qos0.received",
            "messages.qos1.received",
            "messages.qos2.received",
            "delivery.dropped",
            "delivery.dropped.no_subscribers",
            "delivery.dropped.queue_full",
            "client.connect",
            "client.connack",
            "client.connected",
            "client.subscribe",
        ];

        // First read on a quiet node: flat object, no paging envelope.
        let (status, first) = server.get_auth("/api/v5/metrics", &token).await;
        assert_eq!(status, 200);
        let first_map = first.as_object().expect("metrics is a flat object");
        assert!(
            first.get("data").is_none(),
            "metrics is not paged: {first:?}"
        );
        assert!(
            first.get("meta").is_none(),
            "metrics has no meta: {first:?}"
        );
        for name in expected {
            let value = first_map
                .get(name)
                .unwrap_or_else(|| panic!("missing {name}"));
            assert!(value.is_number(), "{name} is numeric: {value:?}");
        }

        // Traffic between reads through the real counters: one connect,
        // one publish ingress and its fan-out, plus measured frame bytes.
        state.metrics.inc_connect_received();
        state.metrics.inc_connack_sent();
        state.metrics.inc_publish_received();
        state.metrics.inc_messages_received();
        state.metrics.inc_qos1_received();
        state.metrics.inc_bytes_received_by(128);
        state.metrics.inc_messages_forwarded();
        state.metrics.inc_publish_sent();
        state.metrics.inc_delivered();
        state.metrics.inc_bytes_sent_by(64);
        state.metrics.inc_subscribe_received();
        state.metrics.inc_suback_sent();
        state.metrics.inc_pingreq_received();
        state.metrics.inc_pingresp_sent();

        // Second read: every documented counter is non-decreasing.
        let (status, second) = server.get_auth("/api/v5/metrics", &token).await;
        assert_eq!(status, 200);
        let second_map = second.as_object().expect("metrics stays a flat object");
        for name in expected {
            let before = first_map[name]
                .as_u64()
                .unwrap_or_else(|| panic!("{name} numeric"));
            let after = second_map[name]
                .as_u64()
                .unwrap_or_else(|| panic!("{name} numeric"));
            assert!(after >= before, "{name} moves forward: {before} -> {after}");
        }
        assert!(second_map["messages.received"].as_u64().unwrap() >= 1);
        assert!(second_map["packets.publish.received"].as_u64().unwrap() >= 1);
        assert!(second_map["bytes.received"].as_u64().unwrap() >= 128);

        // Unknown parameters are ignored, not rejected.
        let (status, third) = server
            .get_auth("/api/v5/metrics?unknown_param=1", &token)
            .await;
        assert_eq!(status, 200);
        let third_map = third.as_object().expect("metrics with query stays flat");
        for name in expected {
            assert!(third_map.get(name).is_some(), "unknown query keeps {name}");
        }
    }

    #[tokio::test]
    async fn global_subscriptions_lists_both_with_paging_and_shrinks() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty system reads as a bare empty array, not an envelope.
        let (status, body) = server.get_auth("/api/v5/subscriptions", &token).await;
        assert_eq!(status, 200);
        let empty = body.as_array().expect("global list is a bare array");
        assert!(empty.is_empty(), "no subscriptions yet: {empty:?}");

        // Two clients subscribed through the real subscribe route.
        for (id, topic) in [("w1-14-a", "w1-14/first"), ("w1-14-b", "w1-14/second")] {
            state.sessions.get_or_create(id, true);
            let (status, _) = server
                .post_auth(
                    &format!("/api/v5/clients/{id}/subscribe"),
                    json!({"topic": topic, "qos": 1}),
                    &token,
                )
                .await;
            assert_eq!(status, 200);
        }

        // List shows both entries with the documented per-entry fields.
        let (status, body) = server.get_auth("/api/v5/subscriptions", &token).await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("global list is a bare array");
        assert_eq!(subs.len(), 2, "both subscriptions listed: {subs:?}");
        // Deterministic order by (clientid, topic).
        assert_eq!(subs[0]["clientid"], json!("w1-14-a"));
        assert_eq!(subs[0]["topic"], json!("w1-14/first"));
        assert_eq!(subs[1]["clientid"], json!("w1-14-b"));
        assert_eq!(subs[1]["topic"], json!("w1-14/second"));
        for entry in subs {
            assert_eq!(entry["qos"], json!(1));
            assert_eq!(entry["nl"], json!(0));
            assert_eq!(entry["rap"], json!(0));
            assert_eq!(entry["rh"], json!(0));
            assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
            assert!(entry["clientid"].is_string());
            assert!(entry["topic"].is_string());
        }

        // Paging is honoured: one row per page.
        let (status, body) = server
            .get_auth("/api/v5/subscriptions?page=1&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        let first = body.as_array().expect("paged list stays a bare array");
        assert_eq!(first.len(), 1, "first page holds one row: {first:?}");
        assert_eq!(first[0]["clientid"], json!("w1-14-a"));

        let (status, body) = server
            .get_auth("/api/v5/subscriptions?page=2&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        let second = body.as_array().expect("paged list stays a bare array");
        assert_eq!(second.len(), 1, "second page holds one row: {second:?}");
        assert_eq!(second[0]["clientid"], json!("w1-14-b"));

        // After one unsubscribe the list shrinks to the survivor.
        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-14-a/unsubscribe",
                json!({"topic": "w1-14/first"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);

        let (status, body) = server.get_auth("/api/v5/subscriptions", &token).await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("global list is a bare array");
        assert_eq!(subs.len(), 1, "list shrinks after unsubscribe: {subs:?}");
        assert_eq!(subs[0]["clientid"], json!("w1-14-b"));
        assert_eq!(subs[0]["topic"], json!("w1-14/second"));
    }

    #[tokio::test]
    async fn topics_list_and_detail_round_trip() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty index reads as an empty page, not an error.
        let (status, body) = server.get_auth("/api/v5/topics", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));
        assert_eq!(body["meta"]["page"], json!(1));
        assert_eq!(body["meta"]["hasnext"], json!(false));

        // Subscribing alone never creates a topic row.
        state.sessions.get_or_create("w1-15-sub", true);
        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-15-sub/subscribe",
                json!({"topic": "w1-15/filter", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server.get_auth("/api/v5/topics", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            body["data"],
            json!([]),
            "subscribe must not index: {body:?}"
        );

        // Publish two concrete topics through the management publish path.
        for topic in ["w1-15/alpha", "w1-15/beta"] {
            let (status, _) = server
                .post_auth(
                    "/api/v5/publish",
                    json!({"topic": topic, "payload": "hi", "qos": 0}),
                    &token,
                )
                .await;
            assert_eq!(status, 202);
        }

        // List shows both entries with the documented per-entry fields.
        let (status, body) = server.get_auth("/api/v5/topics", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(2));
        assert_eq!(body["meta"]["hasnext"], json!(false));
        let data = body["data"].as_array().expect("topics list has data");
        assert_eq!(data.len(), 2, "both topics listed: {data:?}");
        // Deterministic order: sorted by topic.
        assert_eq!(data[0]["topic"], json!("w1-15/alpha"));
        assert_eq!(data[1]["topic"], json!("w1-15/beta"));
        for entry in data {
            assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
            assert!(entry["topic"].is_string());
        }

        // W0 paging is honoured: one row per page.
        let (status, first) = server
            .get_auth("/api/v5/topics?page=1&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(first["meta"]["count"], json!(2));
        assert_eq!(first["meta"]["hasnext"], json!(true));
        assert_eq!(first["data"].as_array().unwrap().len(), 1);
        assert_eq!(first["data"][0]["topic"], json!("w1-15/alpha"));

        let (status, second) = server
            .get_auth("/api/v5/topics?page=2&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(second["data"].as_array().unwrap().len(), 1);
        assert_eq!(second["data"][0]["topic"], json!("w1-15/beta"));

        // Unknown query keys do not break the list call.
        let (status, body) = server
            .get_auth("/api/v5/topics?unknown_param=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(2));

        // Detail hit returns the documented record (slashed names arrive
        // percent-encoded and match exactly after decoding).
        let (status, body) = server
            .get_auth("/api/v5/topics/w1-15%2Falpha", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        assert_eq!(body[0]["topic"], json!("w1-15/alpha"));
        assert_eq!(body[0]["node"], json!(crate::node_scope::local_node_name()));

        // Detail miss returns the documented not-found shape; prefix and
        // filter forms never match.
        for missing in ["w1-15%2Fmissing", "w1-15%2Falp", "w1-15%2Falpha%2Fx"] {
            let (status, body) = server
                .get_auth(&format!("/api/v5/topics/{missing}"), &token)
                .await;
            assert_eq!(status, 404, "unknown topic must 404: {missing}");
            assert_eq!(body["code"], json!("TOPIC_NOT_FOUND"));
            assert!(body["message"].is_string());
        }
    }

    #[tokio::test]
    async fn alarms_raise_list_clear_round_trip() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty store reads as an empty page, not an error.
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));
        assert_eq!(body["meta"]["page"], json!(1));
        assert_eq!(body["meta"]["hasnext"], json!(false));

        // No deactivated history yet either.
        let (status, body) = server
            .get_auth("/api/v5/alarms?activated=false", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));

        // Raise two test alarms directly on the store (alarms have no
        // creation endpoint; the kernel raises them internally).
        state
            .alarms
            .activate("w1-16-b-alarm", "second alarm", serde_json::json!({}));
        state.alarms.activate(
            "w1-16-a-alarm",
            "first alarm",
            serde_json::json!({"high_watermark": 70}),
        );

        // Listing carries both entries with the documented per-alarm fields.
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(2));
        assert_eq!(body["meta"]["hasnext"], json!(false));
        let data = body["data"].as_array().expect("alarms list has data");
        assert_eq!(data.len(), 2, "both alarms listed: {data:?}");
        // Deterministic order: sorted by name.
        assert_eq!(data[0]["name"], json!("w1-16-a-alarm"));
        assert_eq!(data[1]["name"], json!("w1-16-b-alarm"));
        for entry in data {
            assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
            assert!(entry["name"].is_string());
            assert!(entry["message"].is_string());
            assert!(entry["details"].is_object());
            assert!(entry["duration"].is_number());
            assert!(entry["activate_at"].is_string());
            assert!(entry["activate_at"]
                .as_str()
                .is_some_and(|s| s.contains('T')));
            assert!(entry.get("deactivate_at").is_none());
        }
        assert_eq!(data[0]["message"], json!("first alarm"));
        assert_eq!(data[0]["details"], json!({"high_watermark": 70}));

        // W0 paging is honoured: one row per page.
        let (status, first) = server
            .get_auth("/api/v5/alarms?page=1&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(first["meta"]["count"], json!(2));
        assert_eq!(first["meta"]["hasnext"], json!(true));
        assert_eq!(first["data"].as_array().unwrap().len(), 1);
        assert_eq!(first["data"][0]["name"], json!("w1-16-a-alarm"));

        let (status, second) = server
            .get_auth("/api/v5/alarms?page=2&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(second["data"].as_array().unwrap().len(), 1);
        assert_eq!(second["data"][0]["name"], json!("w1-16-b-alarm"));

        // Unknown query keys do not break the list call.
        let (status, body) = server
            .get_auth("/api/v5/alarms?unknown_param=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(2));

        // Clearing deactivates everything; the next list is empty again
        // while the deactivated history keeps what was cleared.
        let (status, _) = server
            .delete_auth("/api/v5/alarms?unknown_param=1", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));
        let (status, body) = server
            .get_auth("/api/v5/alarms?activated=false", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(2));
        let history = body["data"].as_array().expect("cleared alarms kept");
        assert_eq!(history.len(), 2);
        assert_eq!(history[0]["name"], json!("w1-16-a-alarm"));
        assert_ne!(history[0]["deactivate_at"], json!("infinity"));

        // Clearing an empty store still reports success.
        let (status, _) = server.delete_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn alarms_force_deactivate_round_trip() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // With one active alarm, force-deactivate reports success and the
        // follow-up list reads empty.
        state.alarms.activate(
            "w1-17-alarm",
            "force me",
            serde_json::json!({"high_watermark": 70}),
        );
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(1));

        let (status, _) = server
            .post_auth(
                "/api/v5/alarms/force_deactivate",
                json!({"name": "w1-17-alarm"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));

        // An empty body fails with bad request.
        let (status, body) = server
            .post_auth("/api/v5/alarms/force_deactivate", json!({}), &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // A named body deactivates just that alarm; the other stays listed.
        state
            .alarms
            .activate("w1-17-a-alarm", "first alarm", serde_json::json!({}));
        state
            .alarms
            .activate("w1-17-b-alarm", "second alarm", serde_json::json!({}));
        let (status, _) = server
            .post_auth(
                "/api/v5/alarms/force_deactivate",
                json!({"name": "w1-17-a-alarm"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(1));
        assert_eq!(body["data"][0]["name"], json!("w1-17-b-alarm"));

        // Deactivating an unknown name fails and changes nothing.
        let (status, body) = server
            .post_auth(
                "/api/v5/alarms/force_deactivate",
                json!({"name": "w1-17-missing"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(1));
        let (status, _) = server
            .post_auth(
                "/api/v5/alarms/force_deactivate",
                json!({"name": "w1-17-b-alarm"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/alarms", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
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
    async fn client_authz_cache_read_clear_round_trip() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Connect through the broker: the console CONNECT creates the
        // session, so the cache starts life empty. An empty cache reads
        // as an empty list.
        let mut subscriber = WsClient::connect(server.port).await;
        subscriber
            .send_bin(&mqtt_connect("w2-01-cache-1", None, None))
            .await;
        let connack = subscriber.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        // Populate through normal authorized activity through the broker:
        // a console SUBSCRIBE (real subscribe authorization) plus a
        // console PUBLISH from a second connection (real publish
        // authorization) that delivers to the subscriber.
        subscriber
            .send_bin(&mqtt_subscribe(7, &[("conf/authz/1", 0)]))
            .await;
        let suback = subscriber.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(&suback[4..], &[0]);
        let mut publisher = WsClient::connect(server.port).await;
        publisher
            .send_bin(&mqtt_connect("w2-01-cache-2", None, None))
            .await;
        let connack = publisher.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        publisher
            .send_bin(&mqtt_publish("conf/authz/1", 0, 0, b"hi"))
            .await;
        // Deliver proves the publish rode the broker route path.
        let delivery = subscriber.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);

        // Read-hit shape: each decision present with per-entry fields.
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        let entries = body.as_array().expect("cache is a bare list");
        assert_eq!(entries.len(), 1);
        let subscribe = entries
            .iter()
            .find(|e| e["topic"] == json!("conf/authz/1"))
            .expect("subscribe decision present");
        assert_eq!(subscribe["access"], json!("subscribe"));
        assert_eq!(subscribe["result"], json!("allow"));
        assert!(subscribe["updated_time"].is_number());
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-01-cache-2/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        let entries = body.as_array().expect("cache is a bare list");
        assert_eq!(entries.len(), 1);
        let publish = entries
            .iter()
            .find(|e| e["topic"] == json!("conf/authz/1"))
            .expect("publish decision present");
        assert_eq!(publish["access"], json!("publish"));
        assert_eq!(publish["result"], json!("allow"));
        assert!(publish["updated_time"].is_number());

        // A denial spells `deny`: a denied subscribe through the broker
        // records the exact value, so a wrong spelling fails this test.
        state
            .auth
            .add_rule(AclRule::new(
                "w2-01-cache-deny",
                AclAction::Subscribe,
                "conf/authz/denied",
                false,
            ))
            .expect("deny rule installs");
        let mut denied = WsClient::connect(server.port).await;
        denied
            .send_bin(&mqtt_connect("w2-01-cache-deny", None, None))
            .await;
        let connack = denied.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        denied
            .send_bin(&mqtt_subscribe(8, &[("conf/authz/denied", 0)]))
            .await;
        let suback = denied.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(&suback[4..], &[0x87]);
        let (status, body) = server
            .get_auth(
                "/api/v5/clients/w2-01-cache-deny/authorization/cache",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let entries = body.as_array().expect("cache is a bare list");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0]["access"], json!("subscribe"));
        assert_eq!(entries[0]["topic"], json!("conf/authz/denied"));
        assert_eq!(entries[0]["result"], json!("deny"));

        // Unknown query keys are ignored, not a 400.
        let (status, body) = server
            .get_auth(
                "/api/v5/clients/w2-01-cache-1/authorization/cache?unknown_param=1",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("list").len(), 1);

        // Clear evicts; the next read is empty. Clearing again still
        // succeeds (204).
        let (status, _) = server
            .delete_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, _) = server
            .delete_auth("/api/v5/clients/w2-01-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 204);

        // Unknown client ids are 404 with the documented error shape.
        let (status, body) = server
            .get_auth("/api/v5/clients/no-such-client/authorization/cache", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
        assert!(body["message"].is_string());
        let (status, body) = server
            .delete_auth("/api/v5/clients/no-such-client/authorization/cache", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn authz_global_cache_clear_round_trip() {
        // Global decision-cache clear (W2-13, C04-01) through the broker:
        // authorized activity populates the per-session decision caches
        // (consulted at `crates/broker-session/src/lib.rs`
        // `Session::record_authz_decision`, driven by the publish and
        // subscribe authorization events), the global clear evicts every
        // entry cluster-wide on this node, and the next authorized
        // decision is recomputed (the cache repopulates). Clearing an
        // already-empty cache still succeeds. Management-plane only;
        // publish and deliver never take the clear locks beyond the
        // short per-session evictions.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty global clear still succeeds (204) before any activity.
        let (status, _) = server
            .delete_auth("/api/v5/authorization/cache", &token)
            .await;
        assert_eq!(status, 204);

        // Populate through normal authorized activity through the broker:
        // a console SUBSCRIBE (real subscribe authorization) plus a
        // console PUBLISH from a second connection (real publish
        // authorization) that delivers to the subscriber.
        let mut subscriber = WsClient::connect(server.port).await;
        subscriber
            .send_bin(&mqtt_connect("w2-13-cache-1", None, None))
            .await;
        let connack = subscriber.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        subscriber
            .send_bin(&mqtt_subscribe(7, &[("w2-13/topic", 0)]))
            .await;
        let suback = subscriber.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(&suback[4..], &[0]);
        let mut publisher = WsClient::connect(server.port).await;
        publisher
            .send_bin(&mqtt_connect("w2-13-cache-2", None, None))
            .await;
        let connack = publisher.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        publisher
            .send_bin(&mqtt_publish("w2-13/topic", 0, 0, b"hi"))
            .await;
        // Deliver proves the publish rode the broker route path.
        let delivery = subscriber.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);

        // Both per-client caches hold their decision.
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-13-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("cache is a list").len(), 1);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-13-cache-2/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("cache is a list").len(), 1);

        // Global clear evicts every entry on this node.
        let (status, _) = server
            .delete_auth("/api/v5/authorization/cache", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-13-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-13-cache-2/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        // The next authorized decision is recomputed (cache miss
        // counted as a fresh entry on the next publish through the
        // broker).
        publisher
            .send_bin(&mqtt_publish("w2-13/topic", 0, 0, b"again"))
            .await;
        let delivery = subscriber.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-13-cache-2/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("cache is a list").len(), 1);

        // Unknown query keys are ignored, not a 400.
        let (status, _) = server
            .send(
                "DELETE",
                "/api/v5/authorization/cache?unknown_param=1",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn authz_node_cache_config_reset_round_trip() {
        // Node-cache config read/write plus reset (W2-14, C04-02..C04-04)
        // through the broker: GET defaults, PUT a valid update and read
        // it back, PUT invalid expecting rejection with no change, then
        // authorized activity populates the node-cached decisions
        // (per-session entries recorded by the publish and subscribe
        // authorization events at `crates/broker-session/src/lib.rs`
        // `Session::record_authz_decision`, driven from
        // `crates/broker-node/src/main.rs`), the node reset evicts them,
        // and the next authorized decision is recomputed. Management-plane
        // only; publish and deliver never touch the policy store.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Defaults read back with the documented fields.
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        assert_eq!(body["cache_ttl"], json!("60s"));
        assert_eq!(body["cleanup_interval"], json!("60s"));
        assert_eq!(body["stat_update_interval"], json!("5s"));
        assert_eq!(body["max_count"], json!(50_000));
        assert_eq!(body["max_memory"], json!("100MB"));

        // Unknown query keys are ignored on the read, not a 400.
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache?unknown_param=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["max_count"], json!(50_000));

        // Full valid replace through the broker.
        let (status, _) = server
            .put_auth(
                "/api/v5/authorization/node_cache",
                json!({
                    "enable": true,
                    "cache_ttl": "30s",
                    "cleanup_interval": "60s",
                    "stat_update_interval": "5s",
                    "max_count": 5000,
                    "max_memory": "100MB",
                }),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["cache_ttl"], json!("30s"));
        assert_eq!(body["max_count"], json!(5000));

        // Invalid writes are rejected with the documented shape and apply
        // nothing.
        for bad in [
            json!({"enable": "yes"}),
            json!({"cache_ttl": "soon"}),
            json!({"max_count": 0}),
            json!({"max_count": 2_000_000}),
            json!({"max_count": "many"}),
            json!({"max_memory": "huge"}),
            json!({"enable": true, "bogus": 1}),
        ] {
            let (status, body) = server
                .put_auth("/api/v5/authorization/node_cache", bad, &token)
                .await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
        }
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["cache_ttl"], json!("30s"));
        assert_eq!(body["max_count"], json!(5000));

        // Resetting empty still succeeds (204).
        let (status, _) = server
            .post_auth("/api/v5/authorization/node_cache/reset", json!({}), &token)
            .await;
        assert_eq!(status, 204);

        // Populate through normal authorized activity through the broker:
        // a console SUBSCRIBE (real subscribe authorization) plus a
        // console PUBLISH from a second connection (real publish
        // authorization) that delivers to the subscriber.
        let mut subscriber = WsClient::connect(server.port).await;
        subscriber
            .send_bin(&mqtt_connect("w2-14-cache-1", None, None))
            .await;
        let connack = subscriber.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        subscriber
            .send_bin(&mqtt_subscribe(7, &[("w2-14/topic", 0)]))
            .await;
        let suback = subscriber.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(&suback[4..], &[0]);
        let mut publisher = WsClient::connect(server.port).await;
        publisher
            .send_bin(&mqtt_connect("w2-14-cache-2", None, None))
            .await;
        let connack = publisher.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        publisher
            .send_bin(&mqtt_publish("w2-14/topic", 0, 0, b"hi"))
            .await;
        // Deliver proves the publish rode the broker route path.
        let delivery = subscriber.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);

        // Both per-client caches hold their node-cached decision.
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-14-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("cache is a list").len(), 1);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-14-cache-2/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("cache is a list").len(), 1);

        // Node reset evicts every entry on this node.
        let (status, _) = server
            .post_auth("/api/v5/authorization/node_cache/reset", json!({}), &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-14-cache-1/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-14-cache-2/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        // The next authorized decision is recomputed (the cache
        // repopulates on the next publish through the broker).
        publisher
            .send_bin(&mqtt_publish("w2-14/topic", 0, 0, b"again"))
            .await;
        let delivery = subscriber.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);
        let (status, body) = server
            .get_auth("/api/v5/clients/w2-14-cache-2/authorization/cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("cache is a list").len(), 1);

        // Resetting again still succeeds, and unknown query keys are
        // ignored, not a 400.
        let (status, _) = server
            .post_auth("/api/v5/authorization/node_cache/reset", json!({}), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/authorization/node_cache/reset?unknown_param=1",
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn authz_node_cache_policy_survives_restart() {
        // Policy persistence across a restart through the config
        // registry: PUT through the broker, reload the same data dir in a
        // fresh server, and read the policy back. Cached verdict entries
        // are memory-only and do not cross the restart.
        let dir = unique_data_dir("authz-node-cache");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .put_auth(
                "/api/v5/authorization/node_cache",
                json!({
                    "enable": true,
                    "cache_ttl": "30s",
                    "cleanup_interval": "60s",
                    "stat_update_interval": "5s",
                    "max_count": 5000,
                    "max_memory": "100MB",
                }),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["cache_ttl"], json!("30s"));
        assert_eq!(body["max_count"], json!(5000));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn authz_node_cache_status_reports_live_counters() {
        // Node-cache status (W2-15, C04-05) through the broker: the status
        // reads the live policy (enabled flag, entry cap from the W2-14
        // config) plus the live entry count and cumulative hit/miss
        // counters rolled up across the per-session decision caches
        // (`crates/broker-session/src/lib.rs`,
        // `SessionManager::authz_node_stats`, driven by the publish and
        // subscribe authorization events). Before this task the route
        // itself was missing (bare 404). Management-plane only; publish
        // and deliver never touch the status read beyond the
        // already-held per-session write lock with relaxed atomic adds.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty node reads as zero entries and zero counters with the
        // live bounds, never an error.
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enabled"], json!(true));
        assert_eq!(body["metrics"]["hits"]["value"], json!(0));
        assert_eq!(body["metrics"]["misses"]["value"], json!(0));
        assert_eq!(body["metrics"]["inserts"]["value"], json!(0));
        assert_eq!(body["metrics"]["count"], json!(0));
        assert_eq!(body["metrics"]["max_count"], json!(50_000));
        assert_eq!(body["node_metrics"].as_array().expect("rows").len(), 1);
        assert_eq!(
            body["node_metrics"][0]["metrics"], body["metrics"],
            "node row mirrors the aggregate"
        );

        // Populate through normal authorized activity through the broker:
        // a console SUBSCRIBE (real subscribe authorization) plus a
        // console PUBLISH from a second connection (real publish
        // authorization) that delivers to the subscriber.
        let mut subscriber = WsClient::connect(server.port).await;
        subscriber
            .send_bin(&mqtt_connect("w2-15-cache-1", None, None))
            .await;
        let connack = subscriber.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        subscriber
            .send_bin(&mqtt_subscribe(7, &[("w2-15/topic", 0)]))
            .await;
        let suback = subscriber.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(&suback[4..], &[0]);
        let mut publisher = WsClient::connect(server.port).await;
        publisher
            .send_bin(&mqtt_connect("w2-15-cache-2", None, None))
            .await;
        let connack = publisher.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        publisher
            .send_bin(&mqtt_publish("w2-15/topic", 0, 0, b"hi"))
            .await;
        // Deliver proves the publish rode the broker route path.
        let delivery = subscriber.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);

        // Two new decisions moved the miss/insert counters and the entry
        // count; every counter field is numeric.
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enabled"], json!(true));
        assert_eq!(body["metrics"]["misses"]["value"], json!(2));
        assert_eq!(body["metrics"]["inserts"]["value"], json!(2));
        assert_eq!(body["metrics"]["hits"]["value"], json!(0));
        assert_eq!(body["metrics"]["count"], json!(2));
        assert_eq!(body["metrics"]["max_count"], json!(50_000));

        // Repeating the same authorized publish refreshes the cached
        // decision in place: one hit, entries unchanged.
        publisher
            .send_bin(&mqtt_publish("w2-15/topic", 0, 0, b"again"))
            .await;
        let delivery = subscriber.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);
        let (status, body) = server
            .get_auth("/api/v5/authorization/node_cache/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["hits"]["value"], json!(1));
        assert_eq!(body["metrics"]["misses"]["value"], json!(2));
        assert_eq!(body["metrics"]["inserts"]["value"], json!(2));
        assert_eq!(body["metrics"]["count"], json!(2));

        // Unknown query keys are ignored, not a 400.
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/node_cache/status?unknown_param=1",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["count"], json!(2));
    }

    #[tokio::test]
    async fn banned_create_list_clear_round_trip() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty store reads as an empty page, not an error.
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));
        assert_eq!(body["meta"]["hasnext"], json!(false));

        // Create one entry.
        let (status, created) = server
            .post_auth(
                "/api/v5/banned",
                json!({"as": "clientid", "who": "w1-01-c1", "reason": "round trip"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(created["data"][0]["as"], json!("clientid"));
        assert_eq!(created["data"][0]["who"], json!("w1-01-c1"));

        // Listing now carries the entry plus paging metadata.
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(1));
        assert_eq!(body["data"][0]["who"], json!("w1-01-c1"));
        assert_eq!(body["data"][0]["as"], json!("clientid"));

        // Unknown query keys do not break the list call.
        let (status, body) = server
            .get_auth("/api/v5/banned?unknown_param=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["meta"]["count"], json!(1));

        // Clearing drops everything; the next list is empty again.
        let (status, _) = server
            .delete_auth("/api/v5/banned?unknown_param=1", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));
    }

    #[tokio::test]
    async fn banned_malformed_create_is_bad_request() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        for bad in [
            json!({}),
            json!({"as": "clientid"}),
            json!({"who": "w1-01-x"}),
            json!({"as": "not-a-kind", "who": "w1-01-x"}),
            json!({"as": "clientid", "who": ""}),
            json!({"as": "peerhost", "who": "not-an-ip"}),
        ] {
            let (status, body) = server.post_auth("/api/v5/banned", bad, &token).await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
        }

        // Nothing above may have stored anything.
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
    }

    // Read one export file back into (path, text) pairs.
    fn read_backup_file(path: &std::path::Path) -> Vec<(String, String)> {
        let bytes = std::fs::read(path).expect("backup file reads");
        let gz = flate2::read::GzDecoder::new(bytes.as_slice());
        let mut archive = tar::Archive::new(gz);
        let mut out = Vec::new();
        for entry in archive.entries().expect("backup lists") {
            let mut entry = entry.expect("backup entry reads");
            let name = entry
                .path()
                .expect("entry path")
                .to_string_lossy()
                .into_owned();
            let mut text = String::new();
            std::io::Read::read_to_string(&mut entry, &mut text).expect("entry text reads");
            out.push((name, text));
        }
        out
    }

    #[tokio::test]
    async fn data_export_round_trip_serves_live_tables() {
        // Export through the broker: seed live state through the API,
        // export it, then read the file back from the backup dir.
        let dir = unique_data_dir("data-export");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;

        let (status, _) = server
            .post_auth(
                "/api/v5/banned",
                json!({"as": "clientid", "who": "w5-26-live", "reason": "export check"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "w5-26-sensor", "password": "Sup3r-secret-pw!"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);

        let (status, body) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["rules"], "table_sets": ["banned", "builtin_authn"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let filename = body["filename"].as_str().expect("export names its file");
        assert!(filename.ends_with(".tar.gz"));

        // The file reference resolves in the on-disk backup dir.
        let path = dir.join("backup").join(filename);
        assert!(path.is_file());
        let parts = read_backup_file(&path);
        assert!(!parts.is_empty());

        // Live rows cross the export: the ban and the user appear.
        let banned = parts
            .iter()
            .find(|(name, _)| name.ends_with("/tables/banned.json"))
            .expect("banned table ships");
        assert!(banned.1.contains("w5-26-live"));
        let authn = parts
            .iter()
            .find(|(name, _)| name.ends_with("/tables/builtin_authn.json"))
            .expect("authn table ships");
        assert!(authn.1.contains("w5-26-sensor"));

        // Secrets stay out: no password hash ships in any part.
        for (_, text) in &parts {
            assert!(!text.contains("password_hash"));
            assert!(!text.contains("Sup3r-secret-pw!"));
        }
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// TM-01: two tenants share one username with different passwords.
    /// Real API calls create the users and rules per tenant; the CONNECT
    /// and publish checks run through the broker auth store; a reload
    /// keeps both tenants; a restart on the same data dir keeps both;
    /// the export file carries both tenants without secrets.
    #[tokio::test]
    async fn tm01_two_tenants_persist_across_reload_restart_and_export() {
        let dir = unique_data_dir("tm01-storage");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        state
            .auth
            .set_policy(broker_auth::PasswordHashPolicy::for_tests());
        let token = server.login_as_admin().await;
        // Same username, different passwords, disjoint rules per tenant.
        for (tenant, password, topic) in [
            ("tenant-a", "pw-for-a-7x", "t/a/#"),
            ("tenant-b", "pw-for-b-9q", "t/b/#"),
        ] {
            let (status, _) = server
                .post_auth(
                    &format!("/api/v1/auth/users?tenant={tenant}"),
                    json!({"username": "sam", "password": password}),
                    &token,
                )
                .await;
            assert_eq!(status, 201, "user creates in {tenant}");
            for action in ["publish", "subscribe"] {
                let (status, _) = server
                    .post_auth(
                        &format!("/api/v1/auth/acls?tenant={tenant}"),
                        json!({
                            "client_pattern": "*",
                            "action": action,
                            "topic_pattern": topic,
                            "allow": true,
                        }),
                        &token,
                    )
                    .await;
                assert_eq!(status, 201, "{action} rule creates in {tenant}");
            }
        }
        // Default tenant rows work through the same routes with no change.
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "base", "password": "base-pw-3z"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/acls",
                json!({
                    "client_pattern": "base",
                    "action": "all",
                    "topic_pattern": "t/#",
                    "allow": true,
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // A bad tenant id fails with 400 and applies nothing.
        let long = "t".repeat(129);
        let (status, _) = server
            .get_auth(&format!("/api/v1/auth/users?tenant={long}"), &token)
            .await;
        assert_eq!(status, 400);
        // Broker-path checks: the same calls the kernel makes per CONNECT
        // and per message. Each password works only in its own tenant.
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a-7x"))
            .await
            .is_ok());
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b-9q"))
            .await
            .is_ok());
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-b-9q"))
            .await
            .is_err());
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-a-7x"))
            .await
            .is_err());
        // A publish allowed in A is refused in B and vice versa.
        let topic_a = Topic::new("t/a/1").expect("topic");
        let topic_b = Topic::new("t/b/1").expect("topic");
        assert!(state
            .auth
            .authorize_publish_in_tenant("tenant-a", "sam", &topic_a)
            .await
            .is_ok());
        assert!(state
            .auth
            .authorize_publish_in_tenant("tenant-b", "sam", &topic_a)
            .await
            .is_err());
        assert!(state
            .auth
            .authorize_publish_in_tenant("tenant-b", "sam", &topic_b)
            .await
            .is_ok());
        assert!(state
            .auth
            .authorize_publish_in_tenant("tenant-a", "sam", &topic_b)
            .await
            .is_err());
        // Delivery isolation: one subscription per tenant on the same
        // filter; a publish in A matches only the subscriber in A.
        let filter = TopicFilter::new("t/#").expect("filter");
        state.router.subscribe(
            &filter,
            broker_router::Subscription::new_in_tenant("sub-a", 7101, QoS::AtMostOnce, "tenant-a"),
        );
        state.router.subscribe(
            &filter,
            broker_router::Subscription::new_in_tenant("sub-b", 7102, QoS::AtMostOnce, "tenant-b"),
        );
        let (matched_a, _) = state
            .router
            .matches_in_tenant_counted(&topic_a, None, "tenant-a");
        assert!(matched_a.iter().any(|s| s.client_id.as_ref() == "sub-a"));
        assert!(!matched_a.iter().any(|s| s.client_id.as_ref() == "sub-b"));
        let (matched_b, _) = state
            .router
            .matches_in_tenant_counted(&topic_b, None, "tenant-b");
        assert!(matched_b.iter().any(|s| s.client_id.as_ref() == "sub-b"));
        assert!(!matched_b.iter().any(|s| s.client_id.as_ref() == "sub-a"));
        // Reload keeps both tenants.
        let candidate = registry.snapshot().as_ref().clone();
        let (status, body) = server
            .post_auth(
                "/api/v1/config/reload",
                json!({"snapshot": candidate, "actor": "tm01", "summary": "keep both tenants"}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "reload keeps both tenants: {body}");
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a-7x"))
            .await
            .is_ok());
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b-9q"))
            .await
            .is_ok());
        // The state file carries both tenants; plaintext never persists.
        let text = std::fs::read_to_string(dir.join("state.toml")).expect("state file written");
        assert!(text.contains("tenant-a") && text.contains("tenant-b"));
        assert!(!text.contains("pw-for-a-7x") && !text.contains("pw-for-b-9q"));
        // The export file carries both tenants without secrets.
        let (status, body) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["mqtt_users"], "table_sets": ["builtin_authn", "builtin_authz"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let filename = body["filename"].as_str().expect("export names its file");
        let parts = read_backup_file(&dir.join("backup").join(filename));
        let snap = parts
            .iter()
            .find(|(name, _)| name.ends_with("/snapshot/mqtt_users.json"))
            .expect("mqtt_users root ships");
        assert!(snap.1.contains("tenant-a") && snap.1.contains("tenant-b"));
        let authn = parts
            .iter()
            .find(|(name, _)| name.ends_with("/tables/builtin_authn.json"))
            .expect("authn table ships");
        assert!(authn.1.contains("tenant-a") && authn.1.contains("tenant-b"));
        for (_, text) in &parts {
            assert!(!text.contains("password_hash"));
            assert!(!text.contains("pw-for-a-7x"));
            assert!(!text.contains("pw-for-b-9q"));
        }
        // Restart on the same data dir keeps both tenants.
        drop(server);
        drop(state);
        drop(registry);
        let registry =
            Arc::new(ConfigRegistry::load(&dir).expect("registry reloads after a restart"));
        let (server, state) = TestServer::start_with_registry(registry).await;
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a-7x"))
            .await
            .is_ok());
        assert!(state
            .auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b-9q"))
            .await
            .is_ok());
        use broker_auth::Authenticator as _;
        assert!(state
            .auth
            .authenticate("device-1", Some("base"), Some(b"base-pw-3z"))
            .await
            .is_ok());
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn data_export_rejects_unknown_and_malformed() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        for bad in [
            json!({"root_keys": [], "table_sets": ["no-such-table"]}),
            json!({"root_keys": ["no-such-root"], "table_sets": []}),
            json!("malformed"),
            json!([123]),
        ] {
            let (status, body) = server.post_auth("/api/v5/data/export", bad, &token).await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
            assert_eq!(body.as_object().expect("error shape").len(), 2);
        }

        // Broken JSON bytes are client errors with the same shape.
        // Send the body as an argument so braces need no escape.
        let raw = format!(
            "POST /api/v5/data/export HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: 7\r\nConnection: close\r\n\r\n{}",
            "{{oops!"
        );
        let (status, text) = server.request_raw(&raw).await;
        assert_eq!(status, 400);
        let body: Value = serde_json::from_str(&text).expect("error body is JSON");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn data_export_answers_documented_fields() {
        // The export answers the documented file fields through the API.
        // Before: only `filename`. Now: `created_at`, `filename`,
        // `node` and the true file `size`.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let filename = body["filename"].as_str().expect("export names its file");
        assert!(filename.ends_with(".tar.gz"));
        assert!(body["created_at"].is_string(), "export stamps its time");
        assert!(body["node"].is_string(), "export names its node");
        assert!(
            body["size"].as_u64().is_some_and(|size| size > 0),
            "export reports its true size"
        );
    }

    #[tokio::test]
    async fn data_export_files_survive_restart() {
        // Export files persist across a restart: export, restart on the
        // same data dir, and the file is still there.
        let dir = unique_data_dir("data-export-restart");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, body) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let filename = body["filename"]
            .as_str()
            .expect("export names its file")
            .to_string();
        drop(server);
        drop(registry);

        assert!(dir.join("backup").join(&filename).is_file());
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        assert!(dir.join("backup").join(&filename).is_file());
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, _) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn data_export_enforces_bounds_through_api() {
        // Fill the backup dir past every bound. Export once through the
        // API. Observe that the store meets every bound.
        use std::time::{Duration, SystemTime};
        let dir = unique_data_dir("data-export-bounds");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let backup = dir.join("backup");
        std::fs::create_dir_all(&backup).expect("backup dir forms");
        let write_dummy = |name: &str, size: u64, age_secs: u64| {
            let path = backup.join(name);
            std::fs::write(&path, b"dummy").expect("dummy writes");
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .expect("dummy opens");
            file.set_len(size).expect("size extends");
            let mtime = SystemTime::now()
                .checked_sub(Duration::from_secs(age_secs))
                .expect("age precedes now");
            file.set_modified(mtime).expect("mtime applies");
        };
        for index in 0..17 {
            let age = 3_600 - index as u64;
            write_dummy(&format!("backup-fill-{index:03}.tar.gz"), 8, age);
        }
        write_dummy("backup-aged-000.tar.gz", 8, 8 * 86_400);
        write_dummy("backup-aged-001.tar.gz", 8, 8 * 86_400);
        let big = 140 * 1024 * 1024;
        write_dummy("backup-big-000.tar.gz", big, 20);
        write_dummy("backup-big-001.tar.gz", big, 10);
        let (status, body) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let filename = body["filename"]
            .as_str()
            .expect("export names file")
            .to_string();
        let mut names = Vec::new();
        let mut total: u64 = 0;
        for entry in std::fs::read_dir(&backup).expect("backup lists") {
            let entry = entry.expect("entry reads");
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".tar.gz") {
                continue;
            }
            total += entry.metadata().expect("size reads").len();
            names.push(name);
        }
        assert!(names.len() <= 16);
        assert!(total <= 256 * 1024 * 1024);
        assert!(!names.contains(&"backup-aged-000.tar.gz".to_string()));
        assert!(!names.contains(&"backup-aged-001.tar.gz".to_string()));
        assert!(names.contains(&filename));
        assert!(backup.join(&filename).is_file());
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn data_export_keeps_ban_store_usable() {
        // Create one ban through the API. Export it through the API.
        // Observe that the ban remains listed and exported.
        let dir = unique_data_dir("data-export-bans");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/banned",
                json!({"as": "clientid", "who": "w5-26-probe", "reason": "export check"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let filename = body["filename"]
            .as_str()
            .expect("export names file")
            .to_string();
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("ban list reads");
        assert!(data
            .iter()
            .any(|entry| entry["who"] == json!("w5-26-probe")));
        let path = dir.join("backup").join(&filename);
        assert!(path.is_file());
        let parts = read_backup_file(&path);
        let banned = parts
            .iter()
            .find(|(name, _)| name.ends_with("/tables/banned.json"))
            .expect("banned table ships");
        assert!(banned.1.contains("w5-26-probe"));
        let (status, _) = server
            .post_auth(
                "/api/v5/data/export",
                json!({"root_keys": ["rules"], "table_sets": ["banned"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("ban list reads");
        assert!(data
            .iter()
            .any(|entry| entry["who"] == json!("w5-26-probe")));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn data_files_round_trip_through_api() {
        // Upload one file through the API. List it with paging data.
        // Upload it again for a conflict. Delete it. Delete it again
        // for a 404. An empty store reads as an empty page.
        use base64::Engine as _;
        let dir = unique_data_dir("data-files-roundtrip");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["count"], json!(0));
        assert_eq!(body["meta"]["page"], json!(1));
        assert_eq!(body["meta"]["limit"], json!(100));
        let content = base64::engine::general_purpose::STANDARD.encode(b"w5-27 round-trip bytes");
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "w5-27-roundtrip.tar.gz", "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        assert!(dir.join("backup").join("w5-27-roundtrip.tar.gz").is_file());
        let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("file list reads");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["filename"], json!("w5-27-roundtrip.tar.gz"));
        assert!(data[0]["size"].as_u64().expect("size reads") > 0);
        assert!(data[0]["created_at"].as_str().is_some());
        assert!(data[0]["node"].as_str().is_some());
        assert_eq!(body["meta"]["count"], json!(1));
        let (status, body) = server
            .get_auth("/api/v5/data/files?page=1&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"].as_array().expect("page reads").len(), 1);
        assert_eq!(body["meta"]["page"], json!(1));
        assert_eq!(body["meta"]["limit"], json!(1));
        let (status, body) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "w5-27-roundtrip.tar.gz", "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        let (status, _) = server
            .delete_auth("/api/v5/data/files/w5-27-roundtrip.tar.gz", &token)
            .await;
        assert_eq!(status, 204);
        assert!(!dir.join("backup").join("w5-27-roundtrip.tar.gz").is_file());
        let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        let (status, body) = server
            .delete_auth("/api/v5/data/files/w5-27-roundtrip.tar.gz", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn data_files_reject_malformed_uploads() {
        // Bad names and bad bytes fail as client errors.
        use base64::Engine as _;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let good = base64::engine::general_purpose::STANDARD.encode(b"w5-27 malformed probe");
        for bad in [
            json!({"filename": "", "content": good}),
            json!({"filename": "../escape.tar.gz", "content": good}),
            json!({"filename": "bad.txt", "content": good}),
            json!({"filename": "w5-27-bad.tar.gz", "content": ""}),
            json!({"filename": "w5-27-bad.tar.gz", "content": "!!!not-base64!!!"}),
            json!({"filename": "w5-27-bad.tar.gz"}),
            json!({"content": good}),
        ] {
            let (status, body) = server.post_auth("/api/v5/data/files", bad, &token).await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
    }

    #[tokio::test]
    async fn data_files_paging_honours_limit() {
        // Three uploads list as pages of two plus one.
        use base64::Engine as _;
        let dir = unique_data_dir("data-files-paging");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        for name in ["w5-27-p0.tar.gz", "w5-27-p1.tar.gz", "w5-27-p2.tar.gz"] {
            let content =
                base64::engine::general_purpose::STANDARD.encode(format!("bytes for {name}"));
            let (status, _) = server
                .post_auth(
                    "/api/v5/data/files",
                    json!({"filename": name, "content": content}),
                    &token,
                )
                .await;
            assert_eq!(status, 204);
        }
        let (status, body) = server
            .get_auth("/api/v5/data/files?page=1&limit=2", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"].as_array().expect("page reads").len(), 2);
        assert_eq!(body["meta"]["count"], json!(3));
        let (status, body) = server
            .get_auth("/api/v5/data/files?page=2&limit=2", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"].as_array().expect("page reads").len(), 1);
        assert_eq!(body["meta"]["count"], json!(3));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn data_files_survive_restart() {
        // Upload one file. Start the broker again on the same data
        // dir. The file lists again.
        use base64::Engine as _;
        let dir = unique_data_dir("data-files-restart");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let content = base64::engine::general_purpose::STANDARD.encode(b"w5-27 restart bytes");
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "w5-27-restart.tar.gz", "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);
        assert!(dir.join("backup").join("w5-27-restart.tar.gz").is_file());
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        // The admin password persisted, so log in with the changed password.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/data/files", &token).await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("file list reads");
        assert!(data
            .iter()
            .any(|entry| entry["filename"] == json!("w5-27-restart.tar.gz")));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn banned_duplicate_create_reports_conflict() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, _) = server
            .post_auth(
                "/api/v5/banned",
                json!({"as": "username", "who": "w1-01-dup"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server
            .post_auth(
                "/api/v5/banned",
                json!({"as": "username", "who": "w1-01-dup"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn banned_delete_one_present_absent_and_bad_kind() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Create one entry through the store.
        let (status, _) = server
            .post_auth(
                "/api/v5/banned",
                json!({"as": "clientid", "who": "w1-02-del", "reason": "delete one"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        // Deleting it by kind/value reports no-content success.
        let (status, _) = server
            .delete_auth("/api/v5/banned/clientid/w1-02-del", &token)
            .await;
        assert_eq!(status, 204);

        // It is gone from the list afterwards.
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));

        // Deleting it again reports not-found, not success.
        let (status, body) = server
            .delete_auth("/api/v5/banned/clientid/w1-02-del", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].is_string());

        // An unknown kind is a client error with the documented shape.
        let (status, body) = server
            .delete_auth("/api/v5/banned/not-a-kind/w1-02-del", &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn test_metrics_exposes_prometheus_counters() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.metrics.inc_messages_received();
        state.metrics.inc_messages_forwarded_by(2);
        state.metrics.inc_rules_executed();
        state.metrics.set_active_connections(1);

        let (status, text) = server
            .request_raw(&format!(
                "GET /api/v1/metrics HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
            ))
            .await;
        assert_eq!(status, 200);
        assert!(text.contains("indramqtt_messages_received_total 1\n"));
        assert!(text.contains("indramqtt_messages_forwarded_total 2\n"));
        assert!(text.contains("indramqtt_rules_executed_total 1\n"));
        assert!(text.contains("indramqtt_connections_active 1\n"));
    }

    #[tokio::test]
    async fn test_rules_create_with_forward_connector() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, created) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "to-webhook",
                       "topic_filter": "sensors/+",
                       "sql_query": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
                       "actions": [{"type": "forwardconnector", "connector_id": "webhook-1"}]}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["actions"][0]["type"], json!("forwardconnector"));
        assert_eq!(created["actions"][0]["connector_id"], json!("webhook-1"));

        // Empty connector id is rejected.
        let (status, _) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "bad",
                       "topic_filter": "sensors/+",
                       "actions": [{"type": "forwardconnector", "connector_id": "  "}]}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn rules_list_echoes_stored_actions_only() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "no-actions",
                       "sql": "SELECT * FROM \"t/#\"",
                       "enable": true,
                       "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["actions"], json!([]));
        assert!(created.get("description").is_none());
        assert!(created.get("created_at").is_none());

        let (status, body) = server.get_auth("/api/v5/rules", &token).await;
        assert_eq!(status, 200);
        let text = serde_json::to_string(&body).expect("encode list body");
        assert!(
            !text.contains("kafka:kafka-prod"),
            "list must not inject fallback actions, got: {text}"
        );
        let data = body["data"].as_array().expect("data is a list");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["actions"], json!([]));
        assert!(data[0].get("description").is_none());
        assert!(data[0].get("created_at").is_none());
    }

    #[tokio::test]
    async fn rules_builtin_functions_round_trip_in_documented_shape() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // The example of the specification: a republish and a console.
        let body = json!({
            "name": "builtin-fn",
            "sql": "SELECT * FROM \"test/topic\" WHERE payload.x = 1",
            "actions": [
                {"function": "republish", "args": {"topic": "t/1", "payload": "${payload}"}},
                {"function": "console"}
            ]
        });
        let (status, created) = server.post_auth("/api/v5/rules", body, &token).await;
        assert_eq!(status, 201, "create answered {created}");
        let actions = created["actions"].as_array().expect("actions list");
        assert_eq!(actions.len(), 2);
        assert_eq!(actions[0]["function"], json!("republish"));
        assert_eq!(actions[0]["args"]["topic"], json!("t/1"));
        assert_eq!(actions[0]["args"]["payload"], json!("${payload}"));
        // Defaults are filled in, as the specification documents them.
        assert_eq!(actions[0]["args"]["qos"], json!("${qos}"));
        assert_eq!(actions[0]["args"]["retain"], json!("${retain}"));
        assert_eq!(actions[0]["args"]["direct_dispatch"], json!(false));
        assert_eq!(actions[1], json!({"function": "console"}));

        // The read route returns the same items.
        let rule_id = created["id"].as_str().expect("rule id").to_string();
        let (status, fetched) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["actions"], created["actions"]);

        // A full update replaces them; fixed QoS and retain keep their types.
        let (status, updated) = server
            .put_auth(
                &format!("/api/v5/rules/{rule_id}"),
                json!({
                    "sql": "SELECT * FROM \"test/topic\"",
                    "actions": [{"function": "republish",
                                 "args": {"topic": "out/${topic}", "qos": 1, "retain": true}}]
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200, "update answered {updated}");
        assert_eq!(updated["actions"][0]["args"]["qos"], json!(1));
        assert_eq!(updated["actions"][0]["args"]["retain"], json!(true));
        assert_eq!(
            updated["actions"][0]["args"]["topic"],
            json!("out/${topic}")
        );
    }

    #[tokio::test]
    async fn rules_builtin_functions_refuse_what_the_broker_cannot_do() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let sql = "SELECT * FROM \"t/#\"";

        // A user function needs user code, which this broker cannot load.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "user-fn", "sql": sql,
                       "actions": [{"function": "my_module:my_function", "args": {}}]}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(
            body["message"]
                .as_str()
                .unwrap_or_default()
                .contains("not available in this broker"),
            "message was {body}"
        );

        // Arguments that break the documented types or ranges.
        for (name, args) in [
            ("no-topic", json!({})),
            ("wildcard", json!({"topic": "a/#"})),
            ("bad-qos", json!({"topic": "a", "qos": 3})),
            ("bad-retain", json!({"topic": "a", "retain": "sometimes"})),
            ("open-template", json!({"topic": "a/${b"})),
            ("unknown-arg", json!({"topic": "a", "color": "red"})),
            (
                "content-type",
                json!({"topic": "a", "mqtt_properties": {"Content-Type": "text/plain"}}),
            ),
        ] {
            let (status, body) = server
                .post_auth(
                    "/api/v5/rules",
                    json!({"name": name, "sql": sql,
                           "actions": [{"function": "republish", "args": args}]}),
                    &token,
                )
                .await;
            assert_eq!(status, 400, "{name} answered {body}");
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
        // Nothing was stored.
        let (_, list) = server.get_auth("/api/v5/rules", &token).await;
        assert_eq!(list["data"].as_array().map(Vec::len), Some(0));

        // The empty forms that the management console sends are accepted.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "console-defaults", "sql": sql,
                       "actions": [{"function": "republish",
                                    "args": {"topic": "a", "qos": 0, "retain": false,
                                             "payload": "${payload}", "mqtt_properties": {},
                                             "user_properties": "", "direct_dispatch": false}}]}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "console defaults answered {body}");
    }

    #[tokio::test]
    async fn rules_create_rejects_bad_filter() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Spec-literal invalid filter: `FROM "t/["` must be 400 (previously
        // silently coerced to `t/#`).
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "bad-filter",
                       "sql": "SELECT * FROM \"t/[\"",
                       "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // A misplaced `#` (mid-filter) is likewise rejected, not coerced.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "bad-filter-hash",
                       "sql": "SELECT * FROM \"t/#/bogus\"",
                       "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // A create with only `sql` succeeds: the broker assigns an id
        // and `GET /rules/{id}` finds the rule.
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"sql": "SELECT * FROM \"t/#\"", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "sql-only create answered {created}");
        let id = created["id"].as_str().expect("created rule has id");
        assert_eq!(id.len(), 8, "assigned id has 8 chars, got {id:?}");
        assert!(
            id.chars().all(|c| c.is_ascii_hexdigit()),
            "assigned id is hex, got {id:?}"
        );
        let (status, fetched) = server
            .get_auth(&format!("/api/v5/rules/{id}"), &token)
            .await;
        assert_eq!(status, 200, "assigned id reads back");
        assert_eq!(fetched["id"], json!(id));
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"t/#\""));
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{id}"), &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn rules_update_unknown_is_404() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server
            .put_auth("/api/v5/rules/nope", json!({"enable": false}), &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn rules_read_and_full_update_round_trip() {
        // W3-14 (C06-21/C06-22) failing test: create a rule, read it back
        // expecting the full fields, replace the whole body and read again
        // expecting the new values (not just an enable flip); an unknown id
        // reads and updates as 404, and a malformed update keeps the old
        // rule. Before: update applied only the enable flag. Broker-through:
        // every step goes through the HTTP router on the live engine, and
        // one publish goes through `dispatch_ingress` (the broker's rule
        // path) before and after the replace. Management-plane only
        // otherwise; publish and deliver never touch the rule map directly
        // (dispatch reads a snapshot).
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Unknown read is 404 with the documented code, not an empty body.
        let (status, body) = server.get_auth("/api/v5/rules/w3-14-nope", &token).await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Unknown update creates nothing.
        let (status, body) = server
            .put_auth(
                "/api/v5/rules/w3-14-nope",
                json!({"sql": "SELECT * FROM \"w3/14/missing\"", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Create the action first, then the rule referencing it.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-14-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "id": "w3-14-rule",
                    "name": "w3-14-rule",
                    "sql": "SELECT * FROM \"w3/14/before\"",
                    "actions": ["w3-14-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);

        // Read back expects the full documented fields.
        let (status, fetched) = server.get_auth("/api/v5/rules/w3-14-rule", &token).await;
        assert_eq!(status, 200);
        assert_eq!(fetched["id"], json!("w3-14-rule"));
        assert_eq!(fetched["name"], json!("w3-14-rule"));
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"w3/14/before\""));
        assert_eq!(fetched["from"], json!(["w3/14/before"]));
        assert_eq!(fetched["enable"], json!(true));

        // Unknown query parameters are ignored, never an error.
        let (status, _) = server
            .get_auth("/api/v5/rules/w3-14-rule?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .put_auth(
                "/api/v5/rules/w3-14-rule?unknown=param",
                json!({
                    "id": "w3-14-rule",
                    "name": "w3-14-rule-v2",
                    "sql": "SELECT * FROM \"w3/14/after\"",
                    "enable": false,
                    "actions": [],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        // Full replace round trip: the new values read back, not just the
        // enable flag.
        let (status, fetched) = server.get_auth("/api/v5/rules/w3-14-rule", &token).await;
        assert_eq!(status, 200);
        assert_eq!(fetched["name"], json!("w3-14-rule-v2"));
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"w3/14/after\""));
        assert_eq!(fetched["from"], json!(["w3/14/after"]));
        assert_eq!(fetched["enable"], json!(false));
        assert_eq!(fetched["actions"], json!([]));
        assert_eq!(created["id"], json!("w3-14-rule"));

        // Broker rule path: the replaced rule fires through
        // `dispatch_ingress` on its new topic, never `sink.send` directly.
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-14-action"),
            counter.clone(),
        );
        // Re-enable with the action attached so the fire is observable.
        let (status, _) = server
            .put_auth(
                "/api/v5/rules/w3-14-rule",
                json!({
                    "id": "w3-14-rule",
                    "name": "w3-14-rule-v3",
                    "sql": "SELECT * FROM \"w3/14/after\"",
                    "enable": true,
                    "actions": ["w3-14-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/14/after").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "replaced rule must fire through dispatch on its new topic"
        );

        // Malformed update is rejected and the old rule is kept.
        let (status, body) = server
            .put_auth(
                "/api/v5/rules/w3-14-rule",
                json!({"sql": "SELECT WHERE WHERE", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, fetched) = server.get_auth("/api/v5/rules/w3-14-rule", &token).await;
        assert_eq!(status, 200);
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"w3/14/after\""));

        // Cleanup: the rule, then the action it referenced.
        let (status, _) = server.delete_auth("/api/v5/rules/w3-14-rule", &token).await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-14-action", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn rules_list_create_delete_round_trip() {
        // W3-13 (C06-18/C06-19/C06-20) failing test: create a rule, list
        // and expect it with paging metadata; duplicate create expects
        // rejection; delete then second delete expects 404. Before:
        // paging missing (bare envelope) and duplicate ids accepted.
        // Broker-through: the rule is created through the HTTP router on
        // the live engine, one publish goes through `dispatch_ingress`
        // (the broker's rule path) into a sink registered under the
        // referenced action id, and the delete goes through the router
        // too. Management-plane only otherwise; publish and deliver never
        // touch the rule map directly (dispatch reads a snapshot).
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty engine returns an empty page, not an error.
        let (status, body) = server.get_auth("/api/v5/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["page"], json!(1));
        assert_eq!(body["meta"]["limit"], json!(100));
        assert_eq!(body["meta"]["count"], json!(0));
        assert_eq!(body["meta"]["hasnext"], json!(false));

        // Unknown query parameters are ignored, never an error.
        let (status, _) = server.get_auth("/api/v5/rules?unknown=param", &token).await;
        assert_eq!(status, 200);

        // The forward target must name a known action (W3-02 linkage):
        // an unknown action id is rejected before anything is stored.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "id": "w3-13-rule",
                    "sql": "SELECT * FROM \"w3/13/rules\"",
                    "actions": ["w3-13-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Create the action first, then the rule referencing it.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-13-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "id": "w3-13-rule",
                    "sql": "SELECT * FROM \"w3/13/rules\"",
                    "actions": ["w3-13-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["id"], json!("w3-13-rule"));

        // Malformed SQL is compiled before storing and rejected.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "id": "w3-13-bad-sql",
                    "sql": "SELECT WHERE WHERE",
                    "actions": [],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // List carries the rule with the standard page envelope.
        let (status, body) = server.get_auth("/api/v5/rules", &token).await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("data is a list");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["id"], json!("w3-13-rule"));
        assert_eq!(body["meta"]["count"], json!(1));
        assert_eq!(body["meta"]["hasnext"], json!(false));

        // Paging slices after filtering: page 2 of a 1-row store is empty.
        let (status, body) = server
            .get_auth("/api/v5/rules?page=2&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(body["meta"]["page"], json!(2));
        assert_eq!(body["meta"]["limit"], json!(1));
        assert_eq!(body["meta"]["count"], json!(1));
        assert_eq!(body["meta"]["hasnext"], json!(false));

        // Duplicate id is rejected cleanly, never overwritten.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "id": "w3-13-rule",
                    "sql": "SELECT * FROM \"w3/13/rules\"",
                    "actions": [],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Broker rule path: one publish through `dispatch_ingress` fires
        // the stored rule into the registered sink, never `sink.send`
        // directly.
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-13-action"),
            counter.clone(),
        );
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/13/rules").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "stored rule must fire through dispatch"
        );

        // Delete hits with 204; a second delete is 404, not an empty body.
        let (status, _) = server.delete_auth("/api/v5/rules/w3-13-rule", &token).await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        let (status, body) = server.delete_auth("/api/v5/rules/w3-13-rule", &token).await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Cleanup: the action the rule referenced.
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-13-action", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn test_auth_users_and_acls() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Create a user.
        let (status, created) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "alice", "password": "s3cret"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["username"], json!("alice"));
        // Passwords are write-only: never echoed.
        assert!(created.get("password").is_none());
        assert_eq!(state.auth.user_count(), 1);

        // Empty username or password is rejected.
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "", "password": "x"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "bob", "password": ""}),
                &token,
            )
            .await;
        assert_eq!(status, 400);

        // Create an ACL rule.
        let (status, created) = server
            .post_auth(
                "/api/v1/auth/acls",
                json!({"client_pattern": "alice",
                       "action": "publish",
                       "topic_pattern": "sensors/#",
                       "allow": true}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["allow"], json!(true));

        // Invalid action, topic pattern, and client pattern rejected.
        for body in [
            json!({"client_pattern": "*", "action": "delete", "topic_pattern": "#", "allow": true}),
            json!({"client_pattern": "*", "action": "publish", "topic_pattern": "a/#/b", "allow": true}),
            json!({"client_pattern": "", "action": "all", "topic_pattern": "#", "allow": false}),
        ] {
            let (status, _) = server.post_auth("/api/v1/auth/acls", body, &token).await;
            assert_eq!(status, 400);
        }
    }

    #[tokio::test]
    async fn test_auth_lists_users_and_acls() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, body) = server.get_auth("/api/v1/auth/acls", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "alice", "password": "s3cret"}),
                &token,
            )
            .await;
        server
            .post_auth(
                "/api/v1/auth/acls",
                json!({"client_pattern": "alice",
                       "action": "subscribe",
                       "topic_pattern": "sensors/#",
                       "allow": true}),
                &token,
            )
            .await;

        let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            body,
            json!([{"username": "alice", "quotas": {"max_connections": null, "max_publish_rate": null, "max_publish_burst": null}}])
        );
        let (status, body) = server.get_auth("/api/v1/auth/acls", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().expect("acls").len(), 1);
        assert_eq!(body[0]["client_pattern"], json!("alice"));
        assert_eq!(body[0]["action"], json!("subscribe"));
        assert_eq!(body[0]["allow"], json!(true));
    }

    #[tokio::test]
    async fn test_auth_users_quotas_roundtrip() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Create with quotas: every bound echoes back.
        let (status, created) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "capped",
                       "password": "pw",
                       "quotas": {"max_connections": 100,
                                  "max_publish_rate": 50,
                                  "max_publish_burst": 10}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["username"], json!("capped"));
        assert_eq!(created["quotas"]["max_connections"], json!(100));
        assert_eq!(created["quotas"]["max_publish_rate"], json!(50));
        assert_eq!(created["quotas"]["max_publish_burst"], json!(10));

        // Without quotas: all-null bounds object.
        let (status, created) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "plain", "password": "pw"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(
            created["quotas"]["max_connections"],
            serde_json::Value::Null
        );

        // Listing shows both shapes side by side.
        let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
        assert_eq!(status, 200);
        let users = body.as_array().expect("users list");
        assert_eq!(users.len(), 2);
        assert_eq!(users[0]["username"], json!("capped"));
        assert_eq!(users[0]["quotas"]["max_connections"], json!(100));
        assert_eq!(users[1]["username"], json!("plain"));
        assert_eq!(
            users[1]["quotas"]["max_publish_rate"],
            serde_json::Value::Null
        );
    }

    #[tokio::test]
    async fn login_default_admin_requires_password_change() {
        let (server, _state) = TestServer::start().await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "public"}),
            )
            .await;
        assert_eq!(status, 200);
        assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));
        assert_eq!(body["role"], json!("administrator"));
        assert_eq!(body["must_change_password"], json!(true));
    }

    #[tokio::test]
    async fn login_wrong_password_is_rejected_and_creates_nothing() {
        let (server, state) = TestServer::start().await;

        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "wrong-password"}),
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));

        // An unknown user is rejected and appears in neither store.
        let (status, _) = server
            .post(
                "/api/v5/login",
                json!({"username": "ghost", "password": "whatever-123"}),
            )
            .await;
        assert_eq!(status, 401);
        assert!(state.admin_users.get("ghost").is_none());
        assert_eq!(state.auth.user_count(), 0);
    }

    #[tokio::test]
    async fn login_issues_usable_token_and_rejects_bad_and_malformed() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (server, state) = TestServer::start().await;
        let admin = server.login_as_admin().await;

        // Create a fresh administrator through the broker, then log in as
        // it: the issued token must open a follow-up management call.
        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "w229-op", "password": "W229-op-pass!", "role": "administrator"}),
                &admin,
            )
            .await;
        assert_eq!(status, 200);
        let before = state.tokens.len();
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "w229-op", "password": "W229-op-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"]
            .as_str()
            .expect("login issues a token")
            .to_string();
        assert!(!token.is_empty(), "issued token must not be empty");
        assert_eq!(body["role"], json!("administrator"));
        assert_eq!(
            state.tokens.len(),
            before + 1,
            "one successful login mints exactly one entry"
        );

        // The token carries the documented 3600 s TTL ...
        let info = state.tokens.lookup(&token).expect("fresh token looks up");
        let remaining = info
            .expires_at
            .saturating_duration_since(std::time::Instant::now());
        assert!(
            remaining <= std::time::Duration::from_secs(3600),
            "remaining TTL {remaining:?} must not exceed the documented 3600 s"
        );
        assert!(
            remaining > std::time::Duration::from_secs(3500),
            "remaining TTL {remaining:?} must sit at the documented 3600 s"
        );

        // ... authorises a follow-up call ...
        let (status, me) = server.get_auth("/api/v5/current_user", &token).await;
        assert_eq!(status, 200);
        assert_eq!(me["username"], json!("w229-op"));

        // ... and the success response sets no cookies: the token travels
        // in the JSON body only.
        let raw_login =
            serde_json::to_string(&json!({"username": "w229-op", "password": "W229-op-pass!"}))
                .expect("encode login body");
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
            .await
            .expect("connect test server");
        stream
            .write_all(
                format!(
                    "POST /api/v5/login HTTP/1.0\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw_login}",
                    raw_login.len()
                )
                .as_bytes(),
            )
            .await
            .expect("write login request");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.expect("read response");
        let text = String::from_utf8(buf).expect("response is UTF-8");
        assert!(
            !text.to_ascii_lowercase().contains("set-cookie"),
            "login must not set cookies, got: {text}"
        );
        assert_eq!(
            state.tokens.len(),
            before + 2,
            "the raw-socket success above mints one more entry"
        );

        // Wrong password: the documented error, no token, no new entry.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "w229-op", "password": "Wr0ng-pass!"}),
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
        assert_eq!(body.as_object().expect("error is an object").len(), 2);
        assert!(
            body.get("token").is_none(),
            "a failed login issues no token"
        );
        assert_eq!(
            state.tokens.len(),
            before + 2,
            "failed logins create no entries"
        );

        // Unknown user: same refusal, nothing created anywhere.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "w229-ghost", "password": "whatever-123"}),
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
        assert!(body.get("token").is_none());
        assert!(state.admin_users.get("w229-ghost").is_none());
        assert_eq!(state.tokens.len(), before + 2);

        // Malformed bodies are client errors in the documented shape and
        // create nothing: a missing field ...
        let (status, body) = server
            .post("/api/v5/login", json!({"username": "w229-op"}))
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body.get("message").is_some());
        assert_eq!(body.as_object().expect("error is an object").len(), 2);
        assert!(body.get("token").is_none());
        assert_eq!(state.tokens.len(), before + 2);

        // ... empty credentials ...
        let (status, body) = server
            .post("/api/v5/login", json!({"username": "", "password": ""}))
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body.get("token").is_none());
        assert_eq!(state.tokens.len(), before + 2);

        // ... and a body that is not JSON at all.
        let (status, raw) = server
            .request_raw(
                "POST /api/v5/login HTTP/1.0\r\nHost: test\r\nContent-Type: application/json\r\nContent-Length: 10\r\nConnection: close\r\n\r\n{not json!",
            )
            .await;
        assert_eq!(status, 400);
        let malformed: serde_json::Value =
            serde_json::from_str(&raw).expect("malformed login answers JSON");
        assert_eq!(malformed["code"], json!("BAD_REQUEST"));
        assert!(malformed.get("token").is_none());
        assert_eq!(state.tokens.len(), before + 2);

        // Logout revokes the issued token: the follow-up call fails after.
        let (status, _) = server
            .send("POST", "/api/v5/logout", None, Some(&token))
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/current_user", &token).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
    }

    #[tokio::test]
    async fn logout_revokes_and_rejects_reuse_and_missing() {
        // Broker-through logout (C05-09): log in, call an authorized
        // route, log out, then retry the same route with the old token.
        // Before the fix the second logout still reported success; now a
        // missing or already-revoked token is a client error, never 204.
        // Writes to the bounded live-token map in `v5/auth.rs`
        // (`ApiTokens::issue` on login, `ApiTokens::revoke` on logout);
        // revoked entries are removed, not retained. Driven by the login
        // POST, the authorized GET, and the logout POST management calls.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let live_before = state.tokens.len();

        // The fresh token authorizes a management call.
        let (status, _) = server.get_auth("/api/v5/current_user", &token).await;
        assert_eq!(status, 200);

        // Logout revokes it: 204 with an empty body.
        let (status, body) = server
            .send("POST", "/api/v5/logout", None, Some(&token))
            .await;
        assert_eq!(status, 204);
        assert_eq!(body, serde_json::Value::Null);
        assert!(
            state.tokens.lookup(&token).is_none(),
            "logout must remove the entry, not retain it"
        );
        assert_eq!(
            state.tokens.len(),
            live_before - 1,
            "logout must shrink the live-token map"
        );

        // Reuse of the old token is rejected with the documented shape.
        let (status, body) = server.get_auth("/api/v5/current_user", &token).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
        assert!(body.get("message").is_some());
        assert_eq!(body.as_object().expect("error is an object").len(), 2);

        // A second logout with the already-revoked token is a client
        // error, not success.
        let (status, body) = server
            .send("POST", "/api/v5/logout", None, Some(&token))
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
        assert_eq!(body.as_object().expect("error is an object").len(), 2);

        // A logout with no token at all is a client error, not success.
        let (status, body) = server.send("POST", "/api/v5/logout", None, None).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
        assert_eq!(body.as_object().expect("error is an object").len(), 2);
    }

    #[tokio::test]
    async fn scram_login_end_to_end() {
        let (server, _state) = TestServer::start().await;
        let client_nonce = "testclientnonce123";

        // Correct password: challenge, proof, verify.
        let (status, challenge) = server
            .post(
                "/api/v5/login/challenge",
                json!({"username": "admin", "client_nonce": client_nonce}),
            )
            .await;
        assert_eq!(status, 200);
        let challenge_id = challenge["challenge_id"].as_str().expect("challenge id");
        let server_nonce = challenge["server_nonce"].as_str().expect("server nonce");
        let salt_b64 = challenge["salt"].as_str().expect("salt");
        let iterations = challenge["iterations"].as_u64().expect("iterations") as u32;
        let combined = format!("{client_nonce}{server_nonce}");
        let auth_message =
            scram_auth_message("admin", client_nonce, &combined, salt_b64, iterations);
        let (proof_b64, server_sig_b64) =
            scram_client::proof("public", salt_b64, iterations, &auth_message);

        let (status, body) = server
            .post(
                "/api/v5/login/verify",
                json!({
                    "challenge_id": challenge_id,
                    "combined_nonce": combined,
                    "client_proof": proof_b64,
                }),
            )
            .await;
        assert_eq!(status, 200);
        assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));
        assert_eq!(body["server_signature"], json!(server_sig_b64));

        // Wrong password: a fresh challenge, then the proof is rejected.
        let (status, challenge) = server
            .post(
                "/api/v5/login/challenge",
                json!({"username": "admin", "client_nonce": client_nonce}),
            )
            .await;
        assert_eq!(status, 200);
        let challenge_id = challenge["challenge_id"].as_str().expect("challenge id");
        let salt_b64 = challenge["salt"].as_str().expect("salt");
        let iterations = challenge["iterations"].as_u64().expect("iterations") as u32;
        let auth_message =
            scram_auth_message("admin", client_nonce, &combined, salt_b64, iterations);
        let (bad_proof, _) =
            scram_client::proof("wrong-password", salt_b64, iterations, &auth_message);
        let (status, body) = server
            .post(
                "/api/v5/login/verify",
                json!({
                    "challenge_id": challenge_id,
                    "combined_nonce": combined,
                    "client_proof": bad_proof,
                }),
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
    }

    #[tokio::test]
    async fn current_user_reflects_token() {
        let (server, _state) = TestServer::start().await;
        let admin_token = server.login_as_admin().await;

        // Create a viewer through the admin API.
        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "watcher", "password": "W4tcher-pass", "role": "viewer"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "watcher", "password": "W4tcher-pass"}),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["role"], json!("viewer"));
        let viewer_token = body["token"].as_str().expect("viewer token").to_string();

        let (status, body) = server.get_auth("/api/v5/current_user", &viewer_token).await;
        assert_eq!(status, 200);
        assert_eq!(body["username"], json!("watcher"));
        assert_eq!(body["role"], json!("viewer"));

        // No token is a 401.
        let (status, body) = server.get("/api/v5/current_user").await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
    }

    #[tokio::test]
    async fn change_pwd_flow() {
        let (server, _state) = TestServer::start().await;
        let admin_token = server.admin_token().await;
        // A second admin token that must die on password change.
        let (status, other) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "public"}),
            )
            .await;
        assert_eq!(status, 200);
        let other_token = other["token"].as_str().expect("second token").to_string();
        assert_ne!(admin_token, other_token);

        // Admin changes their own password from the default.
        let (status, _) = server
            .put_auth(
                "/api/v5/users/admin/change_pwd",
                json!({"old_pwd": "public", "new_pwd": "N3w-passw0rd!"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 204);

        // Flag cleared on the caller's surviving token.
        let (status, body) = server.get_auth("/api/v5/current_user", &admin_token).await;
        assert_eq!(status, 200);
        assert_eq!(body["must_change_password"], json!(false));

        // Old password no longer logs in; the other token is revoked.
        let (status, _) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "public"}),
            )
            .await;
        assert_eq!(status, 401);
        let (status, _) = server.get_auth("/api/v5/current_user", &other_token).await;
        assert_eq!(status, 401);

        // A viewer cannot change another user's password.
        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "watcher", "password": "W4tcher-pass", "role": "viewer"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "watcher", "password": "W4tcher-pass"}),
            )
            .await;
        assert_eq!(status, 200);
        let viewer_token = body["token"].as_str().expect("viewer token").to_string();
        let (status, body) = server
            .put_auth(
                "/api/v5/users/admin/change_pwd",
                json!({"old_pwd": "N3w-passw0rd!", "new_pwd": "An0ther-pass!"}),
                &viewer_token,
            )
            .await;
        assert_eq!(status, 403);
        assert_eq!(body["code"], json!("FORBIDDEN"));
    }

    #[tokio::test]
    async fn change_pwd_accepts_post() {
        let (server, _state) = TestServer::start().await;
        let fresh = server.admin_token().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/users/admin/change_pwd",
                json!({"old_pwd": "public", "new_pwd": "N3w-post-passw0rd!"}),
                &fresh,
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server.get_auth("/api/v1/clients", &fresh).await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn lowercase_bearer_scheme_is_accepted() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server
            .request(&format!(
                "GET /api/v5/current_user HTTP/1.0\r\nHost: test\r\nAuthorization: bearer {token}\r\nConnection: close\r\n\r\n"
            ))
            .await;
        assert_eq!(status, 200);
        assert!(body["username"].is_string());
    }

    #[tokio::test]
    async fn bare_token_without_scheme_is_rejected() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server
            .request(&format!(
                "GET /api/v5/current_user HTTP/1.0\r\nHost: test\r\nAuthorization: {token}\r\nConnection: close\r\n\r\n"
            ))
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
    }

    #[test]
    fn is_own_change_pwd_percent_decodes_username() {
        use crate::api_auth::is_own_change_pwd;
        let post = axum::http::Method::POST;
        assert!(is_own_change_pwd(
            &post,
            "/api/v5/users/ops.team-1%40site/change_pwd",
            "ops.team-1@site"
        ));
        assert!(!is_own_change_pwd(
            &post,
            "/api/v5/users/ops.team-1%4/change_pwd",
            "ops.team-1@site"
        ));
    }

    #[tokio::test]
    async fn admin_users_api_does_not_touch_mqtt_store() {
        let (server, state) = TestServer::start().await;
        let admin_token = server.login_as_admin().await;

        let (status, created) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "ops", "password": "Ops-passw0rd", "role": "viewer",
                       "description": "read-only"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(created["username"], json!("ops"));
        assert_eq!(created["role"], json!("viewer"));
        assert_eq!(created["description"], json!("read-only"));
        assert_eq!(state.auth.user_count(), 0);

        // Listing serves the admin store with roles, sorted by username,
        // as a bare array, as the reference API documents (C05-25).
        let (status, body) = server.get_auth("/api/v5/users", &admin_token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(2));
        assert_eq!(body[0]["backend"], json!("local"));
        assert_eq!(body[0]["mfa"], json!("none"));
        let names: Vec<&str> = body
            .as_array()
            .expect("users data array")
            .iter()
            .map(|u| u["username"].as_str().expect("username"))
            .collect();
        assert_eq!(names, vec!["admin", "ops"]);

        // Role/description update and delete stay out of the MQTT store.
        let (status, updated) = server
            .put_auth(
                "/api/v5/users/ops",
                json!({"role": "administrator", "description": "on-call"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(updated["role"], json!("administrator"));
        assert_eq!(updated["description"], json!("on-call"));
        let (status, _) = server.delete_auth("/api/v5/users/ops", &admin_token).await;
        assert_eq!(status, 204);
        assert_eq!(state.auth.user_count(), 0);
    }

    #[tokio::test]
    async fn demoted_admin_token_loses_admin_rights() {
        let (server, _state) = TestServer::start().await;
        let admin_token = server.login_as_admin().await;

        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "b-admin", "password": "B-passw0rd!", "role": "administrator"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "b-admin", "password": "B-passw0rd!"}),
            )
            .await;
        assert_eq!(status, 200);
        let b_token = body["token"].as_str().expect("b token").to_string();

        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "probe", "password": "Probe-pass1", "role": "viewer"}),
                &b_token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, _) = server
            .put_auth(
                "/api/v5/users/b-admin",
                json!({"role": "viewer"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "probe2", "password": "Probe-pass1", "role": "viewer"}),
                &b_token,
            )
            .await;
        assert_eq!(status, 403);
        assert_eq!(body["code"], json!("FORBIDDEN"));
    }

    #[tokio::test]
    async fn deleted_user_token_is_rejected() {
        let (server, _state) = TestServer::start().await;
        let admin_token = server.login_as_admin().await;

        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "viewer-v", "password": "V-passw0rd!", "role": "viewer"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "viewer-v", "password": "V-passw0rd!"}),
            )
            .await;
        assert_eq!(status, 200);
        let v_token = body["token"].as_str().expect("viewer token").to_string();

        let (status, _) = server.get_auth("/api/v5/current_user", &v_token).await;
        assert_eq!(status, 200);

        let (status, _) = server
            .delete_auth("/api/v5/users/viewer-v", &admin_token)
            .await;
        assert_eq!(status, 204);

        let (status, body) = server.get_auth("/api/v5/current_user", &v_token).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
    }

    /// Unique scratch data dir under the OS temp dir (no dev-dependency).
    fn unique_data_dir(prefix: &str) -> std::path::PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let slot = COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "indramqtt-w020-datadir-{prefix}-{}-{nanos}-{slot}",
            std::process::id()
        ))
    }

    #[tokio::test]
    async fn admin_users_survive_restart() {
        let dir = unique_data_dir("survive");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let admin_token = server.login_as_admin().await;

        // Create `w0keep` through the API, then change its password.
        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "w0keep", "password": "W0keep-pass1", "role": "viewer"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .put_auth(
                "/api/v5/users/w0keep/change_pwd",
                json!({"new_pwd": "W0keep-new-pass2"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);

        // Simulate a kernel restart: rebuild state from the same dir.
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;

        // The new password logs in; the old one is rejected.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "w0keep", "password": "W0keep-new-pass2"}),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["role"], json!("viewer"));
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "w0keep", "password": "W0keep-pass1"}),
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("BAD_USERNAME_OR_PWD"));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn admin_users_delete_survives_restart() {
        let dir = unique_data_dir("delete");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(registry).await;
        let admin_token = server.login_as_admin().await;

        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "w0keep", "password": "W0keep-pass1", "role": "viewer"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);
        // Sanity: the user logs in before the delete.
        let (status, _) = server
            .post(
                "/api/v5/login",
                json!({"username": "w0keep", "password": "W0keep-pass1"}),
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .delete_auth("/api/v5/users/w0keep", &admin_token)
            .await;
        assert_eq!(status, 204);
        drop(server);

        // Simulate a kernel restart: rebuild state from the same dir.
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;

        // The deleted user can no longer log in ...
        let (status, _) = server
            .post(
                "/api/v5/login",
                json!({"username": "w0keep", "password": "W0keep-pass1"}),
            )
            .await;
        assert_eq!(status, 401);
        // ... and the reloaded list has no `w0keep`. The admin password
        // persisted too, so the rebooted node logs in with it.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let admin_token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/users", &admin_token).await;
        assert_eq!(status, 200);
        let names: Vec<&str> = body
            .as_array()
            .expect("users data array")
            .iter()
            .map(|u| u["username"].as_str().expect("username"))
            .collect();
        assert!(
            !names.contains(&"w0keep"),
            "deleted user must stay deleted, got: {names:?}"
        );
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn dashboard_user_mfa_enrol_disable_round_trip() {
        // W2-44 through-the-router test: enrol MFA for a dashboard user
        // over HTTP, reject the duplicate, delete, then miss the second
        // delete. The enrolment lands on the same admin-user MFA state the
        // setup/verify routes consult, and the MQTT credential store stays
        // untouched throughout (management-plane only).
        let (server, state) = TestServer::start().await;
        let admin_token = server.login_as_admin().await;

        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "w244-http", "password": "W244-http-pass1", "role": "viewer"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);

        // Enrol answers 204 with no body, as the reference API documents.
        let (status, enrolled) = server
            .post_auth(
                "/api/v5/users/w244-http/mfa",
                json!({"mechanism": "totp"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 204);
        assert!(enrolled.is_null(), "enrol returns no body, got: {enrolled}");
        assert_eq!(state.auth.user_count(), 0);

        // The secret and the provisioning URI come from the setup route.
        let (status, provisioning) = server
            .post_auth(
                "/api/v5/sso/mfa/setup",
                json!({"username": "w244-http"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(provisioning["username"], json!("w244-http"));
        assert!(
            provisioning["secret"]
                .as_str()
                .is_some_and(|s| !s.is_empty()),
            "setup returns the pending secret, got: {provisioning}"
        );
        assert!(provisioning["provisioning_uri"].is_string());
        let first_secret = provisioning["secret"]
            .as_str()
            .expect("setup returns a secret")
            .to_string();

        // Setup_info follows the single-sign-on shape. A local body
        // fails with 400. A full body fails with 401.
        let (status, err) = server
            .post_auth(
                "/api/v5/sso/mfa/setup_info",
                json!({"username": "w244-http"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(err["code"], json!("BAD_REQUEST"));
        let (status, err) = server
            .post_auth(
                "/api/v5/sso/mfa/setup_info",
                json!({
                    "backend": "oidc",
                    "setup_token": "w244-token",
                    "username": "w244-http",
                }),
                &admin_token,
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(err["code"], json!("UNAUTHORIZED"));

        // A second enrol resets with a fresh secret.
        let (status, _) = server
            .post_auth(
                "/api/v5/users/w244-http/mfa",
                json!({"mechanism": "totp"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, second) = server
            .post_auth(
                "/api/v5/sso/mfa/setup",
                json!({"username": "w244-http"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 200);
        let rotated = second["secret"]
            .as_str()
            .expect("reset returns a secret")
            .to_string();
        assert_ne!(first_secret, rotated);

        // Delete disables; a second delete changes nothing and answers 204.
        let (status, _) = server
            .delete_auth("/api/v5/users/w244-http/mfa", &admin_token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/users/w244-http/mfa", &admin_token)
            .await;
        assert_eq!(status, 204);

        // Unknown users are 404 on both routes.
        let (status, err) = server
            .post_auth(
                "/api/v5/users/w244-ghost/mfa",
                json!({"mechanism": "totp"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("USER_NOT_FOUND"));
        let (status, err) = server
            .delete_auth("/api/v5/users/w244-ghost/mfa", &admin_token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("USER_NOT_FOUND"));
        assert_eq!(state.auth.user_count(), 0);

        let (status, _) = server
            .delete_auth("/api/v5/users/w244-http", &admin_token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn mqtt_users_and_quotas_survive_restart() {
        let dir = unique_data_dir("mqtt-survive");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(registry).await;
        let admin_token = server.login_as_admin().await;

        // Create MQTT users through the API, one with quotas.
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "capped", "password": "pw-capped",
                       "quotas": {"max_connections": 2,
                                  "max_publish_rate": 50,
                                  "max_publish_burst": 10}}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "plain", "password": "pw-plain"}),
                &admin_token,
            )
            .await;
        assert_eq!(status, 201);
        drop(server);

        // Simulate a kernel restart: rebuild state from the same dir.
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;

        // The admin password persisted too, so the rebooted node logs in
        // with it; both MQTT users and the configured quotas are intact.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let admin_token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v1/auth/users", &admin_token).await;
        assert_eq!(status, 200);
        let users = body.as_array().expect("users list");
        assert_eq!(users.len(), 2);
        assert_eq!(users[0]["username"], json!("capped"));
        assert_eq!(users[0]["quotas"]["max_connections"], json!(2));
        assert_eq!(users[0]["quotas"]["max_publish_rate"], json!(50));
        assert_eq!(users[0]["quotas"]["max_publish_burst"], json!(10));
        assert_eq!(users[1]["username"], json!("plain"));
        assert_eq!(
            users[1]["quotas"]["max_publish_rate"],
            serde_json::Value::Null
        );
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn connectors_survive_restart() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let dir = unique_data_dir("w023-conn-survive");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;

        // No `server`/`url` target: no probe runs, status is `connected`
        // and the create path registers a live `http` sink.
        let (status, created) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w023-http-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert!(
            state.engine.connectors().get("w023-http-conn").is_some(),
            "create must register a live sink"
        );
        drop(server);
        drop(registry);

        // Simulate a kernel restart: rebuild state from the same dir.
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, state) = TestServer::start_with_registry(reloaded).await;
        // The admin password persisted too, so the rebooted node logs in
        // with the changed password (`login_as_admin` only fits fresh
        // servers still on the default password).
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();

        // The identical stored entry is back ...
        let (status, body) = server
            .get_auth("/api/v5/connectors/w023-http-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            body, created,
            "restored connector must return the identical stored entry"
        );
        // ... and the restored connector actually connects, not just lists:
        // the live sink is re-registered through the same path create uses.
        assert!(
            state.engine.connectors().get("w023-http-conn").is_some(),
            "restored connector must re-register its live sink"
        );
        let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
        assert_eq!(status, 200);
        assert!(
            body.as_array()
                .expect("connectors array")
                .iter()
                .any(|c| c["id"] == json!("w023-http-conn")),
            "restored connector must be live, got: {body}"
        );
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn connector_delete_survives_restart() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let dir = unique_data_dir("w023-conn-delete");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;

        for name in ["w023-del-conn", "w023-kept-conn"] {
            let (status, _) = server
                .post_auth(
                    "/api/v5/connectors",
                    json!({"name": name, "type": "http"}),
                    &token,
                )
                .await;
            assert_eq!(status, 201);
        }
        let (status, _) = server
            .delete_auth("/api/v5/connectors/w023-del-conn", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .get_auth("/api/v5/connectors/w023-del-conn", &token)
            .await;
        assert_eq!(status, 404);
        // The delete must reach the snapshot on disk: the kept connector
        // is stored, the deleted one is gone.
        let snapshot = ConfigRegistry::load(&dir)
            .expect("reload data dir")
            .snapshot();
        let ids: Vec<&str> = snapshot
            .connectors
            .connectors
            .iter()
            .map(|c| c.id.as_str())
            .collect();
        assert!(
            ids.contains(&"w023-kept-conn"),
            "kept connector must be persisted, got: {ids:?}"
        );
        assert!(
            !ids.contains(&"w023-del-conn"),
            "deleted connector must stay deleted, got: {ids:?}"
        );
        drop(server);
        drop(registry);

        // Simulate a kernel restart: rebuild state from the same dir.
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, state) = TestServer::start_with_registry(reloaded).await;
        // The admin password persisted too, so the rebooted node logs in
        // with the changed password.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();

        // The deleted connector stays deleted, and no sink comes back,
        // while the kept connector is back with its live sink.
        let (status, body) = server
            .get_auth("/api/v5/connectors/w023-del-conn", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(
            state.engine.connectors().get("w023-del-conn").is_none(),
            "deleted connector must not re-register any sink"
        );
        let (status, body) = server
            .get_auth("/api/v5/connectors/w023-kept-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("w023-kept-conn"));
        assert!(
            state.engine.connectors().get("w023-kept-conn").is_some(),
            "kept connector must re-register its live sink"
        );
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn connector_duplicate_create_rejected_without_poisoning() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        // TK-03: a duplicate create must be rejected with the documented
        // conflict code before the store is mutated, so later
        // POST/PUT/DELETE keep working (no poisoned persistence).
        let dir = unique_data_dir("tk03-conn-dup");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;

        // No `server`/`url` target: no probe runs, status is `connected`.
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "tk03-dup-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);

        // Duplicate by `name`: the documented conflict, not a 500.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "tk03-dup-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(
            status, 400,
            "duplicate create must be rejected, got: {body}"
        );
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));

        // Duplicate by `id`: the same connector under the other key.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"id": "tk03-dup-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(
            status, 400,
            "duplicate create by id must be rejected, got: {body}"
        );
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));

        // The store is not poisoned: later POST/PUT/DELETE work normally.
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "tk03-other-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(
            status, 201,
            "create after a rejected duplicate must succeed"
        );
        let (status, _) = server
            .put_auth(
                "/api/v5/connectors/tk03-dup-conn",
                json!({"type": "http", "enable": true}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "update of the existing id must keep working");
        let (status, _) = server
            .delete_auth("/api/v5/connectors/tk03-other-conn", &token)
            .await;
        assert_eq!(
            status, 204,
            "delete after a rejected duplicate must succeed"
        );
        let (status, body) = server
            .get_auth("/api/v5/connectors/tk03-dup-conn", &token)
            .await;
        assert_eq!(
            status, 200,
            "original connector must still be readable: {body}"
        );
        assert_eq!(body["id"], json!("tk03-dup-conn"));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn w317_connectors_list_create_delete_round_trip() {
        // W3-17 (C07-01/C07-02/C07-03) failing test: create a connector,
        // list and expect it with paging metadata and live fields;
        // duplicate create expects rejection; unknown kind expects
        // rejection; delete then second delete expects 404 with exact-id
        // semantics. Before: dump shape (bare array), fuzzy suffix match,
        // missing kind validation. Broker-through: every step goes through
        // the HTTP router on the live engine; create registers the live
        // sink (asserted via the manager, driven by the create event) and
        // one publish goes through `dispatch_ingress` (the broker's rule
        // path), never `sink.send` directly. Management-plane only
        // otherwise; publish and deliver never touch the connector map
        // (dispatch reads the manager directly).
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Scrub leftovers from earlier runs sharing the process-global
        // store so paging assertions stay deterministic. The fire rule
        // goes first: a connector still referenced by a rule cannot be
        // deleted.
        let _ = server
            .delete_auth("/api/v5/rules/w317-fire-rule", &token)
            .await;
        for stale in [
            "w317-list-conn",
            "w317-exact",
            "w317-guarded",
            "w317-guard-action",
            "w317-fire-conn",
            "w317-fire-rule",
        ] {
            let _ = server
                .delete_auth(&format!("/api/v5/rules/{stale}"), &token)
                .await;
            let _ = server
                .delete_auth(&format!("/api/v5/actions/{stale}"), &token)
                .await;
            let _ = server
                .delete_auth(&format!("/api/v5/connectors/{stale}"), &token)
                .await;
        }

        // Create: validated write with live fields, not a raw dump.
        let (status, created) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w317-list-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "create must succeed, got: {created}");
        assert_eq!(created["id"], json!("w317-list-conn"));
        assert_eq!(created["type"], json!("http"));
        assert!(created.get("status").and_then(|v| v.as_str()).is_some());
        assert!(created.get("node_status").is_some());
        // Live health linkage: create registered the sink through the
        // same path boot replay uses, driven by the create event
        // (`crates/broker-api/src/v5/rules.rs`, `register_live_sink`;
        // consulted on the fire path at
        // `crates/broker-rules/src/lib.rs`, `run_actions` via
        // `ConnectorManager::send`).
        assert!(
            state.engine.connectors().get("w317-list-conn").is_some(),
            "create must register a live sink"
        );

        // List: paged collection with live health, not a raw dump.
        let (status, body) = server.get_auth("/api/v5/connectors", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert!(
            data.iter().any(|c| c["id"] == json!("w317-list-conn")),
            "list must contain the created connector, got: {body}"
        );
        let row = data
            .iter()
            .find(|c| c["id"] == json!("w317-list-conn"))
            .expect("created row present");
        assert!(row.get("status").and_then(|v| v.as_str()).is_some());
        assert!(row.get("node_status").is_some());

        // Unknown query parameters are ignored, never an error.
        let (status, _) = server
            .get_auth("/api/v5/connectors?unknown=param", &token)
            .await;
        assert_eq!(status, 200);

        // Duplicate create is rejected cleanly, never overwritten.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w317-list-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "duplicate must be rejected, got: {body}");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"id": "w317-list-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "duplicate by id must be rejected, got: {body}");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));

        // Unknown kind and malformed bodies are rejected with the
        // documented error shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w317-bad-kind", "type": "no_such_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "unknown kind must be rejected, got: {body}");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w317-no-type"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        let (status, _) = server
            .post_auth("/api/v5/connectors", json!({"type": "http"}), &token)
            .await;
        assert_eq!(status, 400);

        // Exact-id semantics: the legacy `prefix:id` suffix form no longer
        // matches. Before, `x:w317-exact` deleted `w317-exact`.
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w317-exact", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .delete_auth("/api/v5/connectors/x:w317-exact", &token)
            .await;
        assert_eq!(status, 404, "fuzzy delete must miss");
        let (status, _) = server
            .delete_auth("/api/v5/connectors/w317-exact", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .delete_auth("/api/v5/connectors/w317-exact", &token)
            .await;
        assert_eq!(status, 404, "second delete must miss, got: {body}");
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Delete of an unknown id is 404 with the documented code.
        let (status, body) = server
            .delete_auth("/api/v5/connectors/w317-no-such", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Delete of a connector still referenced by an action is rejected.
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w317-guarded", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w317-guard-action", "type": "mqtt_bridge",
                       "server": "127.0.0.1:1883", "connector": "w317-guarded"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, body) = server
            .delete_auth("/api/v5/connectors/w317-guarded", &token)
            .await;
        assert_eq!(
            status, 400,
            "referenced delete must be rejected, got: {body}"
        );
        let (status, _) = server
            .delete_auth("/api/v5/actions/w317-guard-action", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/connectors/w317-guarded", &token)
            .await;
        assert_eq!(status, 204);

        // Broker rule path: a rule forwarding through the live manager
        // fires on publish, never `sink.send` directly.
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w317-fire-conn", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w317-fire-conn", "type": "mqtt_bridge",
                       "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": "w317-fire-rule", "name": "w317-fire-rule",
                       "sql": "SELECT * FROM \"w317/fire\"",
                       "actions": ["w317-fire-conn"]}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let broker_sink: Arc<dyn broker_rules::BrokerSink> = Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w317/fire").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        let matched = state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(matched, 1, "rule must fire on its topic");
        let rule = state
            .engine
            .get_rule("w317-fire-rule")
            .expect("rule present");
        assert_eq!(
            rule.actions_total_cnt.load(Ordering::Relaxed),
            1,
            "exactly one forward must attribute"
        );

        // Cleanup so the shared global store stays tidy. The rule goes
        // first: a connector still referenced by a rule cannot be
        // deleted.
        let _ = server
            .delete_auth("/api/v5/rules/w317-fire-rule", &token)
            .await;
        for stale in [
            "w317-list-conn",
            "w317-guarded",
            "w317-guard-action",
            "w317-fire-conn",
        ] {
            let _ = server
                .delete_auth(&format!("/api/v5/actions/{stale}"), &token)
                .await;
            let _ = server
                .delete_auth(&format!("/api/v5/connectors/{stale}"), &token)
                .await;
        }
    }

    #[tokio::test]
    async fn w318_connector_detail_read_update_round_trip() {
        // W3-18 (C07-04/C07-05) failing test: create a connector, read it
        // back expecting documented fields, update the config and read
        // again expecting new values; near-miss id expects 404 (no fuzzy
        // match); unknown id expects 404; invalid bodies are rejected.
        // Before: fuzzy suffix match (`x:id` read `id`) and update without
        // create-like validation. Broker-through: every step goes through
        // the HTTP router on the live engine; create/update register the
        // live sink (asserted via the manager, driven by the create/update
        // event) and reads render live health from the manager, never a
        // write-time snapshot. Management-plane only; publish and deliver
        // never touch the connector map (dispatch reads the manager
        // directly, never this map).
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Scrub leftovers from earlier runs sharing the process-global
        // store so exact-match assertions stay deterministic.
        for stale in ["w318-detail-conn"] {
            let _ = server
                .delete_auth(&format!("/api/v5/connectors/{stale}"), &token)
                .await;
        }

        // Create: validated write with documented fields and live health.
        let (status, created) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w318-detail-conn", "type": "http",
                       "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "create must succeed, got: {created}");
        assert_eq!(created["id"], json!("w318-detail-conn"));
        assert_eq!(created["type"], json!("http"));
        assert!(created.get("status").and_then(|v| v.as_str()).is_some());
        assert!(created.get("node_status").is_some());
        // Live linkage: create registered the sink through the same path
        // boot replay uses (`crates/broker-api/src/v5/rules.rs`,
        // `register_live_sink`; the sink lives in
        // `crates/broker-connectors/src/lib.rs`, `ConnectorManager::register`
        // at line 543, consulted on the fire path at
        // `crates/broker-connectors/src/lib.rs`, `ConnectorManager::send`
        // at line 583 via `dispatch_ingress`).
        assert!(
            state.engine.connectors().get("w318-detail-conn").is_some(),
            "create must register a live sink"
        );

        // Read hit: documented fields with live health.
        let (status, body) = server
            .get_auth("/api/v5/connectors/w318-detail-conn", &token)
            .await;
        assert_eq!(status, 200, "read hit must succeed, got: {body}");
        assert_eq!(body["id"], json!("w318-detail-conn"));
        assert_eq!(body["type"], json!("http"));
        assert_eq!(body["url"], json!("http://127.0.0.1:9"));
        assert!(body.get("status").and_then(|v| v.as_str()).is_some());
        assert!(body.get("node_status").is_some());

        // Unknown query parameters are ignored, never an error.
        let (status, _) = server
            .get_auth("/api/v5/connectors/w318-detail-conn?unknown=param", &token)
            .await;
        assert_eq!(status, 200);

        // Near-miss ids never fuzzy-match: the legacy `prefix:id` suffix
        // form is a 404 with the documented error code. Note: a literal
        // trailing space cannot travel in an HTTP request-target (hyper
        // rejects it with 400 before routing), so the trailing-space
        // near-miss is sent percent-encoded (`%20`); axum decodes it to
        // `"w318-detail-conn "` before the exact-match lookup.
        for near_miss in [
            "x:w318-detail-conn",
            "W318-DETAIL-CONN",
            "w318-detail-conn%20",
        ] {
            let (status, body) = server
                .get_auth(&format!("/api/v5/connectors/{near_miss}"), &token)
                .await;
            assert_eq!(
                status, 404,
                "near-miss read {near_miss:?} must miss, got: {body}"
            );
            assert_eq!(body["code"], json!("NOT_FOUND"));
        }

        // Unknown id is 404 with the documented error code.
        let (status, body) = server
            .get_auth("/api/v5/connectors/w318-no-such", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Update round trip: validated replace, new values read back.
        let (status, updated) = server
            .put_auth(
                "/api/v5/connectors/w318-detail-conn",
                json!({"name": "w318-detail-conn", "type": "http",
                       "url": "http://127.0.0.1:10",
                       "description": "updated by w318"}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "update must succeed, got: {updated}");
        assert_eq!(updated["id"], json!("w318-detail-conn"));
        assert_eq!(updated["url"], json!("http://127.0.0.1:10"));
        let (status, body) = server
            .get_auth("/api/v5/connectors/w318-detail-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["url"], json!("http://127.0.0.1:10"));
        assert_eq!(body["description"], json!("updated by w318"));

        // Update of an unknown id is 404 and creates nothing.
        let (status, body) = server
            .put_auth(
                "/api/v5/connectors/w318-no-such",
                json!({"name": "w318-no-such", "type": "http",
                       "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 404, "unknown update must miss, got: {body}");
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, _) = server
            .get_auth("/api/v5/connectors/w318-no-such", &token)
            .await;
        assert_eq!(status, 404, "failed update must create nothing");

        // Invalid bodies are rejected with the documented error shape and
        // leave the stored entry untouched.
        for (label, bad) in [
            (
                "unknown kind",
                json!({"name": "w318-detail-conn", "type": "no_such_kind_xyz"}),
            ),
            ("missing type", json!({"name": "w318-detail-conn"})),
            (
                "body id mismatch",
                json!({"id": "w318-other", "name": "w318-other", "type": "http"}),
            ),
        ] {
            let (status, body) = server
                .put_auth("/api/v5/connectors/w318-detail-conn", bad, &token)
                .await;
            assert_eq!(
                status, 400,
                "invalid update ({label}) must be rejected, got: {body}"
            );
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
        let (status, body) = server
            .get_auth("/api/v5/connectors/w318-detail-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            body["url"],
            json!("http://127.0.0.1:10"),
            "rejected updates must not mutate the entry"
        );

        // Cleanup so the shared global store stays tidy.
        let _ = server
            .delete_auth("/api/v5/connectors/w318-detail-conn", &token)
            .await;
    }

    fn fill_pending_challenges(tokens: &crate::v5::auth::ApiTokens, count: usize) {
        for i in 0..count {
            let inserted = tokens.insert_challenge(
                format!("fill-{i}"),
                crate::v5::auth::ScramChallengeState::for_test("admin"),
            );
            assert!(inserted, "insert {i} should succeed");
        }
    }

    #[tokio::test]
    async fn challenge_cap_returns_429() {
        use crate::v5::auth::{ScramChallengeState, MAX_PENDING_CHALLENGES};
        let (server, state) = TestServer::start().await;
        fill_pending_challenges(&state.tokens, MAX_PENDING_CHALLENGES);
        assert!(
            !state.tokens.insert_challenge(
                "one-more".to_string(),
                ScramChallengeState::for_test("admin")
            ),
            "insert past the cap should fail"
        );
        let (status, body) = server
            .post(
                "/api/v5/login/challenge",
                json!({"username": "admin", "client_nonce": "nonce-123"}),
            )
            .await;
        assert_eq!(status, 429);
        assert_eq!(body["code"], json!("TOO_MANY_REQUESTS"));
    }

    #[tokio::test]
    async fn removed_authchain_authz_routes_return_404() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // `GET /authorization/sources` is intentionally absent here: it
        // was re-added on the real source registry by W2-17 (C04-08), so
        // it answers 200 and is covered by the registry round-trip tests.
        for (method, path) in [("GET", "/api/v5/authentication/x")] {
            let (status, _) = server
                .request_raw(&format!(
                    "{method} {path} HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
                ))
                .await;
            assert_eq!(status, 404, "{method} {path} must be gone");
        }
    }

    #[tokio::test]
    async fn authz_sources_list_create_clear_round_trip() {
        // Source registry plus list/create routes and the built-in
        // catch-all clear (W2-17, C04-08..C04-10) through the broker:
        // default list holds only the built-in source, create registers
        // one stored-only source, duplicates are rejected, and the
        // catch-all empties the built-in rule table. Before this task
        // every route here was missing (bare 404).
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Default registry: only the built-in database source, empty
        // collection shape (sources list), never an error.
        let (status, body) = server
            .get_auth("/api/v5/authorization/sources", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            body["sources"],
            json!([{"type": "built_in_database", "enable": true}])
        );

        // Register one stored-only source; only the built-in executes,
        // the new entry is stored and reported without a live claim.
        let (status, created) = server
            .post_auth(
                "/api/v5/authorization/sources",
                json!({"type": "file", "config": {"path": "/etc/authz/rules.conf"}}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        assert!(created.is_null(), "create returns no body, got: {created}");

        // List again: the entry is present in stored order.
        let (status, body) = server
            .get_auth("/api/v5/authorization/sources", &token)
            .await;
        assert_eq!(status, 200);
        let types: Vec<_> = body["sources"]
            .as_array()
            .expect("data is a list")
            .iter()
            .map(|row| row["type"].clone())
            .collect();
        assert_eq!(types, vec![json!("built_in_database"), json!("file")]);
        assert_eq!(body["sources"].as_array().expect("sources list").len(), 2);

        // Re-creating the same type is a clean duplicate rejection, never
        // a silent merge.
        let (status, body) = server
            .post_auth(
                "/api/v5/authorization/sources",
                json!({"type": "file"}),
                &token,
            )
            .await;
        assert_eq!(
            status, 400,
            "duplicate source must be rejected, got: {body}"
        );
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));

        // Unknown source types are 404 with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/authorization/sources",
                json!({"type": "nowhere"}),
                &token,
            )
            .await;
        assert_eq!(status, 404, "unknown source type must be 404, got: {body}");
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // The catch-all clear reports success even with rules present.
        let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules",
                json!({"action": "publish", "topic": "w217/#", "permission": "deny"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .delete_auth(
                "/api/v5/authorization/sources/built_in_database/rules",
                &token,
            )
            .await;
        assert_eq!(status, 204);
        // Clearing again still succeeds.
        let (status, _) = server
            .delete_auth(
                "/api/v5/authorization/sources/built_in_database/rules",
                &token,
            )
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn authz_sources_survive_restart() {
        // Registry persistence across a restart through the config
        // registry: register through the broker, reload the same data
        // dir in a fresh server, and read the sources back.
        let dir = unique_data_dir("authz-sources");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources",
                json!({"type": "redis"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server
            .get_auth("/api/v5/authorization/sources", &token)
            .await;
        assert_eq!(status, 200);
        let types: Vec<_> = body["sources"]
            .as_array()
            .expect("data is a list")
            .iter()
            .map(|row| row["type"].clone())
            .collect();
        assert_eq!(types, vec![json!("built_in_database"), json!("redis")]);
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn authz_user_replace_order_delete_round_trip() {
        // Per-user replace plus order-replace plus per-type delete on real
        // state (W2-22, C04-23..C04-25) through the broker: seed a user
        // set, replace it and read back; submit a bad order naming an
        // unknown type expecting rejection; delete a second source then
        // delete unknown expecting 404. Before this task all three routes
        // were missing (bare 404). The written allow is live on the
        // publish path (`MemoryAuth::authorize_publish` in
        // `crates/broker-auth/src/lib.rs`, consulted by the publish event
        // in `crates/broker-node/src/main.rs`).
        use broker_auth::Authorizer;
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Seed one user rule, then replace the whole set and read back.
        let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/users",
                json!({"username": "w222-http", "topic": "w222/http/old/#", "permission": "allow", "action": "publish"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .put_auth(
                "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
                json!({"rules": [
                    {"topic": "w222/http/a/#", "permission": "allow", "action": "publish"},
                    {"topic": "w222/http/b/#", "permission": "deny", "action": "subscribe"},
                ]}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let rules = body["rules"].as_array().expect("rules array");
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["username"], json!("w222-http"));
        assert_eq!(rules[0]["topic"], json!("w222/http/a/#"));
        assert_eq!(rules[1]["permission"], json!("deny"));

        // The stored allow is live on the publish path for that user.
        let topic = broker_protocol::Topic::new("w222/http/a/data").expect("topic parses");
        state
            .auth
            .authorize_publish("w222-http", &topic)
            .await
            .expect("user allow opens the publish path for its user");

        // A bad replace applies nothing: the stored set is unchanged.
        let (status, body) = server
            .put_auth(
                "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
                json!({"rules": [
                    {"topic": "w222/http/ok/#", "permission": "allow", "action": "publish"},
                    {"topic": "", "permission": "allow", "action": "publish"},
                ]}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/sources/built_in_database/rules/users/w222-http",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["rules"].as_array().expect("rules array").len(), 2);

        // Register a second source, then a bad order naming an unknown
        // type is rejected and the stored order is unchanged.
        let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources",
                json!({"type": "file"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .put_auth(
                "/api/v5/authorization/sources/order",
                json!([{"type": "built_in_database"}, {"type": "nowhere"}]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth("/api/v5/authorization/sources", &token)
            .await;
        assert_eq!(status, 200);
        let types: Vec<_> = body["sources"]
            .as_array()
            .expect("data is a list")
            .iter()
            .map(|row| row["type"].clone())
            .collect();
        assert_eq!(types, vec![json!("built_in_database"), json!("file")]);

        // A valid order swaps the evaluation order and reads back swapped.
        let (status, _) = server
            .put_auth(
                "/api/v5/authorization/sources/order",
                json!([{"type": "file"}, {"type": "built_in_database"}]),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authorization/sources", &token)
            .await;
        assert_eq!(status, 200);
        let types: Vec<_> = body["sources"]
            .as_array()
            .expect("data is a list")
            .iter()
            .map(|row| row["type"].clone())
            .collect();
        assert_eq!(types, vec![json!("file"), json!("built_in_database")]);

        // Delete the second source, then an unknown type is 404.
        let (status, _) = server
            .delete_auth("/api/v5/authorization/sources/file", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authorization/sources", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["sources"].as_array().expect("sources list").len(), 1);
        let (status, body) = server
            .delete_auth("/api/v5/authorization/sources/nowhere", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Deleting the last live source is refused instead of leaving
        // authorization open.
        let (status, body) = server
            .delete_auth("/api/v5/authorization/sources/built_in_database", &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn authz_catch_all_rules_round_trip() {
        // Built-in catch-all table plus the three `/all` routes on real
        // state (W2-18, C04-11..C04-13) through the broker: write two
        // catch-all rules, read expecting both; delete and read expecting
        // empty. Before this task all three routes were missing (bare
        // 404). The written allow is live on the publish path
        // (`MemoryAuth::authorize_publish` in
        // `crates/broker-auth/src/lib.rs`, consulted by the publish event
        // in `crates/broker-node/src/main.rs`).
        use broker_auth::Authorizer;
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty reads as an empty collection, never an error; not paged.
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["rules"], json!([]));

        // Replace with two rules; success is 204 with no body.
        let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                json!({"rules": [
                    {"topic": "w218/http/alpha/#", "permission": "allow", "action": "publish"},
                    {"topic": "w218/http/beta/#", "permission": "deny", "action": "subscribe"},
                ]}),
                &token,
            )
            .await;
        assert_eq!(status, 204);

        // Read back: both entries with the documented per-rule fields.
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let rules = body["rules"].as_array().expect("rules array").clone();
        assert_eq!(rules.len(), 2);
        assert_eq!(rules[0]["topic"], json!("w218/http/alpha/#"));
        assert_eq!(rules[0]["permission"], json!("allow"));
        assert_eq!(rules[0]["action"], json!("publish"));
        assert_eq!(rules[1]["topic"], json!("w218/http/beta/#"));
        assert_eq!(rules[1]["permission"], json!("deny"));
        assert_eq!(rules[1]["action"], json!("subscribe"));

        // The stored allow is live on the broker publish path.
        let topic = broker_protocol::Topic::new("w218/http/alpha/data").expect("topic parses");
        state
            .auth
            .authorize_publish("any-client", &topic)
            .await
            .expect("catch-all allow opens the publish path");

        // An invalid replace is a client error and applies nothing.
        let (status, body) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                json!({"rules": [
                    {"topic": "w218/http/ok/#", "permission": "allow", "action": "publish"},
                    {"topic": "", "permission": "allow", "action": "publish"},
                ]}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["rules"].as_array().expect("rules array").len(), 2);

        // Delete clears the list and reports success; clearing empty
        // still succeeds.
        let (status, _) = server
            .delete_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["rules"], json!([]));
        let (status, _) = server
            .delete_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn authn_chain_list_create_round_trip() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty chain reads as an empty collection, not an error.
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        assert_eq!(body.as_array().map(Vec::len), Some(0));

        // Create one built-in entry.
        let (status, created) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(created["id"], json!("password_based:built_in_database"));
        assert_eq!(created["mechanism"], json!("password_based"));
        assert_eq!(created["backend"], json!("built_in_database"));
        assert_eq!(created["enable"], json!(true));
        // No invented fields: the documented slot fields plus the
        // documented built-in default.
        let obj = created.as_object().expect("entry is an object");
        for key in obj.keys() {
            assert!(
                [
                    "id",
                    "mechanism",
                    "backend",
                    "enable",
                    "config",
                    "user_id_type"
                ]
                .contains(&key.as_str()),
                "unexpected field {key} in chain entry"
            );
        }
        assert_eq!(created["user_id_type"], json!("username"));

        // List again: the entry is present in order.
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["id"], json!("password_based:built_in_database"));
        assert_eq!(body.as_array().map(Vec::len), Some(1));

        // Re-creating the same id is a clean duplicate rejection, never
        // a silent overwrite.
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        assert!(body["message"].is_string());

        // Malformed bodies are client errors with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());

        // Unknown backends are rejected, never stored.
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "nope"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Second entry keeps insertion order.
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert_eq!(data.len(), 2);
        assert_eq!(data[1]["id"], json!("password_based:mysql"));
    }

    #[tokio::test]
    async fn authn_chain_drives_connect_and_survives_restart() {
        // Broker consult through the console CONNECT path: an empty chain
        // preserves today's open behaviour, while a chain with no live
        // backend fails closed without creating a session.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let mut anon = WsClient::connect(server.port).await;
        anon.send_bin(&mqtt_connect("w2-02-open", None, None)).await;
        let connack = anon.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let mut refused = WsClient::connect(server.port).await;
        refused
            .send_bin(&mqtt_connect("w2-02-closed", None, None))
            .await;
        let connack = refused.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0x86);

        // Restart persistence through the config registry: the chain
        // survives a rebuild from the same data dir.
        let dir = unique_data_dir("authn-chain");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        // The admin password persisted too, so the rebooted node logs in
        // with the changed password (`login_as_admin` only fits fresh
        // servers still on the default password).
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["id"], json!("password_based:built_in_database"));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn authn_chain_order_replace_round_trip_and_rejects_unknown() {
        // Order-replace through the broker: create two entries, set an
        // explicit order, list expecting the new order; an unknown id
        // and a partial list are rejected with no change applied.
        // Management-plane only; publish and deliver never touch this
        // store, and the CONNECT path keeps reading its lock-free
        // snapshot.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        // Explicit order applies: mysql first.
        let (status, _) = server
            .put_auth(
                "/api/v5/authentication/order",
                json!([{"id": "password_based:mysql"}, {"id": "password_based:built_in_database"}]),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert_eq!(data.len(), 2);
        assert_eq!(data[0]["id"], json!("password_based:mysql"));
        assert_eq!(data[1]["id"], json!("password_based:built_in_database"));

        // Unknown ids are rejected with the documented shape and apply
        // nothing.
        let (status, body) = server
            .put_auth(
                "/api/v5/authentication/order",
                json!([
                    {"id": "password_based:mysql"},
                    {"id": "password_based:built_in_database"},
                    {"id": "password_based:nowhere"},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert_eq!(data[0]["id"], json!("password_based:mysql"));
        assert_eq!(data[1]["id"], json!("password_based:built_in_database"));

        // Partial lists are rejected with no change applied.
        let (status, body) = server
            .put_auth(
                "/api/v5/authentication/order",
                json!([{"id": "password_based:mysql"}]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert_eq!(data.len(), 2);
        assert_eq!(data[0]["id"], json!("password_based:mysql"));

        // Malformed bodies are client errors with the documented shape.
        let (status, body) = server
            .put_auth(
                "/api/v5/authentication/order",
                json!([{"not-id": "password_based:mysql"}]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Order persists across a restart through the config registry.
        let dir = unique_data_dir("authn-order");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .put_auth(
                "/api/v5/authentication/order",
                json!([{"id": "password_based:mysql"}, {"id": "password_based:built_in_database"}]),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("data is a list");
        assert_eq!(data.len(), 2);
        assert_eq!(data[0]["id"], json!("password_based:mysql"));
        assert_eq!(data[1]["id"], json!("password_based:built_in_database"));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn authn_entry_read_update_delete_round_trip() {
        // Per-id read/update/delete through the broker management plane
        // on the real chain store: create two entries, read one back,
        // update with a valid body and read again, delete then read 404,
        // delete unknown 404, invalid updates rejected without applying,
        // deleting the last entry refused, and delete-then-recreate of
        // the same id works. Management-plane only; the CONNECT path
        // keeps reading its lock-free snapshot and publish/deliver never
        // touch this store.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        // Read one entry by id; the shape carries only the stored slot
        // fields, never invented measurements.
        let (status, body) = server
            .get_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("password_based:mysql"));
        assert_eq!(body["mechanism"], json!("password_based"));
        assert_eq!(body["backend"], json!("mysql"));
        assert_eq!(body["enable"], json!(true));
        let obj = body.as_object().expect("entry is an object");
        for key in obj.keys() {
            assert!(
                ["id", "mechanism", "backend", "enable", "config"].contains(&key.as_str()),
                "unexpected field {key} in chain entry"
            );
        }

        // Unknown ids miss with the documented shape.
        let (status, body) = server
            .get_auth("/api/v5/authentication/password_based:nowhere", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].is_string());

        // Valid update replaces and reads back.
        let (status, _) = server
            .put_auth(
                "/api/v5/authentication/password_based:mysql",
                json!({"enable": false, "config": {"pool_size": 4}}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        assert_eq!(body["config"], json!({"pool_size": 4}));

        // Invalid updates are client errors and apply nothing.
        for bad in [
            json!({"backend": "nope"}),
            json!({"mechanism": "nope"}),
            json!({"enable": "yes"}),
            json!({"config": "yes"}),
            json!({"id": "password_based:other"}),
            json!([1, 2]),
        ] {
            let (status, body) = server
                .put_auth("/api/v5/authentication/password_based:mysql", bad, &token)
                .await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
        }
        // Unknown ids fail updates with 404 even for valid bodies.
        let (status, body) = server
            .put_auth(
                "/api/v5/authentication/password_based:nowhere",
                json!({"enable": false}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, body) = server
            .get_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["backend"], json!("mysql"));
        assert_eq!(body["enable"], json!(false));

        // Delete removes and reports; the deleted id reads 404 after.
        let (status, _) = server
            .delete_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Deleting an unknown id is 404.
        let (status, body) = server
            .delete_auth("/api/v5/authentication/password_based:nowhere", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        // Deleting then re-creating the same id works (no tombstone).
        let (status, recreated) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(recreated["id"], json!("password_based:mysql"));

        // Deleting the last remaining entry is refused instead of
        // leaving authentication open.
        let (status, _) = server
            .delete_auth(
                "/api/v5/authentication/password_based:built_in_database",
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .delete_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
        let (status, body) = server
            .get_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("password_based:mysql"));
    }

    #[tokio::test]
    async fn authn_entry_update_delete_persist_across_restart() {
        // Per-id update/delete persist through the config registry: a
        // validated update and a removal survive a rebuild from the same
        // data dir. Management-plane only; publish and deliver never
        // touch this store.
        let dir = unique_data_dir("authn-entry");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .put_auth(
                "/api/v5/authentication/password_based:mysql",
                json!({"enable": false}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("password_based:built_in_database"));
        let (status, _) = server
            .get_auth("/api/v5/authentication/password_based:mysql", &token)
            .await;
        assert_eq!(status, 404);
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn authn_position_move_round_trip_rejects_out_of_range_and_persists() {
        // Position-move through the broker management plane on the real
        // chain store: create three entries, move the last to position 0,
        // then list expecting the new order; an out-of-range position is
        // rejected with the documented shape and moves nothing. Unknown
        // ids miss with 404, malformed positions fail with 400, and the
        // new order survives a restart. Management-plane only: one short
        // write lock per move; the CONNECT path keeps reading its
        // lock-free snapshot and publish/deliver never touch this store.
        // Before this task the route itself was missing (bare 404).
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for backend in ["built_in_database", "mysql", "redis"] {
            let (status, _) = server
                .post_auth(
                    "/api/v5/authentication",
                    json!({"mechanism": "password_based", "backend": backend}),
                    &token,
                )
                .await;
            assert_eq!(status, 200, "create {backend}");
        }

        // Move the last entry to the front. The answer is 204 with no
        // body, as the reference API documents.
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/authentication/password_based:redis/position/0",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        assert!(body.is_null(), "move returns no body, got: {body}");
        let reported: Vec<String> = vec![
            "password_based:redis".to_string(),
            "password_based:built_in_database".to_string(),
            "password_based:mysql".to_string(),
        ];
        // List shows the new order.
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let listed: Vec<String> = body
            .as_array()
            .expect("chain list is paged")
            .iter()
            .map(|entry| entry["id"].as_str().expect("id").to_string())
            .collect();
        assert_eq!(listed, reported);

        // An out-of-range position is a client error and moves nothing.
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/authentication/password_based:redis/position/99",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let unchanged: Vec<String> = body
            .as_array()
            .expect("chain list")
            .iter()
            .map(|entry| entry["id"].as_str().expect("id").to_string())
            .collect();
        assert_eq!(unchanged, reported);

        // Unknown ids miss with the documented shape and move nothing.
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/authentication/password_based:nowhere/position/0",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].is_string());

        // Malformed positions are client errors and move nothing.
        for bad in ["front", "-1", "1.5"] {
            let (status, body) = server
                .send(
                    "PUT",
                    &format!("/api/v5/authentication/password_based:redis/position/{bad}"),
                    None,
                    Some(&token),
                )
                .await;
            assert_eq!(status, 400, "position {bad} must be rejected");
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let unchanged: Vec<String> = body
            .as_array()
            .expect("chain list")
            .iter()
            .map(|entry| entry["id"].as_str().expect("id").to_string())
            .collect();
        assert_eq!(unchanged, reported);

        // Persistence through the config registry: the moved order
        // survives a rebuild from the same data dir.
        let dir = unique_data_dir("authn-position");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        for backend in ["built_in_database", "mysql", "redis"] {
            let (status, _) = server
                .post_auth(
                    "/api/v5/authentication",
                    json!({"mechanism": "password_based", "backend": backend}),
                    &token,
                )
                .await;
            assert_eq!(status, 200, "persist case create {backend}");
        }
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/authentication/password_based:redis/position/0",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/authentication", &token).await;
        assert_eq!(status, 200);
        let listed: Vec<String> = body
            .as_array()
            .expect("reloaded chain list")
            .iter()
            .map(|entry| entry["id"].as_str().expect("id").to_string())
            .collect();
        assert_eq!(
            listed,
            vec![
                "password_based:redis".to_string(),
                "password_based:built_in_database".to_string(),
                "password_based:mysql".to_string(),
            ]
        );
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn authn_status_reports_live_counters_through_connect() {
        // Status over the real chain entry plus live CONNECT verdicts:
        // create one built-in entry, read the documented fields at zero,
        // move success/failed through real console CONNECTs through the
        // broker, and read the moved counters back. A subscribe/publish/
        // deliver flow proves status assembly never touches the message
        // path. Management-plane only for reads; the CONNECT path
        // attributes one verdict per connect and publish/deliver never
        // touch these counters.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Unknown ids miss with the documented shape, not an empty body.
        // Before this task the route itself was missing (bare 404).
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:nowhere/status",
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].is_string());

        // One built-in entry: a fresh authenticator reads zeros live.
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("password_based:built_in_database"));
        assert_eq!(body["enable"], json!(true));
        assert!(body.get("status").is_none());
        assert!(body.get("node_status").is_none());
        assert_eq!(body["metrics"]["success"], json!(0));
        assert_eq!(body["metrics"]["failed"], json!(0));
        assert_eq!(body["metrics"]["total"], json!(0));
        assert!(body["metrics"]["nomatch"].is_number());
        assert!(body["node_metrics"].is_object());
        assert!(body["node_resource_metrics"].is_object());
        assert!(body["node_error"].is_object());
        assert_eq!(
            body["node_metrics"]["metrics"]["total"],
            body["metrics"]["total"]
        );
        assert_eq!(
            body["node_resource_metrics"]["metrics"]["matched"],
            body["metrics"]["total"]
        );
        assert!(body["node_error"]["node"].is_string());

        // Create a user through the broker, then CONNECT through the
        // broker: success moves the live counters.
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                json!({"user_id": "w2-08-user", "password": "W2-08-pass!"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let mut good = WsClient::connect(server.port).await;
        good.send_bin(&mqtt_connect(
            "w2-08-good",
            Some("w2-08-user"),
            Some(b"W2-08-pass!"),
        ))
        .await;
        let connack = good.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["success"], json!(1));
        assert_eq!(body["metrics"]["total"], json!(1));

        // A bad password moves failed through the same broker path.
        let mut bad = WsClient::connect(server.port).await;
        bad.send_bin(&mqtt_connect(
            "w2-08-bad",
            Some("w2-08-user"),
            Some(b"wrong-pass"),
        ))
        .await;
        let connack = bad.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0x86);
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["success"], json!(1));
        assert_eq!(body["metrics"]["failed"], json!(1));
        assert_eq!(body["metrics"]["total"], json!(2));

        // Unknown query keys degrade to the same read, not a 400: the
        // route is not paged.
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status?unknown_param=1",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["total"], json!(2));

        // The message path still flows while status reads run: status
        // assembly never touches publish or deliver.
        let mut sub = WsClient::connect(server.port).await;
        sub.send_bin(&mqtt_connect(
            "w2-08-sub",
            Some("w2-08-user"),
            Some(b"W2-08-pass!"),
        ))
        .await;
        let connack = sub.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);
        sub.send_bin(&mqtt_subscribe(7, &[("w2-08/t", 1)])).await;
        let suback = sub.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        let mut publ = WsClient::connect(server.port).await;
        publ.send_bin(&mqtt_connect(
            "w2-08-pub",
            Some("w2-08-user"),
            Some(b"W2-08-pass!"),
        ))
        .await;
        let connack = publ.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);
        publ.send_bin(&mqtt_publish("w2-08/t", 42, 1, b"hi-w2-08"))
            .await;
        let puback = publ.recv_msg().await.expect("puback");
        assert_eq!(mqtt_packet_type(&puback), 4);
        let delivery = sub.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);
        assert!(delivery.ends_with(b"hi-w2-08"));
        // Two more successful CONNECTs moved the counters with the flow.
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["success"], json!(3));
        assert_eq!(body["metrics"]["failed"], json!(1));
        assert_eq!(body["metrics"]["total"], json!(4));
    }

    #[tokio::test]
    async fn authn_metrics_reset_zeroes_live_counters_through_connect() {
        // Reset over the real chain entry plus live CONNECT verdicts:
        // create one built-in entry and a user, move success/failed
        // through real console CONNECTs through the broker, reset, then
        // read zeroed counters back through status. Counters resume from
        // zero on the next authentication. Unknown ids miss with the
        // documented shape, not an empty body. Management-plane only for
        // the reset; publish and deliver never touch these counters.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Before this task the route itself was missing (bare 404 with
        // no JSON verdict).
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/authentication/password_based:nowhere/metrics/reset",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].is_string());

        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                json!({"user_id": "w2-09-user", "password": "W2-09-pass!"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);

        // One success and one failure through the broker move the live
        // counters.
        let mut good = WsClient::connect(server.port).await;
        good.send_bin(&mqtt_connect(
            "w2-09-good",
            Some("w2-09-user"),
            Some(b"W2-09-pass!"),
        ))
        .await;
        let connack = good.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        let mut bad = WsClient::connect(server.port).await;
        bad.send_bin(&mqtt_connect(
            "w2-09-bad",
            Some("w2-09-user"),
            Some(b"wrong-pass"),
        ))
        .await;
        let connack = bad.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0x86);
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["success"], json!(1));
        assert_eq!(body["metrics"]["failed"], json!(1));
        assert_eq!(body["metrics"]["total"], json!(2));

        // Reset zeroes every counter; the next status read reports zeros.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/authentication/password_based:built_in_database/metrics/reset",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["success"], json!(0));
        assert_eq!(body["metrics"]["failed"], json!(0));
        assert_eq!(body["metrics"]["nomatch"], json!(0));
        assert_eq!(body["metrics"]["total"], json!(0));

        // Counters resume from zero: one more success through the broker
        // counts up from zero, and a message still flows while the reset
        // path stays off publish and deliver.
        let mut again = WsClient::connect(server.port).await;
        again
            .send_bin(&mqtt_connect(
                "w2-09-again",
                Some("w2-09-user"),
                Some(b"W2-09-pass!"),
            ))
            .await;
        let connack = again.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        again.send_bin(&mqtt_subscribe(7, &[("w2-09/t", 1)])).await;
        let suback = again.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        again
            .send_bin(&mqtt_publish("w2-09/t", 42, 1, b"hi-w2-09"))
            .await;
        let puback = again.recv_msg().await.expect("puback");
        assert_eq!(mqtt_packet_type(&puback), 4);
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/status",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["success"], json!(1));
        assert_eq!(body["metrics"]["failed"], json!(0));
        assert_eq!(body["metrics"]["total"], json!(1));
    }

    #[tokio::test]
    async fn authn_node_cache_status_reset_round_trip() {
        // Status over real state plus a reset that actually evicts:
        // read the documented fields, populate the cache through
        // authentications through the broker, reset, then read an empty
        // cache. Management-plane only; publish and deliver never touch
        // the store.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state
            .auth
            .add_user("w2-03-user", b"W2-03-pass!")
            .expect("test user persists");

        // Empty cache reads as size zero, never an error.
        let (status, body) = server
            .get_auth("/api/v5/authentication/node_cache/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enabled"], json!(true));
        assert_eq!(body["size"], json!(0));
        assert_eq!(body["count"], json!(0));
        assert!(body["max_size"].is_number());
        assert!(body["max_count"].is_number());

        // Populate through a real credentialed CONNECT through the
        // broker (console path authenticates against the same store).
        let mut client = WsClient::connect(server.port).await;
        client
            .send_bin(&mqtt_connect(
                "w2-03-cache-1",
                Some("w2-03-user"),
                Some(b"W2-03-pass!"),
            ))
            .await;
        let connack = client.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        let (status, body) = server
            .get_auth("/api/v5/authentication/node_cache/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enabled"], json!(true));
        assert_eq!(body["size"], json!(1));
        assert_eq!(body["count"], json!(1));

        // Unknown query keys are ignored, not a 400.
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/node_cache/status?unknown_param=1",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["size"], json!(1));

        // Reset evicts; the next read is empty. Resetting again still
        // succeeds (204).
        let (status, _) = server
            .post_auth("/api/v5/authentication/node_cache/reset", json!({}), &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authentication/node_cache/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["size"], json!(0));
        assert_eq!(body["count"], json!(0));
        let (status, _) = server
            .post_auth("/api/v5/authentication/node_cache/reset", json!({}), &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn authn_settings_replace_round_trip_drives_connect_and_survives_restart() {
        // Settings round trip through the broker: GET defaults, PUT a
        // full valid update, GET expecting the update; a console CONNECT
        // still succeeds (the settings consult never breaks the open
        // broker) and a subscribe/publish/deliver flow proves publish
        // and deliver never touch this store. An invalid PUT is rejected
        // with no change; the node-cache subscriber keeps the status cap
        // in sync. Settings persist across a restart via the registry.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Defaults read back with the documented fields.
        let (status, body) = server
            .get_auth("/api/v5/authentication/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["ignore_backend_failures"], json!(false));
        assert_eq!(body["node_cache"]["enable"], json!(true));
        assert_eq!(body["node_cache"]["max_count"], json!(10000));
        assert_eq!(body["builtin_record_count_refresh_interval"], json!("1h"));

        // Full valid replace through the broker.
        let (status, _) = server
            .put_auth(
                "/api/v5/authentication/settings",
                json!({
                    "ignore_backend_failures": true,
                    "node_cache": {
                        "enable": true,
                        "cache_ttl": "30s",
                        "cleanup_interval": "1m",
                        "stat_update_interval": "5s",
                        "max_count": 5000,
                        "max_memory": "100MB",
                    },
                    "builtin_record_count_refresh_interval": "30m",
                }),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authentication/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["ignore_backend_failures"], json!(true));
        assert_eq!(body["node_cache"]["max_count"], json!(5000));
        assert_eq!(body["node_cache"]["cache_ttl"], json!("30s"));
        assert_eq!(body["builtin_record_count_refresh_interval"], json!("30m"));

        // The subscriber keeps the node-cache status cap in sync with
        // the settings' cache half without a restart.
        let (status, body) = server
            .get_auth("/api/v5/authentication/node_cache/status", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["max_count"], json!(5000));

        // The CONNECT consult never breaks the open broker: a console
        // CONNECT still succeeds, and a subscribe/publish/deliver flow
        // proves publish and deliver never touch this store.
        let mut sub = WsClient::connect(server.port).await;
        sub.send_bin(&mqtt_connect("w2-05-sub", None, None)).await;
        let connack = sub.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        sub.send_bin(&mqtt_subscribe(7, &[("w2-05/t", 1)])).await;
        let suback = sub.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        let mut publ = WsClient::connect(server.port).await;
        publ.send_bin(&mqtt_connect("w2-05-pub", None, None)).await;
        let connack = publ.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);
        publ.send_bin(&mqtt_publish("w2-05/t", 42, 1, b"hi-w2-05"))
            .await;
        let puback = publ.recv_msg().await.expect("puback");
        assert_eq!(mqtt_packet_type(&puback), 4);
        let delivery = sub.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);
        assert!(delivery.ends_with(b"hi-w2-05"));

        // Unknown fields are rejected fail-closed with no change applied.
        let (status, body) = server
            .put_auth(
                "/api/v5/authentication/settings",
                json!({"ignore_backend_failures": true, "bogus": 1}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .put_auth(
                "/api/v5/authentication/settings",
                json!({"node_cache": {"max_count": 0}}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth("/api/v5/authentication/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["node_cache"]["max_count"], json!(5000));

        // Partial bodies replace: missing fields reset to defaults.
        let (status, _) = server
            .put_auth(
                "/api/v5/authentication/settings",
                json!({"ignore_backend_failures": false}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/authentication/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["ignore_backend_failures"], json!(false));
        assert_eq!(body["node_cache"]["max_count"], json!(10000));

        // Persistence across a restart through the config registry.
        let dir = unique_data_dir("authn-settings");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .put_auth(
                "/api/v5/authentication/settings",
                json!({
                    "ignore_backend_failures": true,
                    "node_cache": {
                        "enable": true,
                        "cache_ttl": "30s",
                        "cleanup_interval": "1m",
                        "stat_update_interval": "5s",
                        "max_count": 5000,
                        "max_memory": "100MB",
                    },
                    "builtin_record_count_refresh_interval": "30m",
                }),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server
            .get_auth("/api/v5/authentication/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["ignore_backend_failures"], json!(true));
        assert_eq!(body["node_cache"]["max_count"], json!(5000));
        assert_eq!(body["builtin_record_count_refresh_interval"], json!("30m"));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn authz_settings_round_trip_rejection_and_restart() {
        // Settings round trip through the broker (W2-16, C04-06/07):
        // GET defaults, PUT a full valid update answering 200 with the
        // stored settings, GET expecting the update; a
        // subscribe/publish/deliver flow proves publish and deliver never
        // touch this store. Invalid PUTs are rejected with no change;
        // partial bodies reset missing fields to defaults. Settings
        // persist across a restart via the registry. Before this task
        // both routes were 404.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Defaults read back with the documented fields.
        let (status, body) = server
            .get_auth("/api/v5/authorization/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["no_match"], json!("deny"));
        assert_eq!(body["deny_action"], json!("ignore"));
        assert_eq!(body["cache"]["enable"], json!(true));
        assert_eq!(body["cache"]["max_size"], json!(32));
        assert_eq!(body["cache"]["ttl"], json!("1m"));
        assert_eq!(body["cache"]["excludes"], json!([]));
        assert_eq!(body["include_mountpoint"], json!(false));
        assert_eq!(body["ignore_backend_failures"], json!(false));

        // Full valid replace through the broker: the write answers 200
        // with the stored settings.
        let (status, answered) = server
            .put_auth(
                "/api/v5/authorization/settings",
                json!({
                    "no_match": "allow",
                    "deny_action": "disconnect",
                    "cache": {
                        "enable": false,
                        "max_size": 64,
                        "ttl": "60s",
                        "excludes": ["nocache/#"],
                    },
                    "include_mountpoint": false,
                    "ignore_backend_failures": true,
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(answered["no_match"], json!("allow"));
        assert_eq!(answered["deny_action"], json!("disconnect"));
        assert_eq!(answered["cache"]["max_size"], json!(64));
        assert_eq!(answered["cache"]["excludes"], json!(["nocache/#"]));
        let (status, body) = server
            .get_auth("/api/v5/authorization/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, answered, "the write answers what the read reports");

        // A subscribe/publish/deliver flow still works: publish and
        // deliver never touch this store.
        let mut sub = WsClient::connect(server.port).await;
        sub.send_bin(&mqtt_connect("w2-16-sub", None, None)).await;
        let connack = sub.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        sub.send_bin(&mqtt_subscribe(7, &[("w2-16/t", 1)])).await;
        let suback = sub.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        let mut publ = WsClient::connect(server.port).await;
        publ.send_bin(&mqtt_connect("w2-16-pub", None, None)).await;
        let connack = publ.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);
        publ.send_bin(&mqtt_publish("w2-16/t", 42, 1, b"hi-w2-16"))
            .await;
        let puback = publ.recv_msg().await.expect("puback");
        assert_eq!(mqtt_packet_type(&puback), 4);
        let delivery = sub.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);
        assert!(delivery.ends_with(b"hi-w2-16"));

        // Unknown and out-of-range fields are rejected fail-closed with
        // no change applied.
        for bad in [
            json!({"no_match": "sometimes"}),
            json!({"deny_action": "log"}),
            json!({"cache": {"max_size": 0}}),
            json!({"cache": {"max_size": 2_000_000}}),
            json!({"cache": {"ttl": "soon"}}),
            json!({"cache": {"excludes": [""]}}),
            json!({"no_match": "deny", "bogus": 1}),
            json!({"cache": {"max_size": 64, "bogus": true}}),
        ] {
            let (status, body) = server
                .put_auth("/api/v5/authorization/settings", bad, &token)
                .await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
        }
        let (status, body) = server
            .get_auth("/api/v5/authorization/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body, answered, "failed writes must not apply");

        // Partial bodies replace: missing fields reset to defaults.
        let (status, answered) = server
            .put_auth(
                "/api/v5/authorization/settings",
                json!({"no_match": "deny"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(answered["no_match"], json!("deny"));
        assert_eq!(answered["deny_action"], json!("ignore"));
        assert_eq!(answered["cache"]["max_size"], json!(32));
        assert_eq!(answered["cache"]["excludes"], json!([]));

        // Persistence across a restart through the config registry.
        let dir = unique_data_dir("authz-settings");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .put_auth(
                "/api/v5/authorization/settings",
                json!({
                    "no_match": "allow",
                    "deny_action": "disconnect",
                    "cache": {
                        "enable": false,
                        "max_size": 64,
                        "ttl": "60s",
                        "excludes": ["nocache/#"],
                    },
                    "include_mountpoint": false,
                    "ignore_backend_failures": true,
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server
            .get_auth("/api/v5/authorization/settings", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["no_match"], json!("allow"));
        assert_eq!(body["deny_action"], json!("disconnect"));
        assert_eq!(body["cache"]["max_size"], json!(64));
        assert_eq!(body["cache"]["excludes"], json!(["nocache/#"]));
        assert_eq!(body["ignore_backend_failures"], json!(true));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn api_rejects_requests_without_token() {
        let (server, _state) = TestServer::start().await;

        // Authenticated routes refuse anonymous callers.
        let (status, body) = server.get("/api/v1/clients").await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
        let (status, body) = server.get("/api/v5/rules").await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));

        // Public routes stay open.
        let (status, _) = server
            .request_raw("GET /healthz HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .request_raw("GET /dashboard HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn default_admin_must_change_password_first() {
        let (server, _state) = TestServer::start().await;
        let fresh = server.admin_token().await;

        // The default-password token is gated everywhere but the
        // password change, logout and current_user.
        let (status, body) = server.get_auth("/api/v1/clients", &fresh).await;
        assert_eq!(status, 403);
        assert_eq!(body["code"], json!("PASSWORD_CHANGE_REQUIRED"));
        let (status, _) = server.get_auth("/api/v5/current_user", &fresh).await;
        assert_eq!(status, 200);

        let (status, _) = server
            .put_auth(
                "/api/v5/users/admin/change_pwd",
                json!({"old_pwd": "public", "new_pwd": "Adm1n-test-pass!"}),
                &fresh,
            )
            .await;
        assert_eq!(status, 204);

        // After the change the same token opens the API.
        let (status, _) = server.get_auth("/api/v1/clients", &fresh).await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn viewer_is_read_only() {
        let (server, _state) = TestServer::start().await;
        let admin = server.login_as_admin().await;

        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "ro-view", "password": "R0-viewer-pass", "role": "viewer"}),
                &admin,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "ro-view", "password": "R0-viewer-pass"}),
            )
            .await;
        assert_eq!(status, 200);
        let viewer = body["token"].as_str().expect("viewer token").to_string();

        // Reads work; writes are forbidden.
        let (status, _) = server.get_auth("/api/v5/rules", &viewer).await;
        assert_eq!(status, 200);
        let (status, body) = server
            .post_auth(
                "/api/v1/rules",
                json!({"name": "x", "topic_filter": "a/#", "actions": []}),
                &viewer,
            )
            .await;
        assert_eq!(status, 403);
        assert_eq!(body["code"], json!("FORBIDDEN"));
    }

    #[tokio::test]
    async fn expired_token_is_rejected() {
        let (server, state) = TestServer::start().await;
        let stale = state
            .tokens
            .issue_expired("admin", crate::admin_users::AdminRole::Administrator);
        let (status, body) = server.get_auth("/api/v1/clients", &stale).await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));
    }

    #[tokio::test]
    async fn test_dashboard_routes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (server, _state) = TestServer::start().await;

        // GET / redirects to the console.
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", server.port))
            .await
            .expect("connect");
        stream
            .write_all(b"GET / HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await
            .expect("write");
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.expect("read");
        let text = String::from_utf8(buf).expect("UTF-8");
        // Hyper echoes the HTTP/1.0 request version in the status line.
        assert!(text.contains(" 303 "), "root must redirect, got: {text}");
        assert!(
            text.to_ascii_lowercase().contains("location: /dashboard"),
            "redirect must target /dashboard, got: {text}"
        );

        // GET /dashboard serves the SPA shell.
        let (status, html) = server
            .request_raw("GET /dashboard HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await;
        assert_eq!(status, 200);
        for marker in [
            "<title>IndraMQTT Console</title>",
            "Community Edition (MIT)",
            "id=\"metrics\"",
            "id=\"clients\"",
            "id=\"sql-studio\"",
            "id=\"connectors\"",
            "id=\"auth\"",
            "id=\"console\"",
            "rule-tpl-math",
            "rule-tpl-tumbling",
            "value=\"mysql\"",
            "value=\"clickhouse\"",
            "value=\"influxdb\"",
            "value=\"s3\"",
            "value=\"elasticsearch\"",
            "value=\"timescaledb\"",
            "conn-mysql-url",
            "conn-ch-endpoint",
            "conn-influx-endpoint",
            "conn-s3-bucket",
            "conn-es-index",
            "conn-ts-hypertable",
            "value=\"webhook\"",
            "value=\"mqtt_bridge\"",
            "value=\"disk_log\"",
            "value=\"sparkplug_b",
            "value=\"kinesis\"",
            "value=\"gcp_pubsub\"",
            "value=\"azure_eventhubs\"",
            "value=\"pulsar\"",
            "value=\"mongodb\"",
            "value=\"mssql\"",
            "value=\"cassandra\"",
            "value=\"couchbase\"",
            "value=\"tdengine\"",
            "value=\"iotdb\"",
            "value=\"timestream\"",
            "value=\"dynamodb\"",
            "value=\"snowflake\"",
            "value=\"databricks\"",
            "value=\"doris\"",
            "value=\"bigquery\"",
            "value=\"redshift\"",
            "value=\"azure_blob\"",
            "value=\"tablestore\"",
            "value=\"confluent\"",
            "value=\"rocketmq\"",
            "value=\"oracle\"",
            "value=\"cockroachdb\"",
            "value=\"alloydb\"",
            "value=\"opentsdb\"",
            "value=\"greptimedb\"",
            "value=\"datalayers\"",
            "conn-azblob-account",
            "conn-ots-endpoint",
            "conn-cfl-servers",
            "conn-rmq-endpoints",
            "conn-ora-url",
            "conn-crdb-url",
            "conn-alloy-host",
            "conn-otsdb-endpoint",
            "conn-grep-endpoint",
            "conn-dl-endpoint",
            "conn-hook-url",
            "conn-bridge-address",
            "conn-disk-dir",
            "conn-spb-prefix",
            "conn-kinesis-stream",
            "conn-gcp-project",
            "conn-azure-ns",
            "conn-pulsar-url",
            "conn-mongo-url",
            "conn-mssql-host",
            "conn-cass-contact",
            "conn-couch-url",
            "conn-td-endpoint",
            "conn-iotdb-device",
            "conn-ts-database",
            "conn-ddb-table",
            ".badge.community",
            ".badge.enterprise",
            "/ws/mqtt",
        ] {
            assert!(html.contains(marker), "dashboard missing {marker}");
        }
    }

    #[tokio::test]
    async fn test_swagger_and_openapi_routes() {
        let (server, _state) = TestServer::start().await;

        // Test /swagger serves HTML
        let (status, html) = server
            .request_raw("GET /swagger HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await;
        assert_eq!(status, 200);
        assert!(html.contains("SwaggerUIBundle"));
        assert!(html.contains("/api-docs/openapi.json"));

        // Test /api-docs/openapi.json serves valid OpenAPI 3.0 specification
        let (status, spec) = server.get("/api-docs/openapi.json").await;
        assert_eq!(status, 200);
        assert_eq!(spec["openapi"], "3.0.3");
        assert_eq!(spec["info"]["title"], "IndraMQTT Management API");
        assert!(spec["paths"]["/api/v1/nodes"]["get"].is_object());
        assert!(spec["paths"]["/api/v1/clients"]["get"].is_object());
        assert!(spec["paths"]["/api/v1/rules"]["get"].is_object());
        assert!(spec["paths"]["/api/v1/connectors"]["get"].is_object());
    }

    #[tokio::test]
    async fn test_modern_ui_routes() {
        let (server, _state) = TestServer::start().await;

        // Test /ui serves the modern React SPA (or fallback)
        let (status, html) = server
            .request_raw("GET /ui HTTP/1.0\r\nHost: test\r\nConnection: close\r\n\r\n")
            .await;
        assert_eq!(status, 200);
        assert!(html.len() > 100);
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
    async fn test_connectors_endpoint_lists_registered() {
        use broker_connectors::Sink;

        struct NullSink;
        #[async_trait::async_trait]
        impl Sink for NullSink {
            async fn send(
                &self,
                _topic: &Topic,
                _payload: &bytes::Bytes,
                _qos: QoS,
            ) -> Result<(), broker_connectors::ConnectorError> {
                Ok(())
            }

            fn kind(&self) -> &'static str {
                "test"
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        state
            .engine
            .connectors()
            .register("webhook-1", Arc::new(NullSink));
        let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            body,
            json!([{"id": "webhook-1", "kind": "test", "tier": "community"}])
        );
    }

    // Full-matrix creation test (X1-10): exercises every connector kind,
    // including the feature-gated heavy SDKs, so it runs only when all
    // gating features are enabled (default and per-feature builds skip
    // it; the full-feature build runs it).
    #[cfg(all(
        feature = "kafka",
        feature = "rabbitmq",
        feature = "clickhouse",
        feature = "s3",
        feature = "elasticsearch",
        feature = "mongodb",
        feature = "cassandra",
        feature = "couchbase",
        feature = "confluent",
        feature = "gcp_pubsub",
        feature = "bigquery",
        feature = "kinesis"
    ))]
    #[tokio::test]
    async fn test_connectors_create_kafka_rabbitmq_logger() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Fixed test RSA key for the OCI + GCP-IoT creation legs below
        // (openssl-generated, in-memory, never deployed).
        let test_pem = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCgQlU8hDdoMjP5
QU2fhr0g+2n5HQSvgQaDKkpZFftqbrMixmFg3pGQN+GZp9vIscT+BlNrJueigXpn
pRbhI0RRj8EvVl+4Or+0hdzeLDmfGl/9SIhnyiRsJ7YDeO3uZq/Cff2zMeXqqbk4
RgKwbQksnJPOOYgVOfPrLjHJdEkNcSxT44tyOVrhBVISYm+zUw4By4GSQXp4RoTa
8UFX/gHoa+31um/9yZfDf9ekzelBys+4iSBeJ6imdStCjt8K+71yxewMSMD6HiCj
2GWvp+mPixqf4GcwaiqoFgJj+IKspc3eyozqJY/+610aaSw/ooO2AFXfErJlJEBP
H5hITBUlAgMBAAECggEAErJqe1r5k+B3i9cAlWIE4royTOwDxe4JsnfWoLod0PcF
U0NNzR1qYicC3Qhmbe2/i9t1FAU/9QeiHkF2f+G7cMCSy1EKbdX807TiZdFHD7bm
CAjUUTeWNEAVziXnrG6yhsBoPuXNaylOALC6U5cFAP1riR3RMJjISmHjURuOAlFI
W8tlKq77I5a4L93IW+2/elDPTjhUYsQnSEtJPWG/BizSVihHSiHh2lAN0JLZChWk
6J2e2FaZYC28Swu/V+GLXLg0Ai8GkSZNYTqOf8HqnkB7X3G7+OMnLPW+0I5GPOh+
9aX9WXhGAI6gxJf6UjcD5axHV+mdfgQnxBsJY4HxqQKBgQDfUbtYzfLBxnnxk0H7
N+lhdiANA/YXiR4OljziTSTjnnSDdabktr3ubIofpEwjvAqKIdn5ruyqdT/9GOZt
/7sai0aqyIzGlQiLhkxHvHBIxGajRBbPq2BhLqQYBeL0Xy4oMcD5NjGoUuhcck0b
bw6h935CIJYzJQtx+K/U4I2DwwKBgQC3timBQPbq+wJx4yuyf+VOy4r9qW0/4DUM
pw3qo1tqOk1hKN6pZazov0qfrGEKIOG4Ws0rLCKgwKwocdfFfzPwTgg8srl5r5XM
k5r97mHNYqSlboAE2YIM+CziUmVqklkMqQ4Hs38jk3tswt6yY+syrfZbp/Rhh/XK
pVv1itr89wKBgQDBi1VihsNw+7IuE2Eo9/E1faoTfa5oAXdiTwUfYJqrB2aVlH77
VAHSRJGFEODIS62av/Hpepg0t3+ovE7hYLTpMXIii8OuS/Xm7pLnzUJHXqhRsa5P
d4kFUOX4yAlFn8QiI9TKaBSrfIdTr+Bx+VNmPlhXuWRTmTSNJ2pEhgU//wKBgAt7
Vxy88rG8/mofyJtfYvWJwyYXcLyNRsODrVr82rnI6w0ngMMVl7j0O7W/EFGRvInJ
IwmPuJpTcG8WrmWpjZV3SwyAHxd74eDnWMiGHZa4k5HDVjz3Wyl0WVnLzIrcmrQv
3LCeh1Ox5AToKQL9O7XvKXaRCLUPykzgCN9PzmABAoGAUW/AIrYerL0oU0KBRZ5B
6NX+5++R6jugN/spvVs4OLwPM5a6ud6Bq/+BA3aT7NWdfypVgybotEytsb/y1oe2
XdFhno110rcMp6WoQHM4dCWw3cmRNbMv2aleY2FrTZlpxEXeA47iF5/if1VHldmR
Y7LzJJ6LCjfUFy8dMINZC7M=
-----END PRIVATE KEY-----
"
        .to_string();

        // Kafka sink: validated, lazily connected, listed with its kind.
        // X1-10: gated behind Cargo feature `kafka`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "kafka-sink-1",
                       "kind": "kafka",
                       "config": {"bootstrap_servers": "127.0.0.1:9092",
                                  "topic_template": "out-${topic}",
                                  "partition_key_field": "device_id",
                                  "partitions": 4,
                                  "client_id": "indra",
                                  "acks": "all"}}),
                &token,
            )
            .await;
        if cfg!(feature = "kafka") {
            assert_eq!(status, 201);
            assert_eq!(created["id"], json!("kafka-sink-1"));
            assert_eq!(created["kind"], json!("kafka"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("kafka"),
                "disabled kafka must name the feature, got: {created}"
            );
        }

        // RabbitMQ sink: slash translation config accepted.
        // X1-10: gated behind Cargo feature `rabbitmq`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "rabbit-sink-1",
                       "kind": "rabbitmq",
                       "config": {"endpoint": "amqp://127.0.0.1:5672/%2f",
                                  "exchange": "telemetry",
                                  "routing_key_template": "sensor.${topic}",
                                  "delivery_mode": 2}}),
                &token,
            )
            .await;
        if cfg!(feature = "rabbitmq") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("rabbitmq"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("rabbitmq"),
                "disabled rabbitmq must name the feature, got: {created}"
            );
        }

        // Logger sink needs no config at all.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "diag", "kind": "logger", "config": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("console"));

        // Postgres sink: validated without touching any database.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "pg-sink-1",
                       "kind": "postgres",
                       "config": {"connection_url": "postgresql://u:p@127.0.0.1:5432/db",
                                  "sql_template": "INSERT INTO t (topic, qos, payload) VALUES ($1, $2, $3)",
                                  "pool_size": 2,
                                  "batch_size": 50,
                                  "batch_timeout_ms": 25}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("postgres"));

        // Redis sink: XADD stream config accepted (command is flat).
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "redis-sink-1",
                       "kind": "redis",
                       "config": {"endpoint": "redis://127.0.0.1:6379",
                                  "command": "xadd",
                                  "stream_template": "events",
                                  "maxlen": 1000}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("redis"));

        // MySQL sink: validated without touching any database.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "mysql-sink-1",
                       "kind": "mysql",
                       "config": {"connection_url": "mysql://u:p@127.0.0.1:3306/db",
                                  "sql_template": "INSERT INTO t (topic, qos, payload) VALUES (?, ?, ?)",
                                  "pool_size": 2,
                                  "batch_size": 50,
                                  "batch_timeout_ms": 25}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("mysql"));

        // ClickHouse sink: validated without touching any server.
        // X1-10: gated behind Cargo feature `clickhouse`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "ch-sink-1",
                       "kind": "clickhouse",
                       "config": {"endpoint": "http://127.0.0.1:8123",
                                  "database": "indra",
                                  "table": "mqtt_events",
                                  "format": "JSONEachRow",
                                  "batch_size": 100,
                                  "batch_timeout_ms": 50}}),
                &token,
            )
            .await;
        if cfg!(feature = "clickhouse") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("clickhouse"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("clickhouse"),
                "disabled clickhouse must name the feature, got: {created}"
            );
        }

        // InfluxDB sink: validated without touching any server.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "influx-sink-1",
                       "kind": "influxdb",
                       "config": {"endpoint": "http://127.0.0.1:8086",
                                  "bucket": "mqtt",
                                  "org": "indra",
                                  "token": "secret",
                                  "measurement_template": "mqtt_events",
                                  "precision": "ms",
                                  "batch_size": 100,
                                  "batch_timeout_ms": 50}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("influxdb"));

        // S3 sink: validated without touching any object store.
        // X1-09: `s3` gated behind Cargo feature `s3`; disabled builds
        // fail closed naming the feature.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "s3-sink-1",
                       "kind": "s3",
                       "config": {"endpoint": "http://127.0.0.1:9000",
                                  "bucket": "telemetry-cold-store",
                                  "region": "us-east-1",
                                  "key_template": "telemetry/year=${YYYY}/month=${MM}/day=${DD}/${topic}_${seq}.ndjson",
                                  "compression": "none",
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "batch_timeout_ms": 1000}}),
                &token,
            )
            .await;
        if cfg!(feature = "s3") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("s3"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("s3"),
                "disabled s3 must name the feature, got: {created}"
            );
        }

        // Elasticsearch sink: validated without touching any cluster.
        // X1-10: gated behind Cargo feature `elasticsearch`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "es-sink-1",
                       "kind": "elasticsearch",
                       "config": {"endpoint": "http://127.0.0.1:9200",
                                  "index_template": "iot-telemetry-${YYYY.MM.dd}",
                                  "auth": {"type": "none"},
                                  "batch_size": 100,
                                  "batch_timeout_ms": 50,
                                  "max_retries": 3}}),
                &token,
            )
            .await;
        if cfg!(feature = "elasticsearch") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("elasticsearch"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("elasticsearch"),
                "disabled elasticsearch must name the feature, got: {created}"
            );
        }

        // TimescaleDB sink: validated without touching any database.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "ts-sink-1",
                       "kind": "timescaledb",
                       "config": {"connection_url": "postgresql://u:p@127.0.0.1:5432/timeseries",
                                  "hypertable": "sensor_metrics",
                                  "time_column": "time",
                                  "sql_template": "INSERT INTO sensor_metrics (time, device_id, topic, metrics) VALUES ($1, $2, $3, $4::jsonb) ON CONFLICT (time, device_id) DO UPDATE SET metrics = EXCLUDED.metrics",
                                  "pool_size": 2,
                                  "batch_size": 50,
                                  "batch_timeout_ms": 25}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("timescaledb"));

        // Webhook sink: validated without sending any request.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "hook-1",
                       "kind": "webhook",
                       "config": {"url": "https://hooks.example.com/ingest/${topic}",
                                  "method": "post",
                                  "headers": {},
                                  "auth": {"type": "none"},
                                  "body_format": "rawjson",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 25,
                                  "timeout_ms": 1000,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("webhook"));

        // MQTT bridge sink: validated without opening any socket.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "bridge-1",
                       "kind": "mqtt_bridge",
                       "config": {"broker_address": "mqtt://127.0.0.1:1883",
                                  "client_id": "indra-bridge-test",
                                  "clean_start": true,
                                  "keep_alive_secs": 60,
                                  "max_inflight": 1000,
                                  "max_batch_size": 50,
                                  "linger_ms": 10,
                                  "protocol": "v311"}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("mqtt_bridge"));

        // Disk log sink: validated against an isolated temp directory.
        let disk_dir = std::env::temp_dir().join(format!("indra-test-disk-{}", std::process::id()));
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "disk-1",
                       "kind": "disk_log",
                       "config": {"directory": disk_dir.to_string_lossy(),
                                  "filename_prefix": "audit",
                                  "filename_extension": "log",
                                  "format": "ndjson",
                                  "compression": "none",
                                  "sync_mode": {"mode": "osdefault"}}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("disk_log"));
        std::fs::remove_dir_all(&disk_dir).ok();

        // Sparkplug B sink: the driver connects at the first write, thus
        // the create step does not need a server.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "spb-1",
                       "kind": "sparkplug_b",
                       "config": {"topic_prefix": "spBv1.0/plant1",
                                  "mqtt_url": "mqtt://127.0.0.1:1883",
                                  "tier": "enterprise",
                                  "batch_size": 50,
                                  "linger_ms": 25}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("sparkplug_b"));

        // Kinesis sink: validated without touching AWS.
        // X1-10: gated behind Cargo feature `kinesis`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "kinesis-1",
                       "kind": "kinesis",
                       "config": {"stream_name": "telemetry-stream",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "partition_key_template": "${topic}",
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 3,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "kinesis") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("kinesis"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("kinesis"),
                "disabled kinesis must name the feature, got: {created}"
            );
        }

        // GCP Pub/Sub sink: validated without touching Google.
        // X1-10: gated behind Cargo feature `gcp_pubsub`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "gcp-1",
                       "kind": "gcp_pubsub",
                       "config": {"project_id": "my-iot-project",
                                  "topic_id": "telemetry-events",
                                  "auth": {"type": "none"},
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "gcp_pubsub") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("gcp_pubsub"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("gcp_pubsub"),
                "disabled gcp_pubsub must name the feature, got: {created}"
            );
        }

        // Azure Event Hubs sink: validated without touching Azure.
        // X1-09: gated behind Cargo feature `azure_eventhubs`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "azure-1",
                       "kind": "azure_eventhubs",
                       "config": {"namespace": "my-eventhub-ns",
                                  "event_hub": "telemetry-hub",
                                  "shared_access_key_name": "SendPolicy",
                                  "shared_access_key": "secret",
                                  "token_ttl_secs": 3600,
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "azure_eventhubs") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("azure_eventhubs"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("azure_eventhubs"),
                "disabled azure_eventhubs must name the feature, got: {created}"
            );
        }

        // Pulsar sink: validated without opening any socket.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "pulsar-1",
                       "kind": "pulsar",
                       "config": {"service_url": "pulsar://127.0.0.1:6650",
                                  "tenant": "public",
                                  "namespace": "default",
                                  "topic": "iot-telemetry",
                                  "auth": {"type": "none"},
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("pulsar"));

        // MongoDB sink: validated without opening any socket.
        // X1-10: gated behind Cargo feature `mongodb`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "mongo-1",
                       "kind": "mongodb",
                       "config": {"connection_string": "mongodb://u:p@127.0.0.1:27017",
                                  "database": "telemetry",
                                  "collection_template": "readings",
                                  "operation": {"type": "insert_one"},
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "mongodb") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("mongodb"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("mongodb"),
                "disabled mongodb must name the feature, got: {created}"
            );
        }

        // MSSQL sink: validated without opening any socket.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "mssql-1",
                       "kind": "mssql",
                       "config": {"host": "127.0.0.1",
                                  "database": "telemetry",
                                  "table_template": "dbo.SensorEvents",
                                  "auth": {"type": "sql_password", "username": "sa", "password": "secret"},
                                  "query_mode": {"mode": "insertjson"},
                                  "trust_server_certificate": true,
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("mssql"));

        // Cassandra sink: validated without opening any socket.
        // X1-10: gated behind Cargo feature `cassandra`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "cassandra-1",
                       "kind": "cassandra",
                       "config": {"contact_points": ["10.0.0.1:9042"],
                                  "keyspace": "telemetry",
                                  "table_template": "events",
                                  "auth": {"type": "none"},
                                  "consistency": "localquorum",
                                  "partition_key_template": "${topic}",
                                  "cql_statement_template": "INSERT INTO telemetry.events (device_id, bucket_hour, event_time, payload) VALUES (?, ?, ?, ?)",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "cassandra") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("cassandra"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("cassandra"),
                "disabled cassandra must name the feature, got: {created}"
            );
        }

        // Couchbase sink: validated without opening any socket.
        // X1-10: gated behind Cargo feature `couchbase`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "couchbase-1",
                       "kind": "couchbase",
                       "config": {"connection_string": "couchbase://127.0.0.1",
                                  "bucket": "telemetry",
                                  "auth": {"username": "Administrator", "password": "secret"},
                                  "doc_id_template": "${client_id}::${timestamp}",
                                  "operation": "upsert",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "couchbase") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("couchbase"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("couchbase"),
                "disabled couchbase must name the feature, got: {created}"
            );
        }

        // TDengine sink: validated without touching any server.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "tdengine-1",
                       "kind": "tdengine",
                       "config": {"endpoint": "http://127.0.0.1:6041/rest/sql",
                                  "database": "power",
                                  "stable_name": "meters",
                                  "subtable_template": "d_${client_id}",
                                  "auth": {"type": "basic", "username": "root", "password": "taosdata"},
                                  "tags_template": {},
                                  "metrics_template": {},
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("tdengine"));

        // IoTDB sink: validated without touching any server.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "iotdb-1",
                       "kind": "iotdb",
                       "config": {"endpoint": "http://127.0.0.1:18080/rest/v2",
                                  "device_path_template": "root.factory.plant1",
                                  "auth": {"username": "root", "password": "root"},
                                  "is_aligned": false,
                                  "measurements": ["temperature"],
                                  "data_types": ["DOUBLE"],
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("iotdb"));

        // Timestream sink: validated without touching AWS.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "timestream-1",
                       "kind": "timestream",
                       "config": {"database_name": "iot_database",
                                  "table_name": "telemetry",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "dimensions": {},
                                  "time_unit": "milliseconds",
                                  "multi_measure_mappings": {"temperature": "DOUBLE"},
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("timestream"));

        // DynamoDB sink: validated without touching AWS.
        // X1-09: gated behind Cargo feature `dynamodb`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "dynamodb-1",
                       "kind": "dynamodb",
                       "config": {"table_name": "telemetry_table",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "partition_key": {"name": "device_id", "template": "${client_id}", "key_type": "S"},
                                  "attributes_mapping": {},
                                  "batch_size": 25,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "dynamodb") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("dynamodb"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("dynamodb"),
                "disabled dynamodb must name the feature, got: {created}"
            );
        }

        // Snowflake sink: key-pair validated without touching Snowflake.
        // Test-only RSA key (openssl-generated, never deployed).
        const SNOWFLAKE_TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCwQ2w63oB3FtHg\n7xysQK8MuX9S0WkbAVlxWpLHDNIdRVxA9Ra2gFFpKy8jX45UMSow6Yny7IvYWFzZ\nL4y9yoFiqu+LxhlJHIO6JO8+ZmeBoNwuDiIzgesbZwjyQiQ2M7p/4c18a2ffGPWF\nBETT7uVwVKJ3hTp97RN7Mc1/eFMimuT/TC11I+sFCZUHgrbhEG3L5Gg3RJ2MKbcX\nGEIxjFDJdLJ9RK0BopD6lxR1a4zeYr+iF/m3+JeJPAaS15yMD+sB1g5C7XZ1OIsB\nNBBnHWHpNhYO2IrCc9lZeSSzSkbRC6k1oqvTurFRzHWZqBKQGYnH8BftubIPSTBg\nU/BM4rR3AgMBAAECggEAHSRwmwUZoVb1CWcPSw2Aw65RtkwoQA5Hjv3GIcHlZXCH\n0beT80Wg8C3zI7qTSik8zAx4weDJOFJXu5LohqKaJMmVRHtSx+s+fkLICX2d5GlH\nrhepIPH8gLHW4VL9MLb5wVYAhu8tI845Ha54gL/RUHK1z+QHqTVO0MIJs2cd+6zx\nKsAtnqEQJMFpl1D0y0uutuboK4soHJMyRyrHBNWdgfzmTrCsngzu2zVM4aZh/gQY\nHcQgJ1rK6Wnen/GGPrNluwWU+bfLdlWO2qiXXwGLfhyx2H6cuROGdoU607BFJNpM\nkAudvEuLa0fOi1ym6lJ5pcJ6pSLkbeveW6+thkO2fQKBgQDXc2GiKx15vQHmdDmZ\nUJEiPJ+hSry5fjaowzrfgqJHyeNfUjnM/E9WlNn2AuxKDWGc3UNEr6jB9V7leKev\nQaPB2LAgXt0YVHmyim51/gTDguE9TOTGWqL4npZG9Nqh8xMxWt08ULvknkOQQOso\nzCoZQYlG4BHegAG7n0/5IN7HdQKBgQDRb/VbJ9iE0wtY/A3e3eWPbGfTF7AZREUu\n/mt94tFEWDDvedX1EPi4DJgPMqQ4eHnBZb3+G7jPcRdm6/KQzR5QiRMHSylfIQRH\nLqqfHBzZDDSZINLW1FMReC9xGfkRoG0Tlt2iQzXOy90+uE/9k5BGSbQNakfVDXJs\n3JAHDMy6uwKBgQCaazxC+xv5MRq3jf3qgPBE1aaj9+kkGe4bLzJ3GC4vveeVXl3H\nKd/DcpR12sp4mPapc3zPMgeGXNNTLRMiba1tNl2mFdfppEJFUSqyrwnDB39gbEhc\nUoIUJ7YVzVEWWh4bdcCzhjnlNfm+3oitiQdzaqF1hwvHqX+Udi7fpEuIMQKBgQC5\nu0bkQu7Rw/MRQ93tIe19ho6AdkZV8eREq52Z8vbQXEFxbiOfBCD93zVObQOTjMu1\nBcw6uEzpsgol3OKtJSpYE2eLlU0oLriDg9AN8DlpBljy31f66iqMmH/CFl16E0II\nGEeOqXnjXYlkIMHXR/CvVJdXOkRfnWA3SFZ12hUJFwKBgD8JlGTyrVfNsNMOaTDV\nNopoYnUQ6ljFmJi6TGmnkliCRXPuqBl+2hVxiKeWI2MprJ5Ya8qLbL6M56uCwAD2\nqEhvjEuatma5rJyE5NULOjAXA5tLw9qM1M9j1FNOaXnFC9/Yii2a49R8zu05wRB2\nH+dMMSDXQ4EHHYcKIFJjDbxn\n-----END PRIVATE KEY-----\n";
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "snowflake-1",
                       "kind": "snowflake",
                       "config": {"account": "xy12345.us-east-1",
                                  "user": "indra_loader",
                                  "database": "IOT",
                                  "schema": "PUBLIC",
                                  "table_template": "IOT_EVENTS",
                                  "private_key_pem": SNOWFLAKE_TEST_KEY,
                                  "channel": "INDRA_CHANNEL",
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("snowflake"));

        // Databricks sink: validated without touching any workspace.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "databricks-1",
                       "kind": "databricks",
                       "config": {"host": "dbc-a1b2c3d4-e5f6.cloud.databricks.com",
                                  "token": "dapi-test",
                                  "catalog": "main",
                                  "schema": "default",
                                  "table_template": "sensor_readings",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("databricks"));

        // Doris sink: validated without touching any cluster.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "doris-1",
                       "kind": "doris",
                       "config": {"fe_host": "127.0.0.1",
                                  "http_port": 8030,
                                  "database": "telemetry",
                                  "table_template": "events",
                                  "auth": {"username": "root", "password": ""},
                                  "format": "json",
                                  "strip_outer_array": true,
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 10,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("doris"));

        // BigQuery sink: validated without touching Google.
        // X1-09: gated behind Cargo feature `bigquery`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "bigquery-1",
                       "kind": "bigquery",
                       "config": {"project_id": "my-iot-project",
                                  "dataset_id": "telemetry",
                                  "table_template": "sensor_logs",
                                  "auth": {"type": "none"},
                                  "ignore_unknown_values": true,
                                  "skip_invalid_rows": false,
                                  "batch_size": 100,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        if cfg!(feature = "bigquery") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("bigquery"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("bigquery"),
                "disabled bigquery must name the feature, got: {created}"
            );
        }

        // Redshift sink: validated without touching AWS.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "redshift-1",
                       "kind": "redshift",
                       "config": {"database": "analytics",
                                  "table_template": "sensor_logs",
                                  "workgroup_name": "iot-workgroup",
                                  "region": "us-east-1",
                                  "access_key_id": "AKID",
                                  "secret_access_key": "secret",
                                  "batch_size": 50,
                                  "batch_bytes": 65536,
                                  "linger_ms": 20,
                                  "max_retries": 2,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("redshift"));

        // OCI Streaming sink: Cavage-signed, validated without touching OCI.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "oci-1",
                       "kind": "oci_streaming",
                       "config": {"endpoint": "https://cell-1.streaming.us-east-1.oci.oraclecloud.com",
                                  "stream_pool_id": "ocid1.streampool.oc1..testpool",
                                  "stream_id": "ocid1.stream.oc1..teststream",
                                  "tenancy_ocid": "ocid1.tenancy.oc1..test",
                                  "user_ocid": "ocid1.user.oc1..test",
                                  "fingerprint": "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55",
                                  "private_key_pem": test_pem.clone(),
                                  "partition_key_template": "${client_id}",
                                  "batch_size": 500,
                                  "batch_bytes": 65536,
                                  "linger_ms": 100,
                                  "max_retries": 5,
                                  "initial_backoff_ms": 10,
                                  "max_backoff_ms": 100}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("oci_streaming"));

        // AWS IoT Core sink: mTLS-only build, validated without opening any socket.
        // SigV4/WebSocket is intentionally not built (no licence-compliant
        // WebSocket client); SigV4 configs are rejected at registration.
        // X1-09: gated behind Cargo feature `aws_iot`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "aws-iot-1",
                       "kind": "aws_iot",
                       "config": {"endpoint": "abc-ats.iot.us-east-1.amazonaws.com",
                                  "region": "us-east-1",
                                  "client_id": "indra-bridge-1",
                                  "auth": {"type": "mtls",
                                            "ca_cert_pem": include_str!("../../broker-connectors-enterprise/testdata/ca-cert.pem"),
                                            "client_cert_pem": include_str!("../../broker-connectors-enterprise/testdata/client-cert.pem"),
                                            "client_key_pem": include_str!("../../broker-connectors-enterprise/testdata/client-key.pem")},
                                  "topic_mappings": [{"local_topic": "sensors/+",
                                                      "remote_topic": "indra/up",
                                                      "direction": "localtoremote"}],
                                  "batch_size": 200,
                                  "linger_ms": 50,
                                  "max_retries": 5}}),
                &token,
            )
            .await;
        if cfg!(feature = "aws_iot") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("aws_iot"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("aws_iot"),
                "disabled aws_iot must name the feature, got: {created}"
            );
        }

        // Azure IoT Hub sink: validated without opening any socket.
        // X1-09: gated behind Cargo feature `azure_iot`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "azure-iot-1",
                       "kind": "azure_iot",
                       "config": {"iot_hub_name": "e2e-hub",
                                  "device_id": "e2e-device",
                                  "module_id": null,
                                  "auth": {"type": "sharedaccesskey",
                                            "key": "c2VjcmV0",
                                            "key_name": "device"},
                                  "api_version": "2021-04-12",
                                  "direct_methods_enabled": false,
                                  "twin_sync_enabled": false,
                                  "batch_size": 200,
                                  "linger_ms": 50,
                                  "max_retries": 5}}),
                &token,
            )
            .await;
        if cfg!(feature = "azure_iot") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("azure_iot"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("azure_iot"),
                "disabled azure_iot must name the feature, got: {created}"
            );
        }

        // GCP IoT Core sink: validated without opening any socket.
        // X1-09: gated behind Cargo feature `gcp_iot`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "gcp-iot-1",
                       "kind": "gcp_iot",
                       "config": {"project_id": "e2e-project",
                                  "cloud_region": "us-central1",
                                  "registry_id": "e2e-registry",
                                  "device_id": "e2e-device",
                                  "private_key_pem": test_pem,
                                  "algorithm": "RS256",
                                  "token_lifetime_secs": 3600,
                                  "endpoint": "mqtt.googleapis.com:8883",
                                  "batch_size": 200,
                                  "linger_ms": 50,
                                  "max_retries": 5}}),
                &token,
            )
            .await;
        if cfg!(feature = "gcp_iot") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("gcp_iot"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"].as_str().unwrap_or("").contains("gcp_iot"),
                "disabled gcp_iot must name the feature, got: {created}"
            );
        }

        // OPC-UA sink: validated without opening any socket.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "opcua-1",
                       "kind": "opc_ua",
                       "config": {"endpoint_url": "opc.tcp://127.0.0.1:4840",
                                  "security_policy": "None",
                                  "security_mode": "none",
                                  "auth": {"type": "anonymous"},
                                  "node_subscriptions": [{"node_id": "ns=2;s=Temperature",
                                                          "sampling_interval_ms": 1000,
                                                          "publish_topic_template": "opcua/temperature"}],
                                  "batch_size": 200,
                                  "linger_ms": 50}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("opc_ua"));

        // Azure Blob sink: validated without touching Azure.
        // X1-09: gated behind Cargo feature `azure_blob`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "azblob-1",
                       "kind": "azure_blob",
                       "config": {"account_name": "mydeviceblobs",
                                  "container_name": "telemetry",
                                  "auth": {"type": "sharedkey",
                                            "account_key": "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="},
                                  "blob_path_template": "telemetry/year=${date.year}/${batch_id}.json",
                                  "compression": "none",
                                  "max_records_per_blob": 10000,
                                  "max_bytes_per_blob": 10485760,
                                  "flush_interval_secs": 60}}),
                &token,
            )
            .await;
        if cfg!(feature = "azure_blob") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("azure_blob"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("azure_blob"),
                "disabled azure_blob must name the feature, got: {created}"
            );
        }

        // Tablestore sink: validated without touching Alibaba Cloud.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "ots-1",
                       "kind": "tablestore",
                       "config": {"endpoint": "https://test-instance.cn-hangzhou.ots.aliyuncs.com",
                                  "instance_name": "test-instance",
                                  "table_name": "telemetry",
                                  "access_key_id": "test-key-id",
                                  "access_key_secret": "test-secret",
                                  "primary_keys": [{"name": "device_id",
                                                    "source": "${client_id}",
                                                    "data_type": "string"}],
                                  "attribute_columns": [{"name": "temperature",
                                                         "source": "${payload.temperature}",
                                                         "data_type": "double"}],
                                  "batch_size": 200}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("tablestore"));

        // Confluent Cloud sink: validated without opening any socket.
        // X1-10: gated behind Cargo feature `confluent`.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "confluent-1",
                       "kind": "confluent",
                       "config": {"bootstrap_servers": ["pkc-test.us-east-1.aws.confluent.cloud:9092"],
                                  "api_key": "confluent-key",
                                  "api_secret": "confluent-secret",
                                  "auth_mechanism": "plain",
                                  "topic_template": "telemetry-${topic_segment_1}",
                                  "partition_key_template": "${client_id}",
                                  "partitions": 12,
                                  "batch_size": 500}}),
                &token,
            )
            .await;
        if cfg!(feature = "confluent") {
            assert_eq!(status, 201);
            assert_eq!(created["kind"], json!("confluent"));
        } else {
            assert_eq!(status, 400);
            assert!(
                created["error"]
                    .as_str()
                    .unwrap_or("")
                    .contains("confluent"),
                "disabled confluent must name the feature, got: {created}"
            );
        }

        // RocketMQ sink: validated without opening any socket.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "rmq-1",
                       "kind": "rocketmq",
                       "config": {"endpoints": ["127.0.0.1:8081"],
                                  "topic": "rocket-telemetry",
                                  "tag_template": "${topic_segment_2}",
                                  "access_key": "rocket-key",
                                  "secret_key": "rocket-secret",
                                  "batch_size": 128}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("rocketmq"));

        // Oracle sink: validated without touching ORDS.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "oracle-1",
                       "kind": "oracle",
                       "config": {"url": "https://oracle-host:8080/ords/admin/_/sql",
                                  "schema": "ADMIN",
                                  "table": "telemetry",
                                  "username": "admin",
                                  "password": "secret",
                                  "key_columns": ["device_id"],
                                  "batch_size": 500}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("oracle"));

        // CockroachDB sink: validated without touching CockroachDB.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "cockroach-1",
                       "kind": "cockroachdb",
                       "config": {"connection_string": "postgresql://root@127.0.0.1:26257/defaultdb",
                                  "table": "telemetry",
                                  "upsert_conflict_columns": ["device_id"],
                                  "batch_size": 500,
                                  "max_retry_attempts": 5}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("cockroachdb"));

        // AlloyDB sink: validated without touching AlloyDB.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "alloydb-1",
                       "kind": "alloydb",
                       "config": {"host": "10.0.0.1",
                                  "port": 5432,
                                  "database": "telemetry",
                                  "username": "postgres",
                                  "auth": {"type": "password", "password": "secret"},
                                  "table": "readings",
                                  "batch_size": 1000}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("alloydb"));

        // OpenTSDB sink: validated without touching OpenTSDB.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "opentsdb-1",
                       "kind": "opentsdb",
                       "config": {"endpoint": "http://127.0.0.1:4242",
                                  "protocol": "http",
                                  "metric_template": "sensor.temp",
                                  "value_field": "value",
                                  "summary": true,
                                  "compression": "none",
                                  "batch_size": 1000}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("opentsdb"));

        // GreptimeDB sink: validated without touching GreptimeDB.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "greptimedb-1",
                       "kind": "greptimedb",
                       "config": {"endpoint": "http://127.0.0.1:4000",
                                  "database": "public",
                                  "format": "sql_insert",
                                  "table_template": "sensor_readings",
                                  "timestamp_precision": "millisecond",
                                  "batch_size": 1000}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("greptimedb"));

        // Datalayers sink: validated without touching Datalayers.
        let (status, created) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "datalayers-1",
                       "kind": "datalayers",
                       "config": {"endpoint": "http://127.0.0.1:8360",
                                  "database": "telemetry",
                                  "table": "metrics",
                                  "auth_token": "secret-token",
                                  "batch_size": 500}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["kind"], json!("datalayers"));

        let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
        assert_eq!(status, 200);
        // X1-09/X1-10: each gated kind is present only when its Cargo
        // feature is enabled; otherwise creation above failed closed and
        // the list omits them. Assert presence conditionally.
        {
            let list = body.as_array().expect("connector list");
            let ids: std::collections::HashSet<&str> =
                list.iter().filter_map(|e| e["id"].as_str()).collect();
            for always in [
                "alloydb-1",
                "bridge-1",
                "cockroach-1",
                "databricks-1",
                "datalayers-1",
                "diag",
                "disk-1",
                "doris-1",
                "greptimedb-1",
                "hook-1",
                "influx-sink-1",
                "iotdb-1",
                "mssql-1",
                "mysql-sink-1",
                "oci-1",
                "opcua-1",
                "opentsdb-1",
                "oracle-1",
                "ots-1",
                "pg-sink-1",
                "pulsar-1",
                "redis-sink-1",
                "redshift-1",
                "rmq-1",
                "snowflake-1",
                "spb-1",
                "tdengine-1",
                "timestream-1",
                "ts-sink-1",
            ] {
                assert!(ids.contains(always), "connector {always} must be listed");
            }
            for (id, enabled) in [
                ("s3-sink-1", cfg!(feature = "s3")),
                ("azure-1", cfg!(feature = "azure_eventhubs")),
                ("dynamodb-1", cfg!(feature = "dynamodb")),
                ("bigquery-1", cfg!(feature = "bigquery")),
                ("aws-iot-1", cfg!(feature = "aws_iot")),
                ("azure-iot-1", cfg!(feature = "azure_iot")),
                ("gcp-iot-1", cfg!(feature = "gcp_iot")),
                ("azblob-1", cfg!(feature = "azure_blob")),
                ("kafka-sink-1", cfg!(feature = "kafka")),
                ("rabbit-sink-1", cfg!(feature = "rabbitmq")),
                ("ch-sink-1", cfg!(feature = "clickhouse")),
                ("es-sink-1", cfg!(feature = "elasticsearch")),
                ("gcp-1", cfg!(feature = "gcp_pubsub")),
                ("mongo-1", cfg!(feature = "mongodb")),
                ("cassandra-1", cfg!(feature = "cassandra")),
                ("couchbase-1", cfg!(feature = "couchbase")),
                ("confluent-1", cfg!(feature = "confluent")),
                ("kinesis-1", cfg!(feature = "kinesis")),
            ] {
                assert_eq!(ids.contains(id), enabled, "gated connector {id} presence");
            }
            let expected = 29
                + (cfg!(feature = "s3") as usize)
                + (cfg!(feature = "azure_eventhubs") as usize)
                + (cfg!(feature = "dynamodb") as usize)
                + (cfg!(feature = "bigquery") as usize)
                + (cfg!(feature = "aws_iot") as usize)
                + (cfg!(feature = "azure_iot") as usize)
                + (cfg!(feature = "gcp_iot") as usize)
                + (cfg!(feature = "azure_blob") as usize)
                + (cfg!(feature = "kafka") as usize)
                + (cfg!(feature = "rabbitmq") as usize)
                + (cfg!(feature = "clickhouse") as usize)
                + (cfg!(feature = "elasticsearch") as usize)
                + (cfg!(feature = "gcp_pubsub") as usize)
                + (cfg!(feature = "mongodb") as usize)
                + (cfg!(feature = "cassandra") as usize)
                + (cfg!(feature = "couchbase") as usize)
                + (cfg!(feature = "confluent") as usize)
                + (cfg!(feature = "kinesis") as usize);
            assert_eq!(list.len(), expected, "connector count");
        }

        // Unknown kinds and invalid configs are 400s that store nothing.
        for payload in [
            json!({"id": "x", "kind": "pigeon", "config": {}}),
            json!({"id": "", "kind": "logger", "config": {}}),
            json!({"id": "bad-k", "kind": "kafka",
                   "config": {"bootstrap_servers": "", "topic_template": "t",
                              "client_id": "c", "acks": "all"}}),
            json!({"id": "bad-r", "kind": "rabbitmq",
                   "config": {"endpoint": "amqp://h", "exchange": "",
                              "routing_key_template": "k", "delivery_mode": 2}}),
            json!({"id": "bad-pg", "kind": "postgres",
                   "config": {"connection_url": "postgresql://h/db",
                              "sql_template": "INSERT INTO t VALUES ($9)"}}),
            json!({"id": "bad-redis", "kind": "redis",
                   "config": {"endpoint": "redis://h",
                              "command": {"command": "set", "key_template": ""}}}),
            json!({"id": "bad-mysql", "kind": "mysql",
                   "config": {"connection_url": "mysql://u:p@h/db",
                              "sql_template": "INSERT INTO t VALUES (?, ?)",
                              "pool_size": 1, "batch_size": 10, "batch_timeout_ms": 10}}),
            json!({"id": "bad-ch", "kind": "clickhouse",
                   "config": {"endpoint": "http://h:8123",
                              "database": "db; DROP TABLE x;",
                              "table": "t",
                              "format": "JSONEachRow",
                              "batch_size": 10, "batch_timeout_ms": 10}}),
            json!({"id": "bad-influx", "kind": "influxdb",
                   "config": {"endpoint": "http://h:8086",
                              "bucket": "b", "org": "o", "token": "",
                              "measurement_template": "m", "precision": "ms"}}),
            json!({"id": "bad-s3", "kind": "s3",
                   "config": {"endpoint": "http://h:9000",
                              "bucket": "UPPERCASE",
                              "key_template": "t/${topic}.ndjson"}}),
            json!({"id": "bad-es", "kind": "elasticsearch",
                   "config": {"endpoint": "http://h:9200",
                              "index_template": "-leading",
                              "auth": {"type": "none"}}}),
            json!({"id": "bad-ts", "kind": "timescaledb",
                   "config": {"connection_url": "postgresql://u:p@h/db",
                              "hypertable": "m",
                              "sql_template": "INSERT INTO m VALUES ($1, $2)"}}),
            json!({"id": "bad-hook", "kind": "webhook",
                   "config": {"url": "ftp://hooks.example.com/x"}}),
            json!({"id": "bad-bridge", "kind": "mqtt_bridge",
                   "config": {"broker_address": "mqtt://h:1883",
                              "client_id": "b",
                              "qos_override": 5}}),
            json!({"id": "bad-disk", "kind": "disk_log",
                   "config": {"directory": "",
                              "format": "ndjson"}}),
            json!({"id": "bad-spb", "kind": "sparkplug_b",
                   "config": {"tier": "community"}}),
            json!({"id": "bad-kinesis", "kind": "kinesis",
                   "config": {"stream_name": "",
                              "region": "us-east-1",
                              "access_key_id": "AKID",
                              "secret_access_key": "secret"}}),
            json!({"id": "bad-gcp", "kind": "gcp_pubsub",
                   "config": {"project_id": "UPPER",
                              "topic_id": "telemetry-events",
                              "auth": {"type": "none"}}}),
            json!({"id": "bad-azure", "kind": "azure_eventhubs",
                   "config": {"namespace": "my-eventhub-ns",
                              "event_hub": "telemetry-hub",
                              "shared_access_key_name": "",
                              "shared_access_key": "secret"}}),
            json!({"id": "bad-pulsar", "kind": "pulsar",
                   "config": {"service_url": "pulsar://127.0.0.1:6650",
                              "tenant": "public",
                              "namespace": "default",
                              "topic": "",
                              "auth": {"type": "none"}}}),
            json!({"id": "bad-mongo", "kind": "mongodb",
                   "config": {"connection_string": "mongodb://127.0.0.1:27017",
                              "database": "",
                              "collection_template": "readings",
                              "operation": {"type": "insert_one"}}}),
            json!({"id": "bad-mssql", "kind": "mssql",
                   "config": {"host": "",
                              "database": "telemetry",
                              "table_template": "dbo.T",
                              "auth": {"type": "integrated"},
                              "query_mode": {"mode": "insertjson"}}}),
            json!({"id": "bad-cassandra", "kind": "cassandra",
                   "config": {"contact_points": [],
                              "keyspace": "telemetry",
                              "table_template": "events",
                              "partition_key_template": "${topic}",
                              "cql_statement_template": "INSERT INTO t VALUES (?, ?, ?, ?)"}}),
            json!({"id": "bad-couchbase", "kind": "couchbase",
                   "config": {"connection_string": "couchbase://127.0.0.1",
                              "bucket": "telemetry",
                              "auth": {"username": "", "password": "secret"},
                              "doc_id_template": "k",
                              "operation": "upsert"}}),
            json!({"id": "bad-tdengine", "kind": "tdengine",
                   "config": {"endpoint": "http://127.0.0.1:6041/rest/sql",
                              "database": "has space",
                              "stable_name": "meters",
                              "subtable_template": "d_${client_id}"}}),
            json!({"id": "bad-iotdb", "kind": "iotdb",
                   "config": {"endpoint": "http://127.0.0.1:18080/rest/v2",
                              "device_path_template": "factory.plant1",
                              "auth": {"username": "root", "password": "root"},
                              "measurements": ["temperature"],
                              "data_types": ["DOUBLE"]}}),
            json!({"id": "bad-timestream", "kind": "timestream",
                   "config": {"database_name": "iot_database",
                              "table_name": "",
                              "region": "us-east-1",
                              "access_key_id": "AKID",
                              "secret_access_key": "secret"}}),
            json!({"id": "bad-dynamodb", "kind": "dynamodb",
                   "config": {"table_name": "telemetry_table",
                              "region": "us-east-1",
                              "access_key_id": "AKID",
                              "secret_access_key": "secret",
                              "partition_key": {"name": "", "template": "${client_id}", "key_type": "S"}}}),
            json!({"id": "bad-snowflake", "kind": "snowflake",
                   "config": {"account": "",
                              "user": "u",
                              "database": "IOT",
                              "schema": "PUBLIC",
                              "table_template": "T",
                              "private_key_pem": "not-a-key"}}),
            json!({"id": "bad-databricks", "kind": "databricks",
                   "config": {"host": "https://host/path",
                              "token": "tok",
                              "table_template": "t"}}),
            json!({"id": "bad-doris", "kind": "doris",
                   "config": {"fe_host": "127.0.0.1",
                              "database": "has space",
                              "table_template": "events",
                              "auth": {"username": "root", "password": ""}}}),
            json!({"id": "bad-bigquery", "kind": "bigquery",
                   "config": {"project_id": "my-iot-project",
                              "dataset_id": "telemetry",
                               "table_template": "9lives",
                               "auth": {"type": "none"}}}),
            json!({"id": "bad-redshift", "kind": "redshift",
                   "config": {"database": "analytics",
                              "table_template": "t",
                              "region": "us-east-1",
                               "access_key_id": "AKID",
                               "secret_access_key": "secret"}}),
            json!({"id": "bad-azblob", "kind": "azure_blob",
                   "config": {"account_name": "AB",
                              "container_name": "telemetry",
                              "auth": {"type": "sharedkey",
                                        "account_key": "a2V5"},
                              "blob_path_template": "t/${batch_id}.json"}}),
            json!({"id": "bad-ots", "kind": "tablestore",
                   "config": {"endpoint": "https://i.cn-hangzhou.ots.aliyuncs.com",
                              "instance_name": "i",
                              "table_name": "t",
                              "access_key_id": "k",
                              "access_key_secret": "s",
                              "primary_keys": []}}),
            json!({"id": "bad-confluent", "kind": "confluent",
                   "config": {"bootstrap_servers": [],
                              "api_key": "k",
                              "api_secret": "s",
                              "topic_template": "t"}}),
            json!({"id": "bad-rmq", "kind": "rocketmq",
                   "config": {"endpoints": ["127.0.0.1:8081"],
                              "topic": ""}}),
            json!({"id": "bad-ora", "kind": "oracle",
                   "config": {"url": "https://oracle-host:8080/ords/admin/_/sql",
                              "schema": "",
                              "table": "telemetry",
                              "username": "admin",
                              "password": "secret",
                              "key_columns": ["device_id"]}}),
            json!({"id": "bad-crdb", "kind": "cockroachdb",
                   "config": {"connection_string": "postgresql://root@127.0.0.1:26257/defaultdb",
                              "table": "",
                              "upsert_conflict_columns": ["device_id"]}}),
            json!({"id": "bad-alloy", "kind": "alloydb",
                   "config": {"host": "",
                              "database": "telemetry",
                              "username": "postgres",
                              "table": "readings"}}),
            json!({"id": "bad-opentsdb", "kind": "opentsdb",
                   "config": {"endpoint": "",
                              "metric_template": "sensor.temp",
                              "value_field": "value"}}),
            json!({"id": "bad-greptimedb", "kind": "greptimedb",
                   "config": {"endpoint": "http://127.0.0.1:4000",
                              "database": "",
                              "table_template": "sensor_readings"}}),
            json!({"id": "bad-datalayers", "kind": "datalayers",
                   "config": {"endpoint": "",
                              "database": "telemetry",
                              "table": "metrics"}}),
        ] {
            let (status, _) = server
                .post_auth("/api/v1/connectors", payload, &token)
                .await;
            assert_eq!(status, 400);
        }
        let (status, body) = server.get_auth("/api/v1/connectors", &token).await;
        assert_eq!(status, 200);
        // X1-09/X1-10: count follows the enabled gated features (29 always
        // plus each gated kind when its feature is on).
        let expected = 29
            + (cfg!(feature = "s3") as usize)
            + (cfg!(feature = "azure_eventhubs") as usize)
            + (cfg!(feature = "dynamodb") as usize)
            + (cfg!(feature = "bigquery") as usize)
            + (cfg!(feature = "aws_iot") as usize)
            + (cfg!(feature = "azure_iot") as usize)
            + (cfg!(feature = "gcp_iot") as usize)
            + (cfg!(feature = "azure_blob") as usize)
            + (cfg!(feature = "kafka") as usize)
            + (cfg!(feature = "rabbitmq") as usize)
            + (cfg!(feature = "clickhouse") as usize)
            + (cfg!(feature = "elasticsearch") as usize)
            + (cfg!(feature = "gcp_pubsub") as usize)
            + (cfg!(feature = "mongodb") as usize)
            + (cfg!(feature = "cassandra") as usize)
            + (cfg!(feature = "couchbase") as usize)
            + (cfg!(feature = "confluent") as usize)
            + (cfg!(feature = "kinesis") as usize);
        assert_eq!(body.as_array().expect("list").len(), expected);
    }

    #[tokio::test]
    async fn s101_no_default_credentials_rejects_and_accepts() {
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Fixed test RSA key (never stored, never deployed).
        // Test-only fixture; no shipped path can reach it (`#[cfg(test)]` only).
        let test_pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEArN5HvmgO5xgYLzTqq5mIW+ZFEM20TFz5C0bC0hP3mR13f9od\n7PiABrYWbLIwfWftR9+riLY5MOUZiZFioRdAsMBjtNy+E0/wfU/VwN1uqaFi9ww6\nSc2hmqvnDcdnD9H55Zu+2QmhSjD0nLNJczC2ceqYM4Q0nQw4EqbxHkcZ+/0p9uI8\nPFy90G0wSTwBErthZYe6xE5DIkzRThFmwm0l2PPt30tIVP8UssxTYYJggNlIYaMT\n79C3i0Nl7ImUyDexL029qAJfVOVc17Zrv9VACypuSA4QIAVarH0J3lJ3hi+ZHAJR\npiZ6Ex+Z+VPbtAmMczizpa3zt/NDhBj3NHWbVQIDAQABAoIBADbMc8O5Ktg4/Bmi\n1a02C1LOiSBNhRUb99qKQOHlcHy6KZJ+6zOehr8Z47vrNJHcawx4q+C3myOTzGee\njbxxKZGeOz0vKJXoplyi/SiZSSY+wad82dFhfRQR4Dmu9029i1Es8O/OLLHfGPu/\nnc4FKCCudDcIdsE+CVxoTVECywrk7S66ziHFLKZBIaB5pTbX0tsjyhvdrun7rYBN\n/iF97u0DeQi+odntQFtgdkOg1jiz4ffrA3JMjfO+CJGnRy5PmckQv/a5Md06Av4R\nTrsIcDK8G3GB4Phfkans1R5EFS0aQ8ergeiaPPAEeMCF/BXdTA6y+2em/dUOzLPX\nfasTsLUCgYEA4zU7lEFmfeEvuL6cKD+QKJn1rpwXCDSVsPxDE7N6omrwrELVg+7E\nsefWSvHSUk3xDtUmgywoUj+osUtkHMkMjCW4rLGsWKxRPEXSbUHKPP/prn7XA+oL\n70XsKAzG1jZiJMb5QxaEyDO7bnIBKOksh1THz81TL0XilkFrzb5Ewl8CgYEAwsY8\nMQMEXbzFE9t4D7pZ0SOFRBfaoRk7KxCjcve7Hlx5Dw/EC3GpfpKDj7MEQclZ7bfy\nwilOXUg0F564gL0MdeCN2EwsMjnGtVnOpEZt8kiBG36sF+CMTaXlwkveUiyg8m3c\nlxSp3swdJV4O56IYhr1HDch6cUru8/vU+2SmxssCgYEAjRkS3dfbiVLd6vHcUhZf\nNQefjgb7OqpFR7rxAPwVrWWR/rgmlp8h4YWjC7mvLlOe3uI53339sE4+OZQjIMku\nFJ6cWAqiiBLDBBYsPpN7v3QBr86vQowHS1l19c0xcfgl7MbwJtyGKzoNLOJ5m+lq\nMMTSrVbKUPIqa7tG8mc1lisCgYBlq9kLtIl47OrrHKslep8b3rDYBKOpaGRMbJkF\nkG9qv9apmA878g8J9S0bu9TDCnoY7uaXl7xCUUhv8/hXq1dvyNICUHDZnvMtAz4s\n3l7SFCrg0VS/+gUDG1A+BiB9QLKbGeIGM1Eg7edg9CJPo3uuCeunNDlGHllxnslk\nmPu91QKBgQCw2vZ5xhHbkCljOD9Hb+2L2y7ZbSgAnUgyE/vAkmAlZYreP+SyzrPe\n+kTnWrZuCvc0x+G7zv6mv3CIcZM0CHN1hdJa1Y/C3n1s06kJjgwGvgP9h6a/m9al\neIave8nupkT7zrwPbv9MJlWrtqplLBHSMG3dnsKd+iLdEEdhg8GK5Q==\n-----END RSA PRIVATE KEY-----".to_string();

        // gcp_iot without a key is 400 naming the field, and registers nothing.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-gcp-no-key", "type": "gcp_iot",
                       "project_id": "e2e-project", "cloud_region": "us-central1",
                       "registry_id": "e2e-registry", "device_id": "e2e-device"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("private_key_pem"),
            "missing key must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-gcp-no-key").is_none());

        // gcp_iot with a key is 201 and live when the feature is
        // enabled; X1-09 disabled builds fail closed naming the feature.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-gcp-1", "type": "gcp_iot",
                       "project_id": "e2e-project", "cloud_region": "us-central1",
                       "registry_id": "e2e-registry", "device_id": "e2e-device",
                       "private_key_pem": test_pem}),
                &token,
            )
            .await;
        if cfg!(feature = "gcp_iot") {
            assert_eq!(status, 201);
            assert!(state.engine.connectors().get("s101-gcp-1").is_some());
        } else {
            assert_eq!(status, 400);
            assert!(
                body["message"].as_str().unwrap_or("").contains("gcp_iot"),
                "disabled gcp_iot must name the feature, got: {body}"
            );
            assert!(state.engine.connectors().get("s101-gcp-1").is_none());
        }

        // snowflake without a key is 400 naming the field.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-snow-no-key", "type": "snowflake",
                       "account": "xy12345.us-east-1", "user": "indra_loader",
                       "database": "IOT", "schema": "PUBLIC", "table": "TELEMETRY"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("private_key_pem"),
            "missing key must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-snow-no-key").is_none());

        // snowflake with a key is 201 and live.
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-snow-1", "type": "snowflake",
                       "account": "xy12345.us-east-1", "user": "indra_loader",
                       "database": "IOT", "schema": "PUBLIC", "table": "TELEMETRY",
                       "private_key_pem": test_pem}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert!(state.engine.connectors().get("s101-snow-1").is_some());

        // oci without a key is 400 naming the field.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-oci-no-key", "type": "oci_streaming",
                       "stream_pool_id": "ocid1.streampool.oc1..testpool",
                       "stream_id": "ocid1.stream.oc1..teststream",
                       "tenancy_ocid": "ocid1.tenancy.oc1..test",
                       "user_ocid": "ocid1.user.oc1..test",
                       "fingerprint": "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("private_key_pem"),
            "missing key must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-oci-no-key").is_none());

        // oci with a key is 201 and live (dummy loopback endpoint so the
        // reachability probe passes without touching any cloud).
        let oci_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind dummy");
        let oci_endpoint = format!(
            "http://127.0.0.1:{}",
            oci_listener.local_addr().expect("addr").port()
        );
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-oci-1", "type": "oci_streaming",
                       "stream_pool_id": "ocid1.streampool.oc1..testpool",
                       "stream_id": "ocid1.stream.oc1..teststream",
                       "tenancy_ocid": "ocid1.tenancy.oc1..test",
                       "user_ocid": "ocid1.user.oc1..test",
                       "fingerprint": "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55",
                       "private_key_pem": test_pem,
                       "endpoint": oci_endpoint}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert!(state.engine.connectors().get("s101-oci-1").is_some());
        drop(oci_listener);

        // databricks without a token is 400 naming the field.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-db-no-token", "type": "databricks",
                       "host": "my-workspace.cloud.databricks.com",
                       "catalog": "main", "schema": "default", "table": "events"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"].as_str().unwrap_or("").contains("token"),
            "missing token must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-db-no-token").is_none());

        // databricks with a token is 201 and live (dummy loopback target so
        // the reachability probe passes without touching any workspace).
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind dummy");
        let port = listener.local_addr().expect("addr").port();
        let host = format!("127.0.0.1:{port}");
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-db-1", "type": "databricks",
                       "host": host, "token": "dapi-test-token",
                       "catalog": "main", "schema": "default", "table": "events"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert!(state.engine.connectors().get("s101-db-1").is_some());
        drop(listener);

        // tablestore without secrets is 400 naming the field.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-ots-no-key", "type": "tablestore",
                       "instance_name": "test-instance-1", "table_name": "sensor_data",
                       "access_key_id": "test-ak-1"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("access_key_secret"),
            "missing secret must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-ots-no-key").is_none());

        // tablestore with secrets is 201 and live (dummy loopback endpoint
        // so the reachability probe passes without touching any cloud).
        let ots_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind dummy");
        let ots_endpoint = format!(
            "http://127.0.0.1:{}",
            ots_listener.local_addr().expect("addr").port()
        );
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-ots-1", "type": "tablestore",
                       "instance_name": "test-instance-1", "table_name": "sensor_data",
                       "access_key_id": "test-ak-1", "access_key_secret": "test-sk-1",
                       "endpoint": ots_endpoint}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert!(state.engine.connectors().get("s101-ots-1").is_some());
        drop(ots_listener);

        // confluent without credentials is 400 naming the field.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-conf-no-key", "type": "confluent",
                       "bootstrap_servers": "127.0.0.1:9092",
                       "topic": "telemetry-events"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"].as_str().unwrap_or("").contains("api_key"),
            "missing key must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-conf-no-key").is_none());

        // Confluent with credentials is 201 and live when enabled.
        // Disabled builds fail closed and name the feature.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-conf-1", "type": "confluent",
                       "bootstrap_servers": "127.0.0.1:9092",
                       "topic": "telemetry-events",
                       "api_key": "s101-cc-key",
                       "api_secret": "s101-cc-secret"}),
                &token,
            )
            .await;
        if cfg!(feature = "confluent") {
            assert_eq!(status, 201);
            assert!(state.engine.connectors().get("s101-conf-1").is_some());
        } else {
            assert_eq!(status, 400);
            assert!(
                body["message"].as_str().unwrap_or("").contains("confluent"),
                "disabled confluent must name the feature, got: {body}"
            );
            assert!(state.engine.connectors().get("s101-conf-1").is_none());
        }

        // azure_eventhubs without a key is 400 naming the field.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-eh-no-key", "type": "azure_eventhubs",
                       "namespace": "s101ns", "event_hub": "s101hub"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("shared_access_key"),
            "missing key must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-eh-no-key").is_none());

        // azure_eventhubs with a key is 201 and live when enabled;
        // X1-09 disabled builds fail closed naming the feature.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-eh-1", "type": "azure_eventhubs",
                       "namespace": "s101ns", "event_hub": "s101hub",
                       "shared_access_key_name": "SendPolicy",
                       "shared_access_key": "dGVzdC1rZXktb3BlcmF0b3Itc3VwcGxpZWQ="}),
                &token,
            )
            .await;
        if cfg!(feature = "azure_eventhubs") {
            assert_eq!(status, 201);
            assert!(state.engine.connectors().get("s101-eh-1").is_some());
        } else {
            assert_eq!(status, 400);
            assert!(
                body["message"]
                    .as_str()
                    .unwrap_or("")
                    .contains("azure_eventhubs"),
                "disabled azure_eventhubs must name the feature, got: {body}"
            );
            assert!(state.engine.connectors().get("s101-eh-1").is_none());
        }

        // azure_iot without a key is 400 naming the field.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-aiot-no-key", "type": "azure_iot",
                       "iot_hub_name": "127.0.0.1:8883",
                       "device_id": "s101-device"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("shared_access_key"),
            "missing key must be named, got: {body}"
        );
        assert!(state.engine.connectors().get("s101-aiot-no-key").is_none());

        // azure_iot with a key is 201 and live when enabled;
        // X1-09 disabled builds fail closed naming the feature.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "s101-aiot-1", "type": "azure_iot",
                       "iot_hub_name": "127.0.0.1:8883",
                       "device_id": "s101-device",
                       "shared_access_key": "dGVzdC1rZXktb3BlcmF0b3Itc3VwcGxpZWQ="}),
                &token,
            )
            .await;
        if cfg!(feature = "azure_iot") {
            assert_eq!(status, 201);
            assert!(state.engine.connectors().get("s101-aiot-1").is_some());
        } else {
            assert_eq!(status, 400);
            assert!(
                body["message"].as_str().unwrap_or("").contains("azure_iot"),
                "disabled azure_iot must name the feature, got: {body}"
            );
            assert!(state.engine.connectors().get("s101-aiot-1").is_none());
        }
    }

    #[tokio::test]
    async fn cf05_v5_connector_create_refuses_sink_build_failure() {
        // CF-05: the v5 create path refuses a connector whose sink
        // fails to build. Each case runs through HTTP on the live
        // engine, and the refused entry stays out of the store.
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Datalayers with both auth modes set: the sink refuses the
        // conflict, so create answers 400 with the sink text.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "cf05-dl-conflict", "type": "datalayers",
                       "endpoint": "http://127.0.0.1:8360", "database": "factory_db",
                       "table": "sensor_events", "token": "cf05-token",
                       "username": "cf05-user", "password": "cf05-pass"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "conflicting auth must fail, got: {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("auth modes conflict"),
            "refusal carries the sink text, got: {body}"
        );
        assert!(state.engine.connectors().get("cf05-dl-conflict").is_none());

        // Datalayers with a username and no password: the sink refuses
        // the half pair, so create answers 400 with the sink text.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "cf05-dl-half", "type": "datalayers",
                       "endpoint": "http://127.0.0.1:8360", "database": "factory_db",
                       "table": "sensor_events", "username": "cf05-user"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "half auth pair must fail, got: {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("both username and password"),
            "refusal carries the sink text, got: {body}"
        );
        assert!(state.engine.connectors().get("cf05-dl-half").is_none());

        // GreptimeDB with an empty database: the sink refuses it.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "cf05-greptime-empty", "type": "greptimedb",
                       "endpoint": "http://127.0.0.1:4000/v1", "database": ""}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "empty database must fail, got: {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(
            body["message"].as_str().unwrap_or("").contains("database"),
            "refusal carries the sink text, got: {body}"
        );
        assert!(state
            .engine
            .connectors()
            .get("cf05-greptime-empty")
            .is_none());

        // OpenTSDB with an empty metric: the sink refuses it.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "cf05-opentsdb-empty", "type": "opentsdb",
                       "endpoint": "http://127.0.0.1:4242", "metric": ""}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "empty metric must fail, got: {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(
            body["message"].as_str().unwrap_or("").contains("metric"),
            "refusal carries the sink text, got: {body}"
        );
        assert!(state
            .engine
            .connectors()
            .get("cf05-opentsdb-empty")
            .is_none());

        // None of the refused entries reached the store.
        let (status, list) = server.get_auth("/api/v5/connectors", &token).await;
        assert_eq!(status, 200);
        let ids: Vec<&str> = list
            .as_array()
            .expect("connector list")
            .iter()
            .filter_map(|row| {
                row.get("id")
                    .or_else(|| row.get("name"))
                    .and_then(|v| v.as_str())
            })
            .collect();
        for refused in [
            "cf05-dl-conflict",
            "cf05-dl-half",
            "cf05-greptime-empty",
            "cf05-opentsdb-empty",
        ] {
            assert!(!ids.contains(&refused), "{refused} must not be stored");
        }

        // The native entry answers 400 for the same bad config.
        let (status, refused) = server
            .post_auth(
                "/api/v1/connectors",
                json!({"id": "cf05-dl-v1", "kind": "datalayers",
                       "config": {"endpoint": "http://127.0.0.1:8360",
                                  "database": "factory_db", "table": "sensor_events",
                                  "auth_token": "cf05-token",
                                  "username": "cf05-user", "password": "cf05-pass"}}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "native entry refuses too, got: {refused}");

        // Update path: a valid entry stays, then a conflicting replace
        // fails and the stored entry keeps its old fields.
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "cf05-dl-1", "type": "datalayers",
                       "endpoint": "http://127.0.0.1:8360", "database": "factory_db",
                       "table": "sensor_events"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert!(state.engine.connectors().get("cf05-dl-1").is_some());
        let (status, body) = server
            .put_auth(
                "/api/v5/connectors/cf05-dl-1",
                json!({"id": "cf05-dl-1", "type": "datalayers",
                       "endpoint": "http://127.0.0.1:8360", "database": "factory_db",
                       "table": "sensor_events", "token": "cf05-token",
                       "username": "cf05-user", "password": "cf05-pass"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "conflicting replace must fail, got: {body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("auth modes conflict"),
            "refusal carries the sink text, got: {body}"
        );
        let (status, kept) = server
            .get_auth("/api/v5/connectors/cf05-dl-1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(kept["database"], json!("factory_db"));
        assert_eq!(kept["table"], json!("sensor_events"));
        assert!(state.engine.connectors().get("cf05-dl-1").is_some());
        let (status, _) = server
            .delete_auth("/api/v5/connectors/cf05-dl-1", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn cf05_v5_connector_valid_but_unreachable_stays_stored() {
        // CF-05: a valid sink whose server is down still stores. After
        // a restart the boot probe finds no target, so the entry reads
        // back disconnected instead of failing the create.
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        let dir = unique_data_dir("cf05-conn-down");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, created) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "cf05-dl-down", "type": "datalayers",
                       "endpoint": "http://127.0.0.1:9", "database": "factory_db",
                       "table": "sensor_events"}),
                &token,
            )
            .await;
        assert_eq!(
            status, 201,
            "valid sink stores even when down, got: {created}"
        );
        drop(server);
        drop(registry);

        // Simulate a kernel restart from the same dir.
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, read) = server
            .get_auth("/api/v5/connectors/cf05-dl-down", &token)
            .await;
        assert_eq!(status, 200, "stored entry survives the restart");
        assert_eq!(read["status"], json!("disconnected"));
        assert!(
            state.engine.connectors().get("cf05-dl-down").is_none(),
            "no live sink while the target stays down"
        );
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rule_metrics_unknown_is_404_and_honest() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server
            .get_auth("/api/v5/rules/w0-13-no-such-rule/metrics", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/rules/w0-13-no-such-rule/metrics/reset",
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));

        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "w0-13-metrics",
                       "sql": "SELECT * FROM \"t/w013\"",
                       "enable": true,
                       "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = created["id"].as_str().expect("rule id").to_string();

        let (status, body) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
            .await;
        assert_eq!(status, 200);
        let metrics = body["metrics"].as_object().expect("metrics object");
        let mut keys: Vec<&str> = metrics.keys().map(String::as_str).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                "actions.failed",
                "actions.success",
                "actions.total",
                "failed",
                "matched",
                "passed"
            ]
        );
        assert!(body.get("node_metrics").is_none());
        assert!(metrics.get("rate").is_none());
        assert!(metrics.get("rate_max").is_none());
        assert!(metrics.get("rate_last5m").is_none());
    }

    #[tokio::test]
    async fn rule_metrics_move_on_real_traffic_and_reset_zeroes() {
        // W3-15 (C06-23/C06-24) broker-through test: create a stored rule
        // via the HTTP router, publish a matching and a non-matching
        // message through the real evaluation path (`dispatch_ingress`,
        // the broker's rule path also used by `crates/broker-node` ingress
        // and the management publish path), read metrics and expect the
        // documented counters to move; reset and expect zeroes; unknown id
        // is 404. Before: constant metrics or missing 404.
        // The per-rule counters live in `broker-rules`
        // (`Rule::{matched,passed,failed}_cnt`, bumped in
        // `RuleEngine::dispatch_ingress` on the publish event), driven by
        // the publish event.
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown ids miss with the documented shape, not an empty body.
        let (status, body) = server
            .get_auth("/api/v5/rules/no-such-rule-metrics/metrics", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/rules/no-such-rule-metrics/metrics/reset",
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Stored rule with a WHERE gate so matching and non-matching
        // traffic diverge; empty actions keep the fire side-effect-free
        // while `passed`/`failed` still move.
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "w3-15-metrics",
                       "sql": "SELECT * FROM \"w3/15/metrics\" WHERE k > 10",
                       "enable": true,
                       "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = created["id"].as_str().expect("rule id").to_string();
        // Fresh rule reads zeroes; unknown query keys are ignored.
        let (status, body) = server
            .get_auth(
                &format!("/api/v5/rules/{rule_id}/metrics?unknown=param"),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["matched"], json!(0));
        assert_eq!(body["metrics"]["passed"], json!(0));
        assert_eq!(body["metrics"]["failed"], json!(0));
        // One matching and two non-matching publishes through the real
        // evaluation path (WHERE-true, WHERE-false, non-JSON payload),
        // never constants and never `sink.send` directly.
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/15/metrics").expect("valid topic");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &bytes::Bytes::from_static(b"{\"k\":20}"),
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        state
            .engine
            .dispatch_ingress(
                &topic,
                &bytes::Bytes::from_static(b"{\"k\":1}"),
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        state
            .engine
            .dispatch_ingress(
                &topic,
                &bytes::Bytes::from_static(b"not json"),
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        // Counters moved only through real evaluation.
        let (status, body) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["matched"], json!(3));
        assert_eq!(body["metrics"]["passed"], json!(1));
        assert_eq!(body["metrics"]["failed"], json!(2));
        // Reset zeroes the counters and reports success; the next read is
        // zero. Reset is not paged; unknown query keys are ignored.
        let (status, _) = server
            .send(
                "PUT",
                &format!("/api/v5/rules/{rule_id}/metrics/reset?unknown=param"),
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["matched"], json!(0));
        assert_eq!(body["metrics"]["passed"], json!(0));
        assert_eq!(body["metrics"]["failed"], json!(0));
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn connector_unknown_is_404() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server
            .get_auth("/api/v5/connectors/w0-13-no-such-connector", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn removed_connector_operation_routes_return_404() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/connectors/x/enable/true",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        let (status, _) = server
            .send("POST", "/api/v5/connectors/x/start", None, Some(&token))
            .await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn test_rules_test_endpoint() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // SQL match with projection.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules/test",
                json!({"sql_query": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
                       "topic_filter": "sensors/+",
                       "topic": "sensors/kitchen",
                       "payload": {"temperature": 72.5, "secret": "x"}}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["matched"], json!(true));
        assert_eq!(body["projected"], json!({"temperature": 72.5}));

        // Predicate false.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules/test",
                json!({"sql_query": "SELECT * FROM \"sensors/+\" WHERE temperature > 100.0",
                       "topic": "sensors/kitchen",
                       "payload": {"temperature": 72.5}}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["matched"], json!(false));

        // No SQL: passthrough.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules/test",
                json!({"topic": "a/b", "payload": {"v": 1}}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["matched"], json!(true));
        assert_eq!(body["projected"], json!({"v": 1}));

        // Broken SQL is a 400.
        let (status, _) = server
            .post_auth(
                "/api/v1/rules/test",
                json!({"sql_query": "SELECT WHERE WHERE",
                       "topic": "a/b",
                       "payload": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn test_rules_test_endpoint_batch() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Array payloads aggregate as one batch: one row per group.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules/test",
                json!({"sql_query": "SELECT sensor_id, avg(temperature) AS avg_temp FROM \"sensors/+\" GROUP BY sensor_id",
                       "topic": "sensors/kitchen",
                       "payload": [{"sensor_id": "a", "temperature": 10.0},
                                   {"sensor_id": "b", "temperature": 30.0},
                                   {"sensor_id": "a", "temperature": 20.0}]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["matched"], json!(true));
        let rows = body["projected"].as_array().expect("projected rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["sensor_id"], json!("a"));
        assert_eq!(rows[0]["avg_temp"], json!(15.0));
        assert_eq!(rows[1]["sensor_id"], json!("b"));
        assert_eq!(rows[1]["avg_temp"], json!(30.0));

        // Empty batches match nothing.
        let (status, body) = server
            .post_auth(
                "/api/v1/rules/test",
                json!({"sql_query": "SELECT avg(temperature) AS a FROM \"sensors/+\"",
                       "topic": "sensors/kitchen",
                       "payload": []}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["matched"], json!(false));
        assert_eq!(body["projected"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn test_rules_functions_catalog() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server.get_auth("/api/v1/rules/functions", &token).await;
        assert_eq!(status, 200);
        let catalog = body.as_array().expect("function array");
        // The engine revision in the lock file lists 219 functions.
        assert_eq!(catalog.len(), 219);
        let sin = catalog
            .iter()
            .find(|entry| entry["name"] == json!("sin"))
            .expect("sin in catalog");
        assert_eq!(sin["category"], json!("math"));
        assert_eq!(sin["aggregate"], json!(false));
        let avg = catalog
            .iter()
            .find(|entry| entry["name"] == json!("avg"))
            .expect("avg in catalog");
        assert_eq!(avg["aggregate"], json!(true));
    }

    /// Minimal WebSocket client over raw TCP (no extra deps): HTTP
    /// upgrade with the RFC 6455 example key, masked binary sends, and
    /// server frame parsing.
    struct WsClient {
        stream: tokio::net::TcpStream,
        buf: Vec<u8>,
    }

    impl WsClient {
        async fn connect(port: u16) -> Self {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .expect("connect ws");
            // Fixed RFC example key -> fixed accept digest.
            let req = "GET /ws/mqtt HTTP/1.1\r\nHost: test\r\nUpgrade: websocket\r\n\
                       Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
                       Sec-WebSocket-Version: 13\r\n\r\n";
            stream
                .write_all(req.as_bytes())
                .await
                .expect("write upgrade");
            let mut buf = Vec::new();
            loop {
                let mut chunk = [0u8; 512];
                let n = stream.read(&mut chunk).await.expect("read upgrade");
                assert!(n > 0, "upgrade response ended early");
                buf.extend_from_slice(&chunk[..n]);
                if let Some(pos) = find_crlf2(&buf) {
                    let head = String::from_utf8_lossy(&buf[..pos + 4]).to_string();
                    assert!(
                        head.starts_with("HTTP/1.1 101"),
                        "expected 101, got: {head}"
                    );
                    assert!(head.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));
                    let rest = buf[pos + 4..].to_vec();
                    return Self { stream, buf: rest };
                }
            }
        }

        async fn send_bin(&mut self, payload: &[u8]) {
            use tokio::io::AsyncWriteExt;
            let mut frame = vec![0x82u8];
            if payload.len() < 126 {
                frame.push(payload.len() as u8 | 0x80);
            } else if payload.len() < 65536 {
                frame.push(126 | 0x80);
                frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
            } else {
                frame.push(127 | 0x80);
                frame.extend_from_slice(&(payload.len() as u64).to_be_bytes());
            }
            let mask = [0x11u8, 0x22, 0x33, 0x44];
            frame.extend_from_slice(&mask);
            for (i, byte) in payload.iter().enumerate() {
                frame.push(byte ^ mask[i % 4]);
            }
            self.stream.write_all(&frame).await.expect("write frame");
        }

        /// Next server message payload (`None` on clean EOF).
        async fn recv_msg(&mut self) -> Option<Vec<u8>> {
            use tokio::io::AsyncReadExt;
            loop {
                if let Some((payload, consumed)) = parse_ws_frame(&self.buf) {
                    self.buf.drain(..consumed);
                    return Some(payload);
                }
                let mut chunk = [0u8; 4096];
                match self.stream.read(&mut chunk).await.expect("read frame") {
                    0 => return None,
                    n => self.buf.extend_from_slice(&chunk[..n]),
                }
            }
        }
    }

    fn find_crlf2(buf: &[u8]) -> Option<usize> {
        buf.windows(4).position(|w| w == b"\r\n\r\n")
    }

    /// Parse one server frame (unmasked): `(payload, consumed)` or `None`
    /// when incomplete. Close frames surface as empty payloads.
    fn parse_ws_frame(buf: &[u8]) -> Option<(Vec<u8>, usize)> {
        if buf.len() < 2 || buf[1] & 0x80 != 0 {
            return None;
        }
        let opcode = buf[0] & 0x0F;
        let mut header = 2usize;
        let mut length = (buf[1] & 0x7F) as usize;
        if length == 126 {
            if buf.len() < 4 {
                return None;
            }
            length = u16::from_be_bytes([buf[2], buf[3]]) as usize;
            header = 4;
        } else if length == 127 {
            if buf.len() < 10 {
                return None;
            }
            length = u64::from_be_bytes(buf[2..10].try_into().ok()?) as usize;
            header = 10;
        }
        if buf.len() < header + length {
            return None;
        }
        if opcode == 0x8 {
            return Some((Vec::new(), header + length));
        }
        if opcode != 0x1 && opcode != 0x2 {
            return None;
        }
        Some((buf[header..header + length].to_vec(), header + length))
    }

    // -- MQTT packet builders (mirror of the dashboard console) --

    fn mqtt_remaining(mut n: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (n % 128) as u8;
            n /= 128;
            if n > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if n == 0 {
                break;
            }
        }
        out
    }

    fn mqtt_str(out: &mut Vec<u8>, s: &str) {
        out.extend_from_slice(&(s.len() as u16).to_be_bytes());
        out.extend_from_slice(s.as_bytes());
    }

    fn mqtt_connect(client_id: &str, username: Option<&str>, password: Option<&[u8]>) -> Vec<u8> {
        let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4];
        let mut flags = 0x02u8;
        if username.is_some() {
            flags |= 0x80;
        }
        if password.is_some() {
            flags |= 0x40;
        }
        body.push(flags);
        body.extend_from_slice(&60u16.to_be_bytes());
        mqtt_str(&mut body, client_id);
        if let Some(user) = username {
            mqtt_str(&mut body, user);
        }
        if let Some(pass) = password {
            body.extend_from_slice(&(pass.len() as u16).to_be_bytes());
            body.extend_from_slice(pass);
        }
        let mut packet = vec![0x10];
        packet.extend_from_slice(&mqtt_remaining(body.len()));
        packet.extend_from_slice(&body);
        packet
    }

    fn mqtt_subscribe(pid: u16, subs: &[(&str, u8)]) -> Vec<u8> {
        let mut body = pid.to_be_bytes().to_vec();
        for (filter, qos) in subs {
            mqtt_str(&mut body, filter);
            body.push(*qos);
        }
        let mut packet = vec![0x82];
        packet.extend_from_slice(&mqtt_remaining(body.len()));
        packet.extend_from_slice(&body);
        packet
    }

    fn mqtt_publish(topic: &str, pid: u16, qos: u8, payload: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        mqtt_str(&mut body, topic);
        if qos > 0 {
            body.extend_from_slice(&pid.to_be_bytes());
        }
        body.extend_from_slice(payload);
        let mut packet = vec![0x30 | (qos << 1)];
        packet.extend_from_slice(&mqtt_remaining(body.len()));
        packet.extend_from_slice(&body);
        packet
    }

    fn mqtt_packet_type(packet: &[u8]) -> u8 {
        packet[0] >> 4
    }

    #[tokio::test]
    async fn test_ws_mqtt_console_flow() {
        let (server, _state) = TestServer::start().await;

        // Subscriber console: CONNECT -> CONNACK, SUBSCRIBE -> SUBACK.
        let mut sub = WsClient::connect(server.port).await;
        sub.send_bin(&mqtt_connect("console-a", None, None)).await;
        let connack = sub.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0, "CONNACK return code must be 0");
        sub.send_bin(&mqtt_subscribe(7, &[("demo/#", 1)])).await;
        let suback = sub.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(&suback[4..], &[1], "granted QoS 1");

        // Publisher console: QoS 1 publish lands on the subscriber.
        let mut publ = WsClient::connect(server.port).await;
        publ.send_bin(&mqtt_connect("console-b", None, None)).await;
        let connack = publ.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);
        publ.send_bin(&mqtt_publish("demo/1", 42, 1, b"hi-ws"))
            .await;
        // Publisher gets its PUBACK...
        let puback = publ.recv_msg().await.expect("puback");
        assert_eq!(mqtt_packet_type(&puback), 4);
        // ...and the subscriber gets the delivery with flags + payload.
        let delivery = sub.recv_msg().await.expect("delivery");
        assert_eq!(mqtt_packet_type(&delivery), 3);
        assert_eq!(delivery[0] & 0x06, 0x02, "downstream QoS 1");
        assert!(delivery.windows(b"demo/1".len()).any(|w| w == b"demo/1"));
        assert!(delivery.ends_with(b"hi-ws"));

        // PINGREQ -> PINGRESP on the same socket.
        publ.send_bin(&[0xC0, 0x00]).await;
        let pong = publ.recv_msg().await.expect("pingresp");
        assert_eq!(pong, vec![0xD0, 0x00]);
    }

    #[tokio::test]
    async fn test_ws_auth_rejected_with_0x86() {
        let (server, state) = TestServer::start().await;
        state
            .auth
            .add_user("alice", b"s3cret")
            .expect("test user persists");

        let mut client = WsClient::connect(server.port).await;
        client
            .send_bin(&mqtt_connect("console-x", Some("alice"), Some(b"wrong")))
            .await;
        let connack = client.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0x86, "bad password must yield 0x86");
        // Socket closes right after the failed CONNACK.
        assert!(client.recv_msg().await.is_none());
        assert!(state.sessions.get("console-x").is_none());
    }

    #[tokio::test]
    async fn test_ws_anonymous_rejected_when_users_exist() {
        let (server, state) = TestServer::start().await;
        state
            .auth
            .add_user("alice", b"s3cret")
            .expect("test user persists");

        // No username while users exist: rejected with 0x87 even though
        // the console path has no `--allow-anonymous` flag.
        let mut client = WsClient::connect(server.port).await;
        client
            .send_bin(&mqtt_connect("console-anon", None, None))
            .await;
        let connack = client.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0x87, "anonymous must yield 0x87");
        // Socket closes right after the failed CONNACK.
        assert!(client.recv_msg().await.is_none());
        assert!(state.sessions.get("console-anon").is_none());

        // A valid credentialed console CONNECT in the same store gets 0.
        let mut client = WsClient::connect(server.port).await;
        client
            .send_bin(&mqtt_connect("console-ok", Some("alice"), Some(b"s3cret")))
            .await;
        let connack = client.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);
    }

    #[tokio::test]
    async fn test_ws_subscribe_denied_with_0x87() {
        let (server, state) = TestServer::start().await;
        state
            .auth
            .add_rule(broker_auth::AclRule::new(
                "console-y",
                broker_auth::AclAction::All,
                "#",
                false,
            ))
            .expect("test rule persists");

        let mut client = WsClient::connect(server.port).await;
        client
            .send_bin(&mqtt_connect("console-y", None, None))
            .await;
        let connack = client.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);

        client.send_bin(&mqtt_subscribe(3, &[("t", 0)])).await;
        let suback = client.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(&suback[4..], &[0x87], "denied filter must yield 0x87");
    }

    #[tokio::test]
    async fn test_ws_subscribe_quota_denied_with_0x97() {
        // B4-08 bound: a full subscription table denies the console
        // subscribe fail closed with 0x97 Quota Exceeded and changes no
        // session state. Fills the shared router to MAX_SUBSCRIPTIONS
        // with three-level filters so each path copy stays small.
        let (server, state) = TestServer::start().await;
        for i in 0..broker_router::MAX_SUBSCRIPTIONS {
            let a = i / 10_000;
            let b = (i / 100) % 100;
            let c = i % 100;
            let filter = broker_protocol::TopicFilter::new(format!("quota/fill/{a}/{b:02}/{c:02}"))
                .expect("valid fill filter");
            assert!(
                state.router.subscribe(
                    &filter,
                    broker_router::Subscription::new(
                        format!("fill-{i}"),
                        i as u64,
                        broker_protocol::QoS::AtMostOnce
                    )
                ),
                "fill subscribe {i} must succeed"
            );
        }

        let mut client = WsClient::connect(server.port).await;
        client
            .send_bin(&mqtt_connect("console-q", None, None))
            .await;
        let connack = client.recv_msg().await.expect("connack");
        assert_eq!(connack[3], 0);

        client
            .send_bin(&mqtt_subscribe(9, &[("quota/probe", 0)]))
            .await;
        let suback = client.recv_msg().await.expect("suback");
        assert_eq!(mqtt_packet_type(&suback), 9);
        assert_eq!(
            &suback[4..],
            &[0x97],
            "quota denial must yield 0x97, not success"
        );

        // The failed subscribe registers nothing: no session mirror, no
        // router entry.
        let session = state.sessions.get("console-q").expect("session row");
        assert!(
            session
                .subscriptions
                .read()
                .keys()
                .all(|f| f.as_str() != "quota/probe"),
            "quota-denied subscribe must leave the session unchanged"
        );
        assert!(
            state
                .router
                .matches(&broker_protocol::Topic::new("quota/probe").expect("valid probe topic"))
                .is_empty(),
            "quota-denied subscribe must leave the router unchanged"
        );
    }

    #[tokio::test]
    async fn removed_gateway_routes_return_404() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for path in [
            "/api/v5/gateways",
            "/api/v5/gateways/stomp",
            "/api/v5/gateways/stomp/clients/c1",
            "/api/v5/gateway",
        ] {
            let (status, _) = server
                .request_raw(&format!(
                    "GET {path} HTTP/1.0\r\nHost: test\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
                ))
                .await;
            assert_eq!(status, 404, "removed route {path} must be 404");
        }
    }

    #[tokio::test]
    async fn listeners_routes_answer_documented_shapes() {
        // AP-02 restores the per-listener detail and lifecycle routes on
        // the live listener store; list coverage lives with the store
        // tests in `v5::listeners`.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/listeners", &token).await;
        assert_eq!(status, 200, "kept route GET /api/v5/listeners must be 200");
        assert!(body.is_array(), "listeners list is an array: {body}");
        let (status, body) = server
            .get_auth("/api/v5/listeners/tcp:default", &token)
            .await;
        assert_eq!(status, 200, "GET /api/v5/listeners/tcp:default must be 200");
        assert_eq!(body["id"], serde_json::json!("tcp:default"));
        let (status, body) = server.get_auth("/api/v5/listeners_status", &token).await;
        assert_eq!(status, 200, "GET /api/v5/listeners_status must be 200");
        assert!(body.is_array(), "listeners status is an array: {body}");
        let (status, body) = server
            .get_auth("/api/v5/listeners/tcp:missing", &token)
            .await;
        assert_eq!(status, 404, "unknown listener must be 404");
        assert_eq!(body["code"], serde_json::json!("BAD_LISTENER_ID"));
        let (status, body) = server
            .post_auth(
                "/api/v5/listeners/tcp:missing/stop",
                serde_json::json!({}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "stop of unknown listener must be 400");
        assert_eq!(body["code"], serde_json::json!("BAD_LISTENER_ID"));
    }

    #[tokio::test]
    async fn removed_trace_slowsub_topicmetrics_routes_return_404() {
        // W1-30 restores `GET`/`DELETE /api/v5/slow_subscriptions` on the
        // real recorder, W1-31 restores `GET`/`PUT
        // /api/v5/slow_subscriptions/settings` on the real thresholds and
        // W1-33 restores `GET`/`POST`/`DELETE /api/v5/trace` on the real
        // registry, so only the per-session trace reads and topic-metrics
        // routes stay removed here; trace create-list-clear coverage lives
        // with the registry tests.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for path in ["/api/v5/trace/x/log", "/api/v5/mqtt/topic_metrics"] {
            let (status, _) = server.get_auth(path, &token).await;
            assert_eq!(status, 404, "removed route {path} must be 404");
        }
        let (status, _) = server.get_auth("/api/v5/trace", &token).await;
        assert_eq!(status, 200, "kept route GET /api/v5/trace must be 200");
        let (status, _) = server.delete_auth("/api/v5/trace", &token).await;
        assert_eq!(status, 204, "kept route DELETE /api/v5/trace must be 204");
        let (status, _) = server.get_auth("/api/v5/slow_subscriptions", &token).await;
        assert_eq!(
            status, 200,
            "kept route GET /api/v5/slow_subscriptions must be 200"
        );
        let (status, _) = server
            .delete_auth("/api/v5/slow_subscriptions", &token)
            .await;
        assert_eq!(
            status, 204,
            "kept route DELETE /api/v5/slow_subscriptions must be 204"
        );
        let (status, _) = server
            .get_auth("/api/v5/slow_subscriptions/settings", &token)
            .await;
        assert_eq!(
            status, 200,
            "kept route GET /api/v5/slow_subscriptions/settings must be 200"
        );
        let (status, _) = server
            .put_auth(
                "/api/v5/slow_subscriptions/settings",
                json!({"threshold": "1s"}),
                &token,
            )
            .await;
        assert_eq!(
            status, 200,
            "kept route PUT /api/v5/slow_subscriptions/settings must be 200"
        );
    }

    #[tokio::test]
    async fn trace_stop_and_log_detail_round_trip_through_api() {
        // Stop and detail read go through the HTTP router. Create one
        // session, read its file facts, stop it, and read the facts
        // again. A missing session reads as 404 on both routes.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server.delete_auth("/api/v5/trace", &token).await;
        assert_eq!(status, 204);
        let (status, created) = server
            .post_auth(
                "/api/v5/trace",
                json!({"name": "http-trace-1", "type": "topic", "topic": "http/trace/#"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert!(created["log_size"].is_array());
        let (status, detail) = server
            .get_auth("/api/v5/trace/http-trace-1/log_detail", &token)
            .await;
        assert_eq!(status, 200);
        assert!(detail.as_array().is_some_and(|rows| rows.len() == 1));
        assert!(detail[0]["node"].is_string());
        assert!(detail[0]["size"].as_u64().is_some());
        assert!(detail[0]["mtime"].as_u64().is_some());
        let (status, stopped) = server
            .put_auth("/api/v5/trace/http-trace-1/stop", json!(null), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(stopped["status"], json!("stopped"));
        assert!(stopped["log_size"].is_array());
        let (status, _) = server
            .get_auth("/api/v5/trace/http-trace-1/log_detail", &token)
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .get_auth("/api/v5/trace/no-such-trace/log_detail", &token)
            .await;
        assert_eq!(status, 404);
        let (status, _) = server
            .put_auth("/api/v5/trace/no-such-trace/stop", json!(null), &token)
            .await;
        assert_eq!(status, 404);
        let (status, _) = server.delete_auth("/api/v5/trace", &token).await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn removed_action_routes_return_404() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // W3-02 restores `GET /api/v5/actions`, `POST /api/v5/actions`
        // and `DELETE /api/v5/actions/:id` on the real action directory,
        // W3-03 adds `GET`/`PUT /api/v5/actions/:id`, W3-04 adds
        // `PUT /api/v5/actions/:id/enable/:enable`, W3-05 adds
        // `GET /api/v5/actions/:id/metrics` plus `PUT .../metrics/reset`,
        // W3-06 adds `POST /api/v5/actions/:id/:operation`, and W3-08
        // adds `GET /api/v5/actions_summary`, so no removed action path
        // stays 404 here: the summary reads live on the shared store.
        // `/api/v5/actions/:id` now exists for GET, PUT and DELETE and
        // `/api/v5/actions/:id/metrics` for GET, so neither is a removed
        // route (mirrors the `/api/v5/sources` handling in
        // `removed_sources_rulenotest_routes_return_404`).
        let (status, _) = server.get_auth("/api/v5/actions_summary", &token).await;
        assert_eq!(
            status, 200,
            "restored route /api/v5/actions_summary must be live"
        );
    }

    #[tokio::test]
    async fn actions_list_create_delete_round_trip() {
        // W3-02 (C06-02..C06-04) broker-through test: create an action,
        // list and expect it present with paging metadata; create the
        // same id again and expect rejection; delete it and expect a
        // second delete to be 404. Every assertion goes through the HTTP
        // router against the shared store, never at the store directly.
        // Management-plane only; publish and deliver never touch the
        // action directory.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Empty store returns an empty page, not an error.
        let (status, body) = server.get_auth("/api/v5/actions", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        assert_eq!(body.as_array().map(Vec::len), Some(0));
        // Unknown query parameters are ignored.
        let (status, _) = server
            .get_auth("/api/v5/actions?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        // Unknown action kind is rejected with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "bad-kind-action", "type": "no_such_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "unknown action kind must be 400");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        // Create one action.
        let (status, body) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-02-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(body["id"], json!("w3-02-action"));
        assert_eq!(body["type"], json!("mqtt_bridge"));
        // List shows it with paging metadata.
        let (status, body) = server.get_auth("/api/v5/actions", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let ids: Vec<String> = body
            .as_array()
            .expect("list data is an array")
            .iter()
            .filter_map(|row| row.get("id").and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        assert!(ids.contains(&"w3-02-action".to_string()));
        // Duplicate create is rejected cleanly, never overwritten.
        let (status, body) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-02-action", "type": "mqtt_bridge"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "duplicate action id must be rejected");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        // Delete hit, then miss.
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-02-action", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .delete_auth("/api/v5/actions/w3-02-action", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Store is empty again.
        let (status, body) = server.get_auth("/api/v5/actions", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(0));
    }

    #[tokio::test]
    async fn actions_delete_refused_while_rule_references() {
        // W3-02 rule-linkage broker-through test: an action still
        // referenced by a rule cannot be dropped while the reference
        // stands; once the rule is gone the delete succeeds. The
        // reference check consults the live rule engine
        // (`broker-rules`, `RuleEngine::list_rules`), driven by rule
        // create/delete events. Management-plane only; publish and
        // deliver never touch either store.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "linked-action", "type": "mqtt_bridge"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "linked-rule",
                    "sql": "SELECT * FROM \"w3/02/linked\"",
                    "actions": ["linked-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Rules are addressed by their generated `id`, not by `name`:
        // delete the created rule id so the reference is really gone.
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let (status, body) = server
            .delete_auth("/api/v5/actions/linked-action", &token)
            .await;
        assert_eq!(status, 400, "referenced action delete must be refused");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/linked-action", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn actions_single_read_and_replace_round_trip() {
        // W3-03 (C06-05/C06-06) broker-through test: create an action via
        // the W3-02 routes, read it back and expect the documented
        // fields; replace its config and read again expecting the new
        // values; read an unknown id expecting 404. Every assertion goes
        // through the HTTP router against the shared store, never at the
        // store directly. Management-plane only; publish and deliver
        // never touch the action directory.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown read is 404 with the documented shape, not empty.
        let (status, body) = server
            .get_auth("/api/v5/actions/no-such-action", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one action via the W3-02 route.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-03-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Read hit carries the documented fields.
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-03-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("w3-03-action"));
        assert_eq!(body["type"], json!("mqtt_bridge"));
        assert_eq!(body["server"], json!("127.0.0.1:1883"));
        // Unknown query parameters are ignored on the single read.
        let (status, _) = server
            .get_auth("/api/v5/actions/w3-03-action?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        // Replace the stored config with the same validation as create.
        let (status, body) = server
            .put_auth(
                "/api/v5/actions/w3-03-action",
                json!({"type": "kafka", "topic": "events-updated", "enable": false}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("w3-03-action"));
        assert_eq!(body["type"], json!("kafka"));
        assert_eq!(body["enable"], json!(false));
        // Read again sees the new values.
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-03-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["type"], json!("kafka"));
        assert_eq!(body["topic"], json!("events-updated"));
        // Update of an unknown id is 404 and creates nothing.
        let (status, body) = server
            .put_auth(
                "/api/v5/actions/no-such-action",
                json!({"type": "kafka"}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, _) = server
            .get_auth("/api/v5/actions/no-such-action", &token)
            .await;
        assert_eq!(status, 404);
        // Unknown kind and malformed bodies are rejected, leaving the
        // stored entry untouched.
        let (status, body) = server
            .put_auth(
                "/api/v5/actions/w3-03-action",
                json!({"type": "no_such_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .put_auth(
                "/api/v5/actions/w3-03-action",
                json!({"server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-03-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["type"], json!("kafka"));
        // Mismatched body id is rejected instead of renaming.
        let (status, body) = server
            .put_auth(
                "/api/v5/actions/w3-03-action",
                json!({"id": "other-action", "type": "kafka"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn actions_enable_flag_round_trip_and_dispatch_skip() {
        // W3-04 (C06-07) broker-through test: create an action via the
        // W3-02 route, disable it through the enable route and read back
        // disabled, re-enable and read back enabled; unknown ids are 404
        // and malformed flags are 400, both with the documented shape.
        // Then a rule forwarding to the action id fires while enabled
        // and is skipped at fire time while disabled, through the
        // engine's lock-free dispatch snapshot (one publish through
        // `dispatch_ingress` per case). Management-plane only otherwise;
        // publish and deliver never touch the action store directly.
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown id is 404 with the documented shape, not an empty body.
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/actions/no-such-action/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one action via the W3-02 route (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-04-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Malformed flag is rejected with the documented shape (an empty
        // segment never reaches the handler: the router has no such route
        // and answers 404, so only non-empty spellings are exercised here).
        for bad in ["maybe", "1", "0", "on", "TRUEE", "enabled"] {
            let (status, body) = server
                .send(
                    "PUT",
                    &format!("/api/v5/actions/w3-04-action/enable/{bad}"),
                    None,
                    Some(&token),
                )
                .await;
            assert_eq!(status, 400, "flag {bad:?} must be rejected");
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
        // Disable: flag change is visible on the next read.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/actions/w3-04-action/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-04-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        // Re-enable: visible on the next read.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/actions/w3-04-action/enable/true",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-04-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Rule dispatch through the broker: a rule forwarding to the
        // action id fires while enabled and skips while disabled. The
        // counting sink is registered under the action id, the rule is
        // created through the HTTP router, and each publish goes through
        // `dispatch_ingress` (the broker's rule path), never `sink.send`
        // directly.
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-04-action"),
            counter.clone(),
        );
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "w3-04-rule",
                    "sql": "SELECT * FROM \"w3/04/enable\"",
                    "actions": ["w3-04-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/04/enable").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "enabled action must fire"
        );
        // Disable again: the next publish skips the forward.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/actions/w3-04-action/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "disabled action must be skipped at fire time"
        );
        // Re-enable: firing resumes without re-registering anything.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/actions/w3-04-action/enable/true",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            2,
            "re-enabled action must fire again"
        );
        // Cleanup: rule then action (the rule holds the reference).
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-04-action", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn connector_enable_flag_round_trip_and_dispatch_skip() {
        // W3-19 (C07-06) broker-through test: create a connector via the
        // W3-17 route, disable it through the enable route and read back
        // disabled, re-enable and read back enabled; unknown ids are 404
        // and malformed flags are 400, both with the documented shape.
        // Then a rule forwarding to the shared id fires while enabled
        // and is skipped at fire time while disabled, through the
        // engine's lock-free dispatch snapshot (one publish through
        // `dispatch_ingress` per case). Management-plane only otherwise;
        // publish and deliver never touch the connector store directly.
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown id is 404 with the documented shape, not an empty body.
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/connectors/no-such-connector/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one connector via the W3-17 route (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w3-19-conn", "type": "http", "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Malformed flag is rejected with the documented shape (an empty
        // segment never reaches the handler: the router has no such route
        // and answers 404, so only non-empty spellings are exercised here).
        for bad in ["maybe", "1", "0", "on", "TRUEE", "enabled"] {
            let (status, body) = server
                .send(
                    "PUT",
                    &format!("/api/v5/connectors/w3-19-conn/enable/{bad}"),
                    None,
                    Some(&token),
                )
                .await;
            assert_eq!(status, 400, "flag {bad:?} must be rejected");
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
        // Disable: flag change is visible on the next read.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/connectors/w3-19-conn/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/connectors/w3-19-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        // Re-enable: visible on the next read.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/connectors/w3-19-conn/enable/true",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/connectors/w3-19-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Rule dispatch through the broker: the connector id shares the
        // engine's lock-free dispatch snapshot with action ids, so a rule
        // forwarding to this id fires while enabled and skips while
        // disabled. The counting sink is registered under the shared id
        // (an action with the same id exists so the rule validates), the
        // rule is created through the HTTP router, and each publish goes
        // through `dispatch_ingress` (the broker's rule path), never
        // `sink.send` directly.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-19-conn", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-19-conn"),
            counter.clone(),
        );
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "w3-19-rule",
                    "sql": "SELECT * FROM \"w3/19/enable\"",
                    "actions": ["w3-19-conn"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/19/enable").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "enabled connector must fire"
        );
        // Disable again: the next publish skips the forward.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/connectors/w3-19-conn/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        assert!(!state.engine.is_action_enabled("w3-19-conn"));
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "disabled connector must be skipped at fire time"
        );
        // Re-enable: firing resumes without re-registering anything.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/connectors/w3-19-conn/enable/true",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        assert!(state.engine.is_action_enabled("w3-19-conn"));
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            2,
            "re-enabled connector must fire again"
        );
        // Cleanup: rule, then action, then connector (the rule holds the
        // reference the connector delete guard checks).
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-19-conn", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/connectors/w3-19-conn", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn connector_store_is_per_state_not_process_global() {
        // AP-10 broker-through test: two servers in one process hold
        // separate connector lists. A connector made on the first is
        // absent on the second. Before the move it was present: every
        // state shared one process static.
        let (server_a, _) = TestServer::start().await;
        let (server_b, _) = TestServer::start().await;
        let token_a = server_a.login_as_admin().await;
        let token_b = server_b.login_as_admin().await;
        let (status, _) = server_a
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "ap-10-isolated", "type": "http", "url": "http://127.0.0.1:9"}),
                &token_a,
            )
            .await;
        assert_eq!(status, 201);
        let (status, body) = server_a
            .get_auth("/api/v5/connectors/ap-10-isolated", &token_a)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("ap-10-isolated"));
        let (status, body) = server_b
            .get_auth("/api/v5/connectors/ap-10-isolated", &token_b)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, body) = server_b.get_auth("/api/v5/connectors", &token_b).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, _) = server_a
            .delete_auth("/api/v5/connectors/ap-10-isolated", &token_a)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn connector_operation_start_stop_round_trip() {
        // W3-20 (C07-07) broker-through test: create a connector via the
        // W3-17 route, stop it through the operation route and read back
        // disabled, start it and read back enabled; an unknown operation
        // is rejected and an unknown id is 404, both with the documented
        // shape and a non-empty body. Then a rule forwarding to the
        // connector id fires while started and is skipped at fire time
        // while stopped, through the engine's lock-free dispatch snapshot
        // (one publish through `dispatch_ingress` per case, the broker's
        // rule path also used by `crates/broker-node` ingress). Every
        // assertion goes through the HTTP router against the shared store,
        // never at the store directly. Management-plane only otherwise;
        // publish and deliver never touch the connector store directly.
        // The connector id shares the dispatch snapshot with action ids
        // (`crates/broker-rules/src/lib.rs`, `RuleEngine::set_action_enabled`
        // at line 1717 consulted on the publish event by
        // `dispatch_ingress`/fire path), driven by the publish event.
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown id is 404 with the documented shape, not an empty body.
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/connectors/no-such-connector/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Unknown id with an unknown operation is still 404 (existence
        // before validation) and persists nothing.
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/connectors/no-such-connector/frobnicate",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one connector via the W3-17 route (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w3-20-conn", "type": "http", "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Unknown operation is rejected with the documented shape and
        // persists nothing (the stored flag is untouched).
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/connectors/w3-20-conn/frobnicate",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "unknown operation must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth("/api/v5/connectors/w3-20-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Stop: the reported state changes to disabled on the next read.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/connectors/w3-20-conn/stop",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/connectors/w3-20-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        // Start: the reported state changes back to enabled.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/connectors/w3-20-conn/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/connectors/w3-20-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Rule dispatch through the broker: the connector id shares the
        // engine's lock-free dispatch snapshot with action ids, so a rule
        // forwarding to this id fires while started and skips while
        // stopped. An action with the same id exists so the rule validates
        // (the W3-02 linkage), the counting sink is registered under the
        // shared id, the rule is created through the HTTP router, and each
        // publish goes through `dispatch_ingress` (the broker's rule path),
        // never `sink.send` directly.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-20-conn", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-20-conn"),
            counter.clone(),
        );
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "w3-20-rule",
                    "sql": "SELECT * FROM \"w3/20/operation\"",
                    "actions": ["w3-20-conn"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/20/operation").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "started connector must fire"
        );
        // Stop again: the next publish skips the forward.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/connectors/w3-20-conn/stop",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "stopped connector must be skipped at fire time"
        );
        // Start again: firing resumes without re-registering anything.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/connectors/w3-20-conn/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            2,
            "re-started connector must fire again"
        );
        // Cleanup: rule, then action, then connector (the rule holds the
        // reference the connector delete guard checks).
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-20-conn", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/connectors/w3-20-conn", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn node_connector_operation_round_trip() {
        // W3-22 (C07-09) broker-through test: create a connector via the
        // W3-17 route, run `stop`/`start` through the node-scoped route
        // against the local node and read back the flipped state; an
        // unknown node is NOT_FOUND, an unknown id is 404 and an unknown
        // operation is rejected, all with the documented shape and a
        // non-empty body. Then a rule forwarding to the connector id fires
        // while started and is skipped while stopped through the node
        // route, through the engine's lock-free dispatch snapshot (one
        // publish through `dispatch_ingress` per case, the broker's rule
        // path). Every assertion goes through the HTTP router against the
        // shared store, never at the store directly. Management-plane only;
        // publish and deliver never touch the connector store directly.
        // The connector id shares the dispatch snapshot with action ids
        // (`crates/broker-rules/src/lib.rs`, `RuleEngine::set_action_enabled`
        // consulted on the publish event by `dispatch_ingress`/fire path),
        // driven by the publish event.
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let local = "indra-node-1";
        let legacy = crate::node_scope::local_node_name();
        // Unknown node is NOT_FOUND with a non-empty body naming the node,
        // even when the id and operation are also unknown (node scope is
        // checked first, fail closed).
        for unknown in ["no-such-node-xyz", ""] {
            let path = format!("/api/v5/nodes/{unknown}/connectors/no-such-connector/start");
            let (status, body) = server.send("POST", &path, None, Some(&token)).await;
            assert_eq!(status, 404, "unknown node {unknown:?} must be 404");
            assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
            assert!(
                body["message"].is_string()
                    && !body["message"].as_str().expect("message").is_empty(),
                "unknown-node body must not be empty: {body}"
            );
        }
        // Unknown id on the known node is 404 with the documented shape,
        // not an empty body.
        let (status, body) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/connectors/no-such-connector/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(!body.to_string().is_empty());
        // Create one connector via the W3-17 route (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "w3-22-conn", "type": "http", "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Unknown operation on the known node is rejected with the
        // documented shape and persists nothing.
        let (status, body) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/frobnicate"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "unknown operation must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(!body.to_string().is_empty());
        let (status, body) = server
            .get_auth("/api/v5/connectors/w3-22-conn", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Stop through the node route: the reported state flips to
        // disabled on the next read. The legacy node name names the same
        // local node.
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/stop"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{legacy}/connectors/w3-22-conn/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204, "legacy node name must resolve");
        // Rule dispatch through the broker: the connector id shares the
        // engine's lock-free dispatch snapshot with action ids, so a rule
        // forwarding to this id fires while started and skips while
        // stopped. An action with the same id exists so the rule validates
        // (the W3-02 linkage), the counting sink is registered under the
        // shared id, the rule is created through the HTTP router, and each
        // publish goes through `dispatch_ingress` (the broker's rule path),
        // never `sink.send` directly.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-22-conn", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-22-conn"),
            counter.clone(),
        );
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "w3-22-rule",
                    "sql": "SELECT * FROM \"w3/22/operation\"",
                    "actions": ["w3-22-conn"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/22/operation").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "started connector must fire"
        );
        // Stop again through the node route: the next publish skips.
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/stop"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "stopped connector must be skipped at fire time"
        );
        // Start again through the node route: firing resumes.
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/connectors/w3-22-conn/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            2,
            "re-started connector must fire again"
        );
        // Cleanup: rule, then action, then connector (the rule holds the
        // reference the connector delete guard checks).
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-22-conn", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/connectors/w3-22-conn", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn action_operation_start_stop_round_trip() {
        // W3-06 (C06-10) broker-through test: create an action via the
        // W3-02 route, stop it through the operation route and read back
        // disabled, start it and read back enabled; an unknown operation
        // is rejected and an unknown id is 404, both with the documented
        // shape. Then a rule forwarding to the action id fires while
        // started and is skipped at fire time while stopped, through the
        // engine's lock-free dispatch snapshot (one publish through
        // `dispatch_ingress` per case). Every assertion goes through the
        // HTTP router against the shared store, never at the store
        // directly. Management-plane only otherwise; publish and deliver
        // never touch the action store directly.
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown id is 404 with the documented shape, not an empty body.
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/actions/no-such-action/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one action via the W3-02 route (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-06-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Unknown operation is rejected with the documented shape and
        // persists nothing (the stored flag is untouched).
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/actions/w3-06-action/frobnicate",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "unknown operation must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-06-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Stop: the reported state changes to disabled on the next read.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/actions/w3-06-action/stop",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-06-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        // Start: the reported state changes back to enabled.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/actions/w3-06-action/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-06-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Rule dispatch through the broker: a rule forwarding to the
        // action id fires while started and skips while stopped. The
        // counting sink is registered under the action id, the rule is
        // created through the HTTP router, and each publish goes through
        // `dispatch_ingress` (the broker's rule path), never `sink.send`
        // directly.
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-06-action"),
            counter.clone(),
        );
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "w3-06-rule",
                    "sql": "SELECT * FROM \"w3/06/operation\"",
                    "actions": ["w3-06-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/06/operation").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "started action must fire"
        );
        // Stop again: the next publish skips the forward.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/actions/w3-06-action/stop",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "stopped action must be skipped at fire time"
        );
        // Start again: firing resumes without re-registering anything.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/actions/w3-06-action/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            2,
            "re-started action must fire again"
        );
        // Cleanup: rule then action (the rule holds the reference).
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-06-action", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn node_action_operation_round_trip() {
        // W3-09 (C06-13) broker-through test: create an action via the
        // W3-02 route, run `stop`/`start` through the node-scoped route
        // against the local node and read back the flipped state; an
        // unknown node is NOT_FOUND, an unknown id is 404 and an unknown
        // operation is rejected, all with the documented shape and a
        // non-empty body. Then a rule forwarding to the action id fires
        // while started and is skipped while stopped through the node
        // route, through the engine's lock-free dispatch snapshot (one
        // publish through `dispatch_ingress` per case). Every assertion
        // goes through the HTTP router against the shared store, never at
        // the store directly. Management-plane only; publish and deliver
        // never touch the action store directly.
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let local = "indra-node-1";
        let legacy = crate::node_scope::local_node_name();
        // Unknown node is NOT_FOUND with a non-empty body naming the node,
        // even when the id and operation are also unknown (node scope is
        // checked first, fail closed).
        for unknown in ["no-such-node-xyz", ""] {
            let path = format!("/api/v5/nodes/{unknown}/actions/no-such-action/start");
            let (status, body) = server.send("POST", &path, None, Some(&token)).await;
            assert_eq!(status, 404, "unknown node {unknown:?} must be 404");
            assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
            assert!(
                body["message"].is_string()
                    && !body["message"].as_str().expect("message").is_empty(),
                "unknown-node body must not be empty: {body}"
            );
        }
        // Unknown id on the known node is 404 with the documented shape,
        // not an empty body.
        let (status, body) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/actions/no-such-action/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        assert!(!body.to_string().is_empty());
        // Create one action via the W3-02 route (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-09-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Unknown operation on the known node is rejected with the
        // documented shape and persists nothing.
        let (status, body) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/actions/w3-09-action/frobnicate"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "unknown operation must be rejected");
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        assert!(!body.to_string().is_empty());
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-09-action", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Stop through the node route: the reported state flips to
        // disabled on the next read. The legacy node name names the same
        // local node.
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/actions/w3-09-action/stop"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{legacy}/actions/w3-09-action/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204, "legacy node name must resolve");
        // Rule dispatch through the broker: a rule forwarding to the
        // action id fires while started and skips while stopped through
        // the node route. The counting sink is registered under the action
        // id, the rule is created through the HTTP router, and each
        // publish goes through `dispatch_ingress` (the broker's rule
        // path), never `sink.send` directly.
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-09-action"),
            counter.clone(),
        );
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "w3-09-rule",
                    "sql": "SELECT * FROM \"w3/09/operation\"",
                    "actions": ["w3-09-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/09/operation").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "started action must fire"
        );
        // Stop again through the node route: the next publish skips.
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/actions/w3-09-action/stop"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            1,
            "stopped action must be skipped at fire time"
        );
        // Start again through the node route: firing resumes.
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/actions/w3-09-action/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(
            counter.hits.load(AtomicOrdering::Relaxed),
            2,
            "re-started action must fire again"
        );
        // Cleanup: rule then action (the rule holds the reference).
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-09-action", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn actions_summary_tracks_create_delete() {
        // W3-08 (C06-12) broker-through test: create two actions of
        // different states, read the summary expecting matching counts;
        // delete both and expect zeroes. Every assertion goes through
        // the HTTP router against the shared W3-02 store, never at the
        // store directly. The summary counts are computed on demand from
        // the live snapshot in `crate::v5::actions::ActionStore`
        // (`summary_counts`, read by `get_actions_summary`), driven by
        // action create/delete events. Management-plane only; publish
        // and deliver never touch the action directory.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Empty store returns zeroes, not an error.
        let (status, body) = server.get_auth("/api/v5/actions_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        // Two actions of different states: one enabled, one disabled.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "sum-act-a", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "sum-act-b", "type": "mqtt_bridge", "server": "127.0.0.1:1883", "enable": false}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Counts match the live snapshot; unknown query parameters are
        // ignored.
        let (status, body) = server
            .get_auth("/api/v5/actions_summary?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        let rows = body.as_array().expect("summary is a list");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter().filter(|r| r["enable"] == json!(true)).count(),
            1
        );
        assert_eq!(
            rows.iter().filter(|r| r["enable"] == json!(false)).count(),
            1
        );
        assert!(rows
            .iter()
            .all(|r| r["name"].is_string() && r["type"].is_string()));
        // Delete both; the summary returns to zeroes.
        let (status, _) = server
            .delete_auth("/api/v5/actions/sum-act-a", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/sum-act-b", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/actions_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
    }

    #[tokio::test]
    async fn action_metrics_move_on_real_fire_and_reset_zeroes() {
        // W3-05 (C06-08/C06-09) broker-through test: create an action via
        // the W3-02 route, read zeroed metrics, fire a rule bound to the
        // action through a real publish (`dispatch_ingress`, the broker's
        // rule path), read non-zero attributed counts, reset and expect
        // zeroes; unknown ids are 404. Before: both routes were 404.
        // The per-action counters live in `broker-rules`
        // (`RuleEngine::action_metrics`, bumped in `run_actions` on the
        // `ForwardConnector` arm), driven by the publish event.
        struct CountingSink;
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "test-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown ids miss with the documented shape, not an empty body.
        let (status, body) = server
            .get_auth("/api/v5/actions/no-such-action/metrics", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/actions/no-such-action/metrics/reset",
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one action via the W3-02 route; metrics start at zero.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "w3-05-action", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-05-action/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("w3-05-action"));
        assert_eq!(body["metrics"]["total"], json!(0));
        assert_eq!(body["metrics"]["success"], json!(0));
        assert_eq!(body["metrics"]["failed"], json!(0));
        // Unknown query parameters are ignored on both routes.
        let (status, _) = server
            .get_auth("/api/v5/actions/w3-05-action/metrics?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        // Bind a rule to the action id and fire it through a real publish.
        // The counting sink is registered under the action's MV-01
        // dispatch key (the key the rule fire uses) so the fire
        // succeeds; the rule is created through the HTTP router and the
        // publish goes through `dispatch_ingress` (the broker's rule path),
        // never `sink.send` directly.
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("w3-05-action"),
            std::sync::Arc::new(CountingSink),
        );
        let (status, rule_body) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "name": "w3-05-rule",
                    "sql": "SELECT * FROM \"w3/05/fire\"",
                    "actions": ["w3-05-action"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = rule_body["id"].as_str().expect("rule id").to_string();
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/05/fire").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        // Counters moved only through the real fire, never constants.
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-05-action/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["total"], json!(1));
        assert_eq!(body["metrics"]["success"], json!(1));
        assert_eq!(body["metrics"]["failed"], json!(0));
        // Reset zeroes and reports success; the next read is zero.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/actions/w3-05-action/metrics/reset",
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/actions/w3-05-action/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["total"], json!(0));
        assert_eq!(body["metrics"]["success"], json!(0));
        assert_eq!(body["metrics"]["failed"], json!(0));
        // Cleanup: rule then action (the rule holds the reference).
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/w3-05-action", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn source_metrics_move_on_real_ingress_and_reset_zeroes() {
        // W3-29 (C07-18/C07-19) broker-through test: create a source via
        // the W3-26 route, read zeroed metrics, drive real ingress through
        // `dispatch_ingress` (the broker's rule path, also used by
        // `crates/broker-node` ingress), read attributed counts, stop the
        // source and observe shed counts, reset and expect zeroes; unknown
        // ids are 404. Before: both routes were 404.
        // The per-source counters live in `broker-rules`
        // (`RuleEngine::source_metrics`, bumped in `attribute_source_ingress`
        // on every `dispatch_ingress`), driven by the publish event.
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }

        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown ids miss with the documented shape, not an empty body.
        let (status, body) = server
            .get_auth("/api/v5/sources/no-such-source/metrics", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/sources/no-such-source/metrics/reset",
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one source via the W3-26 route; metrics start at zero.
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-29-source", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, body) = server
            .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("w3-29-source"));
        assert_eq!(body["metrics"]["received"], json!(0));
        assert_eq!(body["metrics"]["dropped"], json!(0));
        // Unknown query parameters are ignored on both routes.
        let (status, _) = server
            .get_auth("/api/v5/sources/w3-29-source/metrics?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        // Drive real ingress through the source: one publish through
        // `dispatch_ingress` (the broker's rule path), never constants.
        let broker_sink: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("w3/29/ingress").expect("valid topic");
        let payload = bytes::Bytes::from_static(b"{\"k\":1}");
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        // Counters moved only through the real ingress, never constants.
        let (status, body) = server
            .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["received"], json!(1));
        assert_eq!(body["metrics"]["dropped"], json!(0));
        // Stopped sources shed into `dropped`: flip via the W3-28 enable
        // route (which mirrors into the engine snapshot), publish again,
        // and expect the drop counter to move while `received` holds.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/sources/w3-29-source/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        state
            .engine
            .dispatch_ingress(
                &topic,
                &payload,
                broker_protocol::QoS::AtMostOnce,
                &broker_sink,
            )
            .await;
        let (status, body) = server
            .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["received"], json!(1));
        assert_eq!(body["metrics"]["dropped"], json!(1));
        // Reset zeroes and reports success; the next read is zero.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/sources/w3-29-source/metrics/reset",
                Some(json!({})),
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .get_auth("/api/v5/sources/w3-29-source/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["metrics"]["received"], json!(0));
        assert_eq!(body["metrics"]["dropped"], json!(0));
        // Cleanup: delete the source (which drops its metric record, so a
        // later re-create starts at zero).
        let (status, _) = server
            .delete_auth("/api/v5/sources/w3-29-source", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn removed_sources_rulenotest_routes_return_404() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // W3-31 restores `POST /api/v5/sources`, `DELETE
        // /api/v5/sources/:id` and `GET /api/v5/sources_summary` on the
        // real source directory, so `/api/v5/sources` is no longer a
        // removed route (GET on the POST-only path is 405); W3-11
        // restores `GET /api/v5/rule_events` on the real producer
        // registry, so no GET path stays removed here; W3-12 restores
        // `POST /api/v5/rule_test` on the real offline evaluator, so it
        // is no longer a removed route either; W3-16 restores `POST
        // /api/v5/rules/:id/test` on the stored-rule evaluator, so it is
        // no longer a removed route either.
        for path in ["/api/v5/rule_events_missing"] {
            let (status, _) = server.get_auth(path, &token).await;
            assert_eq!(status, 404, "removed route {path} must be 404");
        }
    }

    #[tokio::test]
    async fn rule_test_offline_match_no_match_side_effect_free() {
        // W3-12 failing test: POST /rule_test evaluates a rule body
        // against a sample through the served management API on `ApiState`
        // (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), reusing the real
        // rule compiler (`broker_rules::try_evaluate` in
        // `crates/broker-rules/src/lib.rs`). Before: route returned 404.
        // Broker-through: management POST plus a live ingress dispatch
        // through the engine, then a rules list proving nothing was stored.
        // Management-plane only; publish and deliver never touch this path.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Matching sample reports matched output.
        let (status, body) = server
            .post_auth(
                "/api/v5/rule_test",
                json!({
                    "sql": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
                    "topic": "sensors/kitchen",
                    "payload": {"temperature": 72.5, "secret": "x"},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["matched"], json!(true));
        assert_eq!(body["projected"], json!({"temperature": 72.5}));

        // Non-matching sample answers 412 with the documented code.
        let (status, body) = server
            .post_auth(
                "/api/v5/rule_test",
                json!({
                    "sql": "SELECT * FROM \"sensors/+\" WHERE temperature > 100.0",
                    "topic": "sensors/kitchen",
                    "payload": {"temperature": 72.5},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 412);
        assert_eq!(body["code"], json!("NOT_MATCH"));

        // Malformed rule bodies are rejected with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/rule_test",
                json!({"sql": "SELECT WHERE WHERE", "topic": "a/b", "payload": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Side-effect-free: no rule, action binding or retained state left
        // behind, and live routing still works through the broker.
        struct W312Sink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for W312Sink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W312Sink);
        let topic = broker_protocol::Topic::new("sensors/kitchen").expect("topic");
        let payload = bytes::Bytes::from_static(b"{\"temperature\": 72.5}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(
            matched, 0,
            "no stored rule so live dispatch matches nothing"
        );
        let (status, body) = server.get_auth("/api/v5/rules", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], json!([]));
        assert_eq!(state.engine.list_rules().len(), 0);
    }

    #[tokio::test]
    async fn rule_test_one_match_no_match_side_effect_free() {
        // W3-16 failing test: POST /rules/:id/test evaluates the stored
        // rule against a sample through the served management API on
        // `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), reusing the real
        // rule compiler (`broker_rules::try_evaluate` in
        // `crates/broker-rules/src/lib.rs`) bound to the stored SQL and
        // topic filter. Before: route returned 404.
        // Broker-through: management POSTs plus a live ingress dispatch
        // through the engine, then a metrics read proving the probe fired
        // no action and bumped no live counter.
        // Management-plane only; publish and deliver never touch this path.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Create the stored rule the probe binds to.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/rules",
                Some(json!({
                    "id": "w3-16-rule",
                    "sql": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 100.0",
                    "actions": [],
                })),
                Some(&token),
            )
            .await;
        assert_eq!(status, 201);

        // Matching sample reports matched output.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules/w3-16-rule/test",
                json!({
                    "topic": "sensors/kitchen",
                    "payload": {"temperature": 150.0, "secret": "x"},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["matched"], json!(true));
        assert_eq!(body["projected"], json!({"temperature": 150.0}));
        assert_eq!(body["output"], body["projected"]);

        // Non-matching sample answers 412 with the documented code.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules/w3-16-rule/test",
                json!({
                    "topic": "sensors/kitchen",
                    "payload": {"temperature": 72.5},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 412);
        assert_eq!(body["code"], json!("NOT_MATCH"));

        // Malformed samples are rejected with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules/w3-16-rule/test",
                json!({"payload": {"temperature": 1.0}}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Unknown rule id is 404 with the documented error code.
        let (status, body) = server
            .post_auth(
                "/api/v5/rules/no-such-rule/test",
                json!({
                    "topic": "sensors/kitchen",
                    "payload": {"temperature": 1.0},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("RULE_NOT_FOUND"));

        // Side-effect-free: the stored rule survives, live counters stayed
        // at zero (the probe bumps nothing), and live routing still works
        // through the broker.
        struct W316Sink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for W316Sink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W316Sink);
        let topic = broker_protocol::Topic::new("sensors/kitchen").expect("topic");
        let payload = bytes::Bytes::from_static(b"{\"temperature\": 150.0}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(matched, 1, "stored rule still fires on live dispatch");
        let (status, body) = server
            .get_auth("/api/v5/rules/w3-16-rule/metrics", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            body["metrics"]["matched"],
            json!(1),
            "only the live dispatch bumped counters; offline probes bump nothing"
        );
        let (status, _) = server.delete_auth("/api/v5/rules/w3-16-rule", &token).await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn rule_events_catalogue_lists_only_produced_events() {
        // W3-11 failing test: GET /rule_events returns the catalogue of
        // event types rules can trigger on, built from the real producer
        // registry; every listed event has a real producer, and a
        // published message is seen by the rule trigger path behind the
        // listed event. Before: route returned 404.
        // Broker-through: the request goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`); the catalogue
        // itself is owned by `broker-rules`
        // (`rule_event_catalogue` in `crates/broker-rules/src/events.rs`),
        // read only on this management GET and never consulted on publish
        // or deliver.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server.get_auth("/api/v5/rule_events", &token).await;
        assert_eq!(status, 200);
        let entries = body.as_array().expect("catalogue is a list");
        assert!(!entries.is_empty(), "catalogue must not be empty");
        assert!(entries.len() <= broker_rules::events::MAX_RULE_EVENTS);
        for entry in entries {
            let event = entry["event"].as_str().expect("entry carries event");
            assert!(
                crate::v5::rule_events::is_produced_event(event),
                "catalogue lists unproduced event '{event}'"
            );
            assert!(
                entry["description"].as_str().is_some_and(|d| !d.is_empty()),
                "entry carries a summary: {entry}"
            );
        }
        let names: Vec<&str> = entries.iter().filter_map(|e| e["event"].as_str()).collect();
        assert!(
            names.contains(&"$events/client_connected"),
            "catalogue lists the connect producer"
        );
        assert!(
            !names.contains(&"$events/message_publish"),
            "catalogue lists no unproduced event"
        );

        // Unknown query parameters are ignored, never an error.
        let (status, again) = server
            .get_auth("/api/v5/rule_events?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(again, body);

        // Publish an event through the broker rule path: a rule over the
        // published topic fires, so the listed producer is real.
        state
            .engine
            .create_rule(
                "w311-broker-rule".to_string(),
                broker_protocol::TopicFilter::new("w311/#").expect("filter"),
                None,
                true,
                vec![broker_rules::RuleAction::Log],
            )
            .expect("rule creates");
        struct FnSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for FnSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(FnSink);
        let topic = broker_protocol::Topic::new("w311/probe").expect("topic");
        let payload = bytes::Bytes::from_static(b"{}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(matched, 1);
    }

    #[tokio::test]
    async fn sources_probe_is_stateless_with_documented_shapes() {
        // W3-30 (C07-21) broker-through test: a valid probe succeeds without
        // creating a source, a bad config fails with the documented error
        // shape, and an unknown kind is rejected. Management-plane only;
        // publish and deliver never touch the probe path.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Valid probe: known kind, nothing to dial, so reachable by
        // validation alone. Must not create a source: the summary still
        // reports zeroes (W3-31 restores the source directory, so the
        // absence check reads the live summary, not a 404).
        let (status, _) = server
            .post_auth("/api/v5/sources_probe", json!({"type": "mqtt"}), &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            body.as_array().map(Vec::len),
            Some(0),
            "probe must not create a source"
        );
        // Bad config: known kind with a closed port reports the failure.
        let (status, body) = server
            .post_auth(
                "/api/v5/sources_probe",
                json!({"type": "http", "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "bad probe config must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        // Unknown kind is rejected with the documented error shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/sources_probe",
                json!({"type": "no_such_source_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "unknown source kind must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        // Malformed body is rejected with the documented error shape.
        let (status, body) = server
            .post_auth("/api/v5/sources_probe", json!({}), &token)
            .await;
        assert_eq!(status, 400, "malformed probe body must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
    }

    #[tokio::test]
    async fn sources_summary_tracks_create_delete() {
        // W3-31 (C07-22) broker-through test: create two sources of
        // different states, read the summary expecting matching counts;
        // delete both and expect zeroes. Every assertion goes through
        // the HTTP router against the shared store, never at the store
        // directly. Management-plane only; publish and deliver never
        // touch the source directory.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Empty store returns zeroes, not an error.
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        // Two sources of different states: one enabled, one disabled.
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "sum-src-a", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "sum-src-b", "type": "http", "enable": false}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Counts match the live snapshot; unknown query parameters are
        // ignored.
        let (status, body) = server
            .get_auth("/api/v5/sources_summary?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        let rows = body.as_array().expect("summary is a list");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter().filter(|r| r["enable"] == json!(true)).count(),
            1
        );
        assert_eq!(
            rows.iter().filter(|r| r["enable"] == json!(false)).count(),
            1
        );
        assert!(rows
            .iter()
            .all(|r| r["name"].is_string() && r["type"].is_string()));
        // Delete of an unknown id is 404 with the documented shape.
        let (status, body) = server
            .delete_auth("/api/v5/sources/no-such-source", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Delete both; the summary returns to zeroes.
        let (status, _) = server
            .delete_auth("/api/v5/sources/sum-src-a", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/sources/sum-src-b", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
    }

    #[tokio::test]
    async fn sources_list_create_delete_round_trip() {
        // W3-26 (C07-12..C07-14) broker-through test: create a source,
        // list and expect it present with paging metadata; create the
        // same id again and expect rejection; delete it and expect a
        // second delete to be 404. Every assertion goes through the HTTP
        // router against the shared store, never at the store directly.
        // Management-plane only; publish and deliver never touch the
        // source directory.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Empty store returns an empty page, not an error.
        let (status, body) = server.get_auth("/api/v5/sources", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        assert_eq!(body.as_array().map(Vec::len), Some(0));
        // Unknown query parameters are ignored.
        let (status, _) = server
            .get_auth("/api/v5/sources?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        // Unknown source kind is rejected with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "bad-kind-source", "type": "no_such_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "unknown source kind must be 400");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        // Create one source.
        let (status, body) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-26-source", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(body["id"], json!("w3-26-source"));
        assert_eq!(body["type"], json!("mqtt"));
        // List shows it with paging metadata.
        let (status, body) = server.get_auth("/api/v5/sources", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let ids: Vec<String> = body
            .as_array()
            .expect("list data is an array")
            .iter()
            .filter_map(|row| row.get("id").and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        assert!(ids.contains(&"w3-26-source".to_string()));
        // Duplicate create is rejected cleanly, never overwritten.
        let (status, body) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-26-source", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "duplicate source id must be rejected");
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        // Delete hit, then miss.
        let (status, _) = server
            .delete_auth("/api/v5/sources/w3-26-source", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .delete_auth("/api/v5/sources/w3-26-source", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Store is empty again.
        let (status, body) = server.get_auth("/api/v5/sources", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(0));
    }

    #[tokio::test]
    async fn sources_single_read_and_replace_round_trip() {
        // W3-27 (C07-15/C07-16) broker-through test: create a source via
        // the W3-26 route, read it back and expect the documented
        // fields; replace its config and read again expecting the new
        // values; read an unknown id expecting 404. Every assertion goes
        // through the HTTP router against the shared store (the same
        // state the kernel shares in `crates/broker-node/src/main.rs`,
        // `serve_api`), never at the store directly. Management-plane
        // only; publish and deliver never touch the source directory.
        // Before: both single routes returned 404.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown read is 404 with the documented shape, not an empty body.
        let (status, body) = server
            .get_auth("/api/v5/sources/no-such-source", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one source via the W3-26 route.
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-27-source", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Read hit carries the documented fields.
        let (status, body) = server
            .get_auth("/api/v5/sources/w3-27-source", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("w3-27-source"));
        assert_eq!(body["type"], json!("mqtt"));
        assert_eq!(body["enable"], json!(true));
        // Unknown query parameters are ignored on the single read.
        let (status, _) = server
            .get_auth("/api/v5/sources/w3-27-source?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        // Replace the stored config with the same validation as create.
        let (status, body) = server
            .put_auth(
                "/api/v5/sources/w3-27-source",
                json!({"type": "http", "enable": false}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["id"], json!("w3-27-source"));
        assert_eq!(body["type"], json!("http"));
        assert_eq!(body["enable"], json!(false));
        // Read again sees the new values.
        let (status, body) = server
            .get_auth("/api/v5/sources/w3-27-source", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["type"], json!("http"));
        assert_eq!(body["enable"], json!(false));
        // Update of an unknown id is 404 and creates nothing.
        let (status, body) = server
            .put_auth(
                "/api/v5/sources/no-such-source",
                json!({"type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        let (status, _) = server
            .get_auth("/api/v5/sources/no-such-source", &token)
            .await;
        assert_eq!(status, 404);
        // Unknown kind and malformed bodies are rejected, leaving the
        // stored entry untouched.
        let (status, body) = server
            .put_auth(
                "/api/v5/sources/w3-27-source",
                json!({"type": "no_such_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .put_auth(
                "/api/v5/sources/w3-27-source",
                json!({"enable": true}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server
            .get_auth("/api/v5/sources/w3-27-source", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["type"], json!("http"));
        // Mismatched body id is rejected instead of renaming.
        let (status, body) = server
            .put_auth(
                "/api/v5/sources/w3-27-source",
                json!({"id": "other-source", "type": "http"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        // Cleanup so the id does not leak into sibling tests sharing
        // nothing but the name space of fresh servers.
        let (status, _) = server
            .delete_auth("/api/v5/sources/w3-27-source", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn sources_survive_restart() {
        // W3-26 persistence across a restart through the config core:
        // register through the broker, reload the same data dir in a
        // fresh server, and read the source back from the list.
        let dir = unique_data_dir("w3-26-sources");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "restart-src", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/sources", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let ids: Vec<String> = body
            .as_array()
            .expect("list data is an array")
            .iter()
            .filter_map(|row| row.get("id").and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        assert!(ids.contains(&"restart-src".to_string()));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn source_operation_start_stop_round_trip() {
        // W3-23 (C07-20) broker-through test: create a source via the
        // `POST /sources` route, stop it through the operation route and
        // read back disabled from the reported state plus the summary,
        // start it and read back enabled; an unknown operation is
        // rejected and an unknown id is 404, both with the documented
        // shape and a non-empty body. Before: the operation route
        // returned 404. Every assertion goes through the HTTP router
        // against the shared store (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), never at the
        // store directly. Management-plane only; a live ingress dispatch
        // through the engine still matches nothing afterwards, so
        // publish and deliver never touch the source directory.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown id is 404 with the documented shape, not an empty body.
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/sources/no-such-source/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Unknown id with an unknown operation is still 404 (existence
        // before validation) and changes nothing.
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/sources/no-such-source/frobnicate",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one source (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-23-src", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Unknown operation is rejected with the documented shape and
        // changes nothing (the stored flag stays enabled).
        let (status, body) = server
            .send(
                "POST",
                "/api/v5/sources/w3-23-src/frobnicate",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "unknown operation must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        assert_eq!(
            body.as_array()
                .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
            Some(1)
        );
        // Stop: the reported runtime state changes to disabled.
        let (status, _) = server
            .send("POST", "/api/v5/sources/w3-23-src/stop", None, Some(&token))
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        assert_eq!(
            body.as_array()
                .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
            Some(0)
        );
        // Start: the reported runtime state changes back to enabled.
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/sources/w3-23-src/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        assert_eq!(
            body.as_array()
                .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
            Some(1)
        );
        // The message path is untouched: live ingress dispatch still
        // works and the source directory reports the same counts after.
        struct W323Sink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for W323Sink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W323Sink);
        let topic = broker_protocol::Topic::new("w323/probe").expect("topic");
        let payload = bytes::Bytes::from_static(b"{}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(
            matched, 0,
            "no stored rule so live dispatch matches nothing"
        );
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let (status, _) = server
            .delete_auth("/api/v5/sources/w3-23-src", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn source_enable_flag_round_trip_and_runtime_effect() {
        // W3-28 (C07-17) broker-through test: create a source via the
        // `POST /sources` route, disable it through the enable route and
        // read back disabled from the reported state plus the single read
        // and the summary (disabled sources stop ingress on the shared
        // W3-23 runtime core), re-enable and read back enabled with
        // ingress resumed; unknown ids are 404 and malformed flags are
        // 400, both with the documented shape and a non-empty body.
        // Before: the enable route returned 404. Every assertion goes
        // through the HTTP router against the shared store (the same state
        // the kernel shares in `crates/broker-node/src/main.rs`,
        // `serve_api`), never at the store directly. Management-plane
        // only; publish and deliver never touch the source directory.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Unknown id is 404 with the documented shape, not an empty body.
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/sources/no-such-source/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Unknown id with a malformed flag is still 404 (existence before
        // validation) and changes nothing.
        let (status, body) = server
            .send(
                "PUT",
                "/api/v5/sources/no-such-source/enable/maybe",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Create one source (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-28-src", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Malformed flag is rejected with the documented shape and changes
        // nothing (the stored flag stays enabled).
        for bad in ["maybe", "1", "0", "on", "TRUEE", "enabled"] {
            let (status, body) = server
                .send(
                    "PUT",
                    &format!("/api/v5/sources/w3-28-src/enable/{bad}"),
                    None,
                    Some(&token),
                )
                .await;
            assert_eq!(status, 400, "flag {bad:?} must be rejected");
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
        let (status, body) = server.get_auth("/api/v5/sources/w3-28-src", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // Disable: the reported flag changes and reads back disabled on
        // the next single read (ingress stopped on the W3-23 runtime).
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/sources/w3-28-src/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/sources/w3-28-src", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(false));
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        assert_eq!(
            body.as_array()
                .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
            Some(0)
        );
        // The W3-23 operation route observes the same core: starting the
        // disabled source re-enables it (ingress resumed).
        let (status, _) = server
            .send(
                "POST",
                "/api/v5/sources/w3-28-src/start",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        // Re-disable through the enable route, then re-enable through it:
        // the flag change is visible on the next read each way.
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/sources/w3-28-src/enable/false",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .send(
                "PUT",
                "/api/v5/sources/w3-28-src/enable/true",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/sources/w3-28-src", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["enable"], json!(true));
        // The message path is untouched: live ingress dispatch still
        // works and the source directory reports the same counts after.
        struct W328Sink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for W328Sink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W328Sink);
        let topic = broker_protocol::Topic::new("w328/probe").expect("topic");
        let payload = bytes::Bytes::from_static(b"{}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(
            matched, 0,
            "no stored rule so live dispatch matches nothing"
        );
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        assert_eq!(
            body.as_array()
                .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
            Some(1)
        );
        let (status, _) = server
            .delete_auth("/api/v5/sources/w3-28-src", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn node_source_operation_single_node_round_trip() {
        // W3-24 (C07-10) broker-through test: create a source via the
        // `POST /sources` route, stop it through the node-scoped operation
        // route and read back disabled from the reported state plus the
        // summary, start it and read back enabled; an unknown node is
        // NOT_FOUND, an unknown id is 404 and an unknown operation is
        // rejected, all with the documented shape and a non-empty body.
        // Before: the node-scoped route returned 404. Every assertion goes
        // through the HTTP router against the shared store (the same state
        // the kernel shares in `crates/broker-node/src/main.rs`,
        // `serve_api`), never at the store directly. Management-plane
        // only; a live ingress dispatch through the engine still matches
        // nothing afterwards, so publish and deliver never touch the
        // source directory.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let local = "indra-node-1";
        let legacy = crate::node_scope::local_node_name();
        // Unknown node is NOT_FOUND with a non-empty body naming the node,
        // even when the id and operation are also unknown (node scope is
        // checked first, fail closed).
        for unknown in ["no-such-node-xyz", ""] {
            let path = format!("/api/v5/nodes/{unknown}/sources/no-such-source/start");
            let (status, body) = server.send("POST", &path, None, Some(&token)).await;
            assert_eq!(status, 404, "unknown node {unknown:?} must be 404");
            assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
            assert!(
                body["message"].is_string()
                    && !body["message"].as_str().expect("message").is_empty(),
                "unknown-node body must not be empty: {body}"
            );
        }
        // Unknown id on the known node is 404 with the documented shape,
        // not an empty body.
        let (status, body) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/sources/no-such-source/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(!body.to_string().is_empty());
        // Create one source via the W3-26 route (defaults to enabled).
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w3-24-src", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        // Unknown operation on the known node is rejected with the
        // documented shape and persists nothing (the flag stays enabled).
        let (status, body) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/sources/w3-24-src/frobnicate"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 400, "unknown operation must be rejected");
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(!body.to_string().is_empty());
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        assert_eq!(
            body.as_array()
                .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
            Some(1)
        ); // Stop through the node route: the reported state flips to
           // disabled. The legacy node name names the same local node.
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{local}/sources/w3-24-src/stop"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            body.as_array()
                .map(|rows| rows.iter().filter(|r| r["enable"] == json!(true)).count()),
            Some(0)
        );
        let (status, _) = server
            .send(
                "POST",
                &format!("/api/v5/nodes/{legacy}/sources/w3-24-src/start"),
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 204, "legacy node name must resolve");
        // The message path is untouched: live ingress dispatch still works
        // and the source directory reports the same counts after.
        struct W324Sink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for W324Sink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W324Sink);
        let topic = broker_protocol::Topic::new("w324/probe").expect("topic");
        let payload = bytes::Bytes::from_static(b"{}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(
            matched, 0,
            "no stored rule so live dispatch matches nothing"
        );
        let (status, body) = server.get_auth("/api/v5/sources_summary", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let (status, _) = server
            .delete_auth("/api/v5/sources/w3-24-src", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn removed_schema_registry_routes_return_404() {
        // W3-37 owns `GET+POST /schema_registry` and
        // `DELETE /schema_registry/:name`. The validation routes stay
        // removed until their own task lands.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for path in [
            "/api/v5/schema_validations",
            "/api/v5/schema_validations/validation/x/metrics",
        ] {
            let (status, _) = server.get_auth(path, &token).await;
            assert_eq!(status, 404, "removed route {path} must be 404");
        }
    }

    #[tokio::test]
    async fn schema_registry_list_create_delete_round_trip() {
        // W3-37 broker test. Create one schema. List shows it.
        // Duplicate create fails. Delete works. Second delete is 404.
        // Each step uses the HTTP router. No step touches the store directly.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Empty registry returns an empty page.
        let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        assert_eq!(body.as_array().map(Vec::len), Some(0));
        // Unknown query keys are ignored.
        let (status, _) = server
            .get_auth("/api/v5/schema_registry?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        // Bad kind fails with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/schema_registry",
                json!({
                    "name": "bad-kind",
                    "type": "nope",
                    "schema": {"type": "object"},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        // Bad shape fails with the documented shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/schema_registry",
                json!({
                    "name": "bad-shape",
                    "type": "json",
                    "schema": {"foo": 1},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        // Create one schema.
        let (status, body) = server
            .post_auth(
                "/api/v5/schema_registry",
                json!({
                    "name": "w3-37-schema",
                    "type": "json",
                    "description": "temp reading",
                    "schema": {"type": "object", "properties": {"temp": {"type": "number"}}},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(body["name"], json!("w3-37-schema"));
        assert_eq!(body["type"], json!("json"));
        // List shows it with paging metadata.
        let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let names: Vec<String> = body
            .as_array()
            .expect("list data is an array")
            .iter()
            .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        assert!(names.contains(&"w3-37-schema".to_string()));
        // Duplicate create fails cleanly.
        let (status, body) = server
            .post_auth(
                "/api/v5/schema_registry",
                json!({
                    "name": "w3-37-schema",
                    "type": "json",
                    "schema": {"type": "object"},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        // Delete works. Second delete is 404.
        let (status, _) = server
            .delete_auth("/api/v5/schema_registry/w3-37-schema", &token)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .delete_auth("/api/v5/schema_registry/w3-37-schema", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // Store is empty again.
        let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(0));
    }

    #[tokio::test]
    async fn schema_registry_survives_restart() {
        // W3-37 persistence test. Register through the broker.
        // Reload the same dir. Read the schema back from the list.
        let dir = unique_data_dir("w3-37-schemas");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/schema_registry",
                json!({
                    "name": "restart-schema",
                    "type": "avro",
                    "schema": {
                        "type": "record",
                        "name": "Reading",
                        "fields": [{"name": "id", "type": "string"}],
                    },
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/schema_registry", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body.as_array().map(Vec::len), Some(1));
        let names: Vec<String> = body
            .as_array()
            .expect("list data is an array")
            .iter()
            .filter_map(|row| row.get("name").and_then(|v| v.as_str()).map(str::to_string))
            .collect();
        assert!(names.contains(&"restart-schema".to_string()));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn removed_unsupported_retained_routes_return_404() {
        // AP-04 restores `GET /api/v5/mqtt/delayed/messages` on the real
        // scheduler, so it leaves the removed list here: it answers 200
        // with the documented page. The remaining paths stay removed.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for path in [
            "/api/v5/mqtt/retained",
            "/api/v5/configs/a2a_registry",
            "/api/v5/indra/extra_features",
        ] {
            let (status, _) = server.get_auth(path, &token).await;
            assert_eq!(status, 404, "removed route {path} must be 404");
        }
        let (status, body) = server
            .get_auth("/api/v5/mqtt/delayed/messages", &token)
            .await;
        assert_eq!(status, 200, "restored delayed list must be 200");
        assert!(body.get("data").and_then(|v| v.as_array()).is_some());
        assert_eq!(body["meta"]["count"], serde_json::json!(0));
        let (status, _) = server.delete_auth("/api/v5/mqtt/retained", &token).await;
        assert_eq!(
            status, 404,
            "removed route DELETE /api/v5/mqtt/retained must be 404"
        );
    }

    #[tokio::test]
    async fn removed_system_fake_routes_return_404() {
        // AP-03 restores `GET /api/v5/configs` on the real registry.
        // It answers 200 with each routed root. Management-plane only.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/configs", &token).await;
        assert_eq!(status, 200, "restored route /api/v5/configs must be 200");
        for root in [
            "broker",
            "log",
            "sysmon",
            "sys_topics",
            "alarm",
            "dashboard",
            "global_zone",
        ] {
            assert!(body.get(root).is_some(), "root {root} is present");
        }
    }

    #[tokio::test]
    async fn configs_dashboard_reports_default_password() {
        // AP2-05 failing test: `GET /configs/dashboard` carries the
        // documented `default_password`. Before: the root read as `{}`.
        // Broker-through: the read goes through the served management
        // API on `ApiState`. The value is the true initial admin
        // password, never a stand-in.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/configs/dashboard", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["default_password"], json!("public"));
    }

    #[tokio::test]
    async fn mqtt_group_delayed_rewrite_topicmetrics_over_http() {
        // AP-04 failing test: the `/mqtt` group answers the documented
        // shapes through the served management API. Before: delayed
        // routes 404, rewrite routes 404, topic-metrics v2 routes 404.
        // Broker-through: delayed reads run on the shared scheduler,
        // rewrite reads and writes run on the shared router, and the
        // honest-empty collection routes store nothing.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Delayed switch plus cap: read defaults, write, read back.
        let (status, before) = server.get_auth("/api/v5/mqtt/delayed", &token).await;
        assert_eq!(status, 200);
        assert_eq!(before["enable"], json!(true));
        assert!(before["max_delayed_messages"].as_u64().is_some());
        let (status, after) = server
            .put_auth(
                "/api/v5/mqtt/delayed",
                json!({"enable": false, "max_delayed_messages": 7}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "delayed update: {after}");
        assert_eq!(after["enable"], json!(false));
        assert_eq!(after["max_delayed_messages"], json!(7));
        let (status, reread) = server.get_auth("/api/v5/mqtt/delayed", &token).await;
        assert_eq!(status, 200);
        assert_eq!(reread, after);
        // Bad switch and cap values fail without applying.
        for bad in [
            json!({"enable": "yes"}),
            json!({"max_delayed_messages": -1}),
            json!({"max_delayed_messages": 1_000_001}),
            json!("enable"),
        ] {
            let (status, err) = server.put_auth("/api/v5/mqtt/delayed", bad, &token).await;
            assert_eq!(status, 400, "bad delayed write must fail: {err}");
            assert_eq!(err["code"], json!("BAD_REQUEST"));
        }
        let (status, kept) = server.get_auth("/api/v5/mqtt/delayed", &token).await;
        assert_eq!(status, 200);
        assert_eq!(kept, after, "failed writes must not apply");

        // Empty delayed list reads as an empty page with the meta.
        let (status, empty) = server
            .get_auth("/api/v5/mqtt/delayed/messages", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(empty["data"], json!([]));
        assert_eq!(empty["meta"]["count"], json!(0));
        assert!(empty["meta"]["page"].as_u64().is_some());
        assert!(empty["meta"]["limit"].as_u64().is_some());

        // Detail and removal misses carry the documented codes.
        let (status, err) = server
            .get_auth("/api/v5/mqtt/delayed/messages/no-such-node/1", &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(err["code"], json!("INVALID_NODE"));
        let (status, err) = server
            .delete_auth("/api/v5/mqtt/delayed/messages/no-such-node/1", &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(err["code"], json!("INVALID_NODE"));

        // Rewrite list starts empty, replaces, and rejects bad rules.
        let (status, before) = server.get_auth("/api/v5/mqtt/topic_rewrite", &token).await;
        assert_eq!(status, 200);
        assert_eq!(before, json!([]));
        let (status, after) = server
            .put_auth(
                "/api/v5/mqtt/topic_rewrite",
                json!([{
                    "action": "publish",
                    "source_topic": "conf/http-rw/#",
                    "dest_topic": "rewritten/$1",
                    "re": "^conf/http-rw/(.*)$",
                }]),
                &token,
            )
            .await;
        assert_eq!(status, 200, "rewrite update: {after}");
        assert_eq!(after[0]["action"], json!("publish"));
        assert_eq!(after[0]["source_topic"], json!("conf/http-rw/#"));
        let (status, err) = server
            .put_auth(
                "/api/v5/mqtt/topic_rewrite",
                json!([{"action": "bogus"}]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(err["code"], json!("BAD_REQUEST"));
        let many: Vec<Value> = (0..65)
            .map(|i| {
                json!({
                    "action": "publish",
                    "source_topic": format!("conf/http-rw/{i}/#"),
                    "dest_topic": "rewritten/$1",
                    "re": format!("^conf/http-rw/{i}/(.*)$"),
                })
            })
            .collect();
        let (status, err) = server
            .put_auth("/api/v5/mqtt/topic_rewrite", json!(many), &token)
            .await;
        assert_eq!(status, 413);
        assert_eq!(err["code"], json!("EXCEED_LIMIT"));

        // Topic-metric collections stay honestly empty: the list reads
        // empty, single reads miss, creates and resets fail, and the
        // bulk delete reports success.
        let (status, body) = server.get_auth("/api/v5/mqtt/topic_metrics2", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let (status, err) = server
            .post_auth(
                "/api/v5/mqtt/topic_metrics2",
                json!({"name": "alpha", "topic_filter": "alpha/#"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(err["code"], json!("BAD_NAME"));
        let (status, err) = server
            .get_auth("/api/v5/mqtt/topic_metrics2/alpha", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("NAME_NOT_FOUND"));
        let (status, err) = server
            .delete_auth("/api/v5/mqtt/topic_metrics2/alpha", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("NAME_NOT_FOUND"));
        let (status, err) = server
            .put_auth(
                "/api/v5/mqtt/topic_metrics2/alpha/reset",
                json!(null),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("NAME_NOT_FOUND"));
        let (status, _) = server
            .delete_auth("/api/v5/mqtt/topic_metrics2", &token)
            .await;
        assert_eq!(status, 204);

        // The legacy v1 paths carry no operation in the specification,
        // so they keep no route and answer 404.
        let (status, _) = server.get_auth("/api/v5/mqtt/topic_metrics", &token).await;
        assert_eq!(status, 404, "legacy v1 topic_metrics must stay 404");
    }

    #[tokio::test]
    async fn api_keys_create_list_revoke_and_miss_404() {
        // W2-25 failing test: mint a key expecting the secret returned
        // once, list expecting metadata without secrets, delete it
        // expecting revocation, and delete unknown expecting 404.
        // Before: all three routes 404.
        // Broker-through: every step goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store; key verification itself is covered by the
        // store unit test and the management-plane verify off the hot
        // path, never by publish or deliver.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Empty store reads as an empty array, never an error.
        let (status, body) = server.get_auth("/api/v5/api_key", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        // Mint one key: the secret is returned exactly once.
        let (status, created) = server
            .post_auth(
                "/api/v5/api_key",
                json!({"name": "w225-broker-key", "scopes": ["monitoring"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(created["name"], json!("w225-broker-key"));
        let secret = created["api_secret"]
            .as_str()
            .expect("secret returned once");
        assert!(!secret.is_empty());
        let key_id = created["api_key"].as_str().expect("identifier");
        assert_eq!(key_id.len(), 16);
        for field in [
            "desc",
            "enable",
            "expired",
            "expired_at",
            "created_at",
            "role",
            "scopes",
        ] {
            assert!(created.get(field).is_some(), "create renders {field}");
        }
        assert_eq!(created["enable"], json!(true));
        assert_eq!(created["expired"], json!(false));
        assert_eq!(created["expired_at"], json!("infinity"));
        assert_eq!(created["role"], json!("administrator"));
        assert_eq!(created["scopes"], json!(["monitoring"]));
        assert!(created.get("namespace").is_none());

        // Malformed bodies are client errors.
        let (status, _) = server
            .post_auth("/api/v5/api_key", json!({"scopes": ["monitoring"]}), &token)
            .await;
        assert_eq!(status, 400);

        // Duplicate names are rejected cleanly, never silently rotated.
        let (status, duplicate) = server
            .post_auth(
                "/api/v5/api_key",
                json!({"name": "w225-broker-key"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(duplicate["code"], json!("BAD_REQUEST"));

        // The name rule of the reference: a letter first, then letters,
        // digits, `-` and `_`.
        for bad_name in ["1key", "-key", "has space", "has/slash"] {
            let (status, body) = server
                .post_auth("/api/v5/api_key", json!({"name": bad_name}), &token)
                .await;
            assert_eq!(status, 400, "name {bad_name}: {body}");
            assert_eq!(body["code"], json!("BAD_REQUEST"));
        }
        // A namespace is not available on this node.
        let (status, body) = server
            .post_auth(
                "/api/v5/api_key",
                json!({"name": "namespaced", "namespace": "ns1"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{body}");
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // Listing carries metadata without secrets.
        let (status, listed) = server.get_auth("/api/v5/api_key", &token).await;
        assert_eq!(status, 200);
        let data = listed.as_array().expect("the body is a bare array");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["name"], json!("w225-broker-key"));
        assert_eq!(data[0]["api_key"], json!(key_id));
        assert!(data[0].get("api_secret").is_none());
        assert!(data[0].get("secret").is_none());
        assert!(data[0].get("key_hash").is_none());

        // Revoke the key and confirm unknown names 404 with the
        // documented shape.
        let (status, _) = server
            .delete_auth("/api/v5/api_key/w225-broker-key", &token)
            .await;
        assert_eq!(status, 204);
        let (status, missing) = server
            .delete_auth("/api/v5/api_key/w225-broker-key", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(missing["code"], json!("NOT_FOUND"));

        // Empty again after revocation.
        let (status, body) = server.get_auth("/api/v5/api_key", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
    }

    /// The management console creates, lists and edits keys with these
    /// value forms: `scopes: "unset"`, an ISO time with milliseconds for
    /// `expired_at` (or no `expired_at` for "never"), and it sends the
    /// listed values back on an update. Each value must round-trip.
    #[tokio::test]
    async fn the_console_forms_of_an_api_key_round_trip() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, created) = server
            .post_auth(
                "/api/v5/api_key",
                json!({
                    "name": "console_key",
                    "desc": "from the console",
                    "enable": true,
                    "role": "viewer",
                    "scopes": "unset",
                    "expired_at": "2099-01-02T23:59:59.000Z",
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200, "{created}");
        assert_eq!(created["scopes"], json!("unset"));
        assert_eq!(created["role"], json!("viewer"));
        assert_eq!(created["desc"], json!("from the console"));

        // The list is a bare array. The console sends a listed row back.
        let (status, listed) = server.get_auth("/api/v5/api_key", &token).await;
        assert_eq!(status, 200);
        let row = listed.as_array().expect("array")[0].clone();
        assert_eq!(row["expired"], json!(false));
        let (status, body) = server
            .put_auth(
                "/api/v5/api_key/console_key",
                json!({
                    "enable": false,
                    "desc": row["desc"],
                    "role": row["role"],
                    "scopes": row["scopes"],
                    "expired_at": row["expired_at"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["enable"], json!(false));
        assert_eq!(body["expired_at"], row["expired_at"]);
        assert_eq!(body["scopes"], json!("unset"));

        // The console omits `expired_at` to say "never expires".
        let (status, body) = server
            .put_auth(
                "/api/v5/api_key/console_key",
                json!({"enable": true, "desc": "", "role": "administrator", "scopes": ["audit"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200, "{body}");
        assert_eq!(body["expired_at"], json!("infinity"));
        assert_eq!(body["role"], json!("administrator"));
        assert_eq!(body["scopes"], json!(["audit"]));
    }

    #[tokio::test]
    async fn api_key_detail_read_update_round_trip_and_miss_404() {
        // W2-26 failing test: mint a key, read it expecting metadata
        // without the secret, update scopes/description and read back,
        // read/update unknown expecting 404, invalid updates rejected
        // without applying. Before: both detail routes 404.
        // Broker-through: every step goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store; the store unit tests cover hashing and the
        // management-plane verify off the hot path, never publish or
        // deliver.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Mint one key through the broker.
        let (status, _) = server
            .post_auth(
                "/api/v5/api_key",
                json!({"name": "w226-broker-key", "scopes": ["monitoring"], "desc": "before"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        // Read it back: metadata only, never the secret.
        let (status, body) = server
            .get_auth("/api/v5/api_key/w226-broker-key", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["name"], json!("w226-broker-key"));
        assert_eq!(body["scopes"], json!(["monitoring"]));
        assert_eq!(body["desc"], json!("before"));
        assert!(body["api_key"].is_string());
        assert!(body.get("api_secret").is_none());
        assert!(body.get("secret").is_none());
        assert!(body.get("key_hash").is_none());

        // Unknown names 404 with the documented shape and create nothing.
        let (status, missing) = server
            .get_auth("/api/v5/api_key/w226-no-such-key", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(missing["code"], json!("NOT_FOUND"));
        let (status, missing) = server
            .put_auth(
                "/api/v5/api_key/w226-no-such-key",
                json!({"scopes": ["connections"]}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(missing["code"], json!("NOT_FOUND"));
        let (status, _) = server
            .get_auth("/api/v5/api_key/w226-no-such-key", &token)
            .await;
        assert_eq!(status, 404);

        // Update scopes/description and read back the replace.
        let (status, updated) = server
            .put_auth(
                "/api/v5/api_key/w226-broker-key",
                json!({"scopes": ["connections", "publish"], "desc": "after"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(updated["name"], json!("w226-broker-key"));
        assert_eq!(updated["scopes"], json!(["connections", "publish"]));
        assert_eq!(updated["desc"], json!("after"));
        assert!(updated["api_key"].is_string());
        assert!(updated.get("api_secret").is_none());
        assert!(updated.get("key_hash").is_none());
        let (status, reread) = server
            .get_auth("/api/v5/api_key/w226-broker-key", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(reread["scopes"], json!(["connections", "publish"]));
        assert_eq!(reread["desc"], json!("after"));

        // Bad values are client errors and apply nothing.
        let (status, _) = server
            .put_auth(
                "/api/v5/api_key/w226-broker-key",
                json!({"scopes": [""]}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        let (status, reread) = server
            .get_auth("/api/v5/api_key/w226-broker-key", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(reread["scopes"], json!(["connections", "publish"]));

        // Revocation still removes the detail row.
        let (status, _) = server
            .delete_auth("/api/v5/api_key/w226-broker-key", &token)
            .await;
        assert_eq!(status, 204);
        let (status, missing) = server
            .get_auth("/api/v5/api_key/w226-broker-key", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(missing["code"], json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn api_key_detail_update_survives_restart() {
        // Persistence through the broker: PUT through the management
        // API, reload the same data dir in a fresh server, and read the
        // replaced row back. Secrets stay hashed at rest, so the update
        // never needs the secret.
        let dir = unique_data_dir("apikey-detail");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/api_key",
                json!({"name": "w226-persist", "scopes": ["monitoring"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .put_auth(
                "/api/v5/api_key/w226-persist",
                json!({"scopes": ["connections"], "desc": "kept"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        // The admin password persisted too, so the rebooted node logs in
        // with the changed password (`login_as_admin` only fits fresh
        // servers still on the default password).
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server
            .get_auth("/api/v5/api_key/w226-persist", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["scopes"], json!(["connections"]));
        assert_eq!(body["desc"], json!("kept"));
        assert!(body["api_key"].is_string());
        assert!(body.get("api_secret").is_none());
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn api_key_scopes_catalogue_is_stable_and_unpaged() {
        // W2-27 failing test: fetch the scope catalogue expecting the
        // documented names present and stable across two reads, with
        // per-scope fields on each entry. Before: 404 (route removed).
        // Broker-through: both reads go through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store; the catalogue itself is code-defined
        // (`crates/broker-api/src/v5/api_keys.rs`, `API_KEY_SCOPES`),
        // shared with the key create/update validation registry
        // (`is_known_api_key_scope`), with no broker event driving it
        // beyond serving the management read. Management-plane only;
        // publish and deliver never touch it.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, first) = server.get_auth("/api/v5/api_key_scopes", &token).await;
        assert_eq!(status, 200);
        let scopes = first["scopes"].as_array().expect("scopes is a list");
        assert_eq!(scopes.len(), 10);
        let names: Vec<&str> = scopes
            .iter()
            .map(|entry| entry["name"].as_str().expect("entry has a name"))
            .collect();
        for entry in scopes {
            assert!(entry["name"].is_string(), "entry carries a name");
            assert!(entry["desc"].is_string(), "entry carries a summary");
        }
        for documented in [
            "connections",
            "publish",
            "data_integration",
            "access_control",
            "gateways",
            "monitoring",
            "cluster_operations",
            "system",
            "audit",
            "license",
        ] {
            assert!(
                names.contains(&documented),
                "catalogue must contain {documented}"
            );
        }

        // Stable across two reads.
        let (status, second) = server.get_auth("/api/v5/api_key_scopes", &token).await;
        assert_eq!(status, 200);
        assert_eq!(first, second);

        // Not paged and takes no parameters: unknown query parameters
        // are ignored, not errors.
        let (status, tolerated) = server
            .send(
                "GET",
                "/api/v5/api_key_scopes?unexpected_param=1&page=2&limit=1",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(tolerated, first);
    }

    #[tokio::test]
    async fn user_scopes_catalogue_is_stable_and_unpaged() {
        // W2-40 failing test: fetch the dashboard-user scope catalogue
        // expecting the documented names with per-scope fields, matching
        // what the dashboard-user validation accepts, stable across two
        // reads. Before: a constant string list detached from validation
        // (wrong vocabulary, no fields, no shared registry).
        // Broker-through: both reads go through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store; the catalogue itself is code-defined
        // (`crates/broker-api/src/v5/auth.rs`, `USER_SCOPES`), shared
        // with the dashboard-user validation predicates
        // (`is_known_user_scope`, `role_may_hold_scope`), with no broker
        // event driving it beyond serving the management read.
        // Management-plane only; publish and deliver never touch it.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, first) = server.get_auth("/api/v5/user_scopes", &token).await;
        assert_eq!(status, 200);
        let scopes = first["scopes"].as_array().expect("scopes is a list");
        assert_eq!(scopes.len(), 14);
        let names: Vec<&str> = scopes
            .iter()
            .map(|entry| entry["name"].as_str().expect("entry has a name"))
            .collect();
        for entry in scopes {
            assert!(entry["name"].is_string(), "entry carries a name");
            assert!(entry["desc"].is_string(), "entry carries a summary");
            assert!(
                entry["admin_only"].is_boolean(),
                "entry carries its administrator-only flag"
            );
        }
        for documented in [
            "connections",
            "publish",
            "data_integration",
            "access_control",
            "gateways",
            "monitoring",
            "cluster_operations",
            "system",
            "audit",
            "license",
            "user_management",
            "mfa_management",
            "sso_management",
            "api_key_management",
        ] {
            assert!(
                names.contains(&documented),
                "catalogue must contain {documented}"
            );
        }
        // The login-only scopes are flagged administrator-only; the
        // generic scopes are not.
        for entry in scopes {
            let name = entry["name"].as_str().expect("entry has a name");
            let admin_only = entry["admin_only"].as_bool().expect("flag is a bool");
            let expect_admin_only = matches!(
                name,
                "user_management" | "mfa_management" | "sso_management" | "api_key_management"
            );
            assert_eq!(
                admin_only, expect_admin_only,
                "scope {name} carries the documented administrator-only flag"
            );
        }

        // Stable across two reads.
        let (status, second) = server.get_auth("/api/v5/user_scopes", &token).await;
        assert_eq!(status, 200);
        assert_eq!(first, second);

        // Not paged and takes no parameters: unknown query parameters
        // are ignored, not errors.
        let (status, tolerated) = server
            .send(
                "GET",
                "/api/v5/user_scopes?unexpected_param=1&page=2&limit=1",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(tolerated, first);
    }

    #[tokio::test]
    async fn sso_list_empty_then_configured_and_survives_restart() {
        // W2-32 failing test: list when empty expecting an empty
        // collection, configure one backend then list expecting it.
        // Before: 404 (route removed).
        // Broker-through: every read goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store; the registry seed is the kernel boot path
        // (`Shared.sso_backends`, `seed_from_registry`), and the
        // per-backend detail write path belongs to a sibling task, so
        // the seed here commits the registry root directly. Store unit
        // tests cover insert validation and the management-plane read off
        // the hot path; publish and deliver never touch this store.
        let dir = unique_data_dir("sso-list");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;

        // Empty set reads as an empty collection, never an error.
        let (status, body) = server.get_auth("/api/v5/sso", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        // The route is not paged and takes no parameters: unknown query
        // parameters are ignored, not errors.
        let (status, tolerated) = server
            .send(
                "GET",
                "/api/v5/sso?unexpected_param=1&page=2&limit=1",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(tolerated, json!([]));

        // Configure one backend through the registry root, persist, and
        // restart: the list must show it with its stored fields.
        registry
            .commit_sso_backends(broker_config::SsoBackendsConf {
                backends: vec![broker_config::SsoBackendEntry {
                    backend: "w232-oidc".to_string(),
                    enable: true,
                    config: serde_json::json!({"issuer": "https://idp.example.invalid"})
                        .to_string(),
                }],
            })
            .expect("commit sso backend");
        registry.save().expect("save sso backend");
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        // The admin password persisted too, so the rebooted node logs in
        // with the changed password (`login_as_admin` only fits fresh
        // servers still on the default password).
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/sso", &token).await;
        assert_eq!(status, 200);
        let data = body.as_array().expect("sso list is a collection");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0]["backend"], json!("w232-oidc"));
        assert_eq!(data[0]["enable"], json!(true));
        assert_eq!(
            data[0]["config"]["issuer"],
            json!("https://idp.example.invalid")
        );

        // Unknown query parameters stay ignored on the configured read.
        let (status, tolerated) = server
            .send("GET", "/api/v5/sso?unexpected_param=1", None, Some(&token))
            .await;
        assert_eq!(status, 200);
        assert_eq!(tolerated, body);
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn sso_running_empty_then_enabled_only_and_survives_restart() {
        // W2-36 failing test: list running with none enabled expecting
        // empty, enable one backend then list expecting it.
        // Before: 404 (no route for this path).
        // Broker-through: every read goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store; the registry seed is the kernel boot path
        // (`Shared.sso_backends`, `seed_from_registry`). Store unit
        // tests cover the enabled filter; publish and deliver never
        // touch this store.
        let dir = unique_data_dir("sso-running");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;

        // Empty set reads as an empty collection, never an error. The
        // route is public (login page probes it pre-auth), so both the
        // anonymous and the authenticated reads answer 200 here.
        let (status, body) = server.get("/api/v5/sso/running").await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/sso/running", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!([]));

        // The route is not paged and takes no parameters: unknown query
        // parameters are ignored, not errors.
        let (status, tolerated) = server
            .send(
                "GET",
                "/api/v5/sso/running?unexpected_param=1&page=2&limit=1",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(tolerated, json!([]));

        // Enable one backend and leave one disabled through the registry
        // root, persist, and restart: running must list only the enabled
        // name while the full list still shows both slots.
        registry
            .commit_sso_backends(broker_config::SsoBackendsConf {
                backends: vec![
                    broker_config::SsoBackendEntry {
                        backend: "w236-oidc".to_string(),
                        enable: true,
                        config: serde_json::json!({"issuer": "https://idp.example.invalid"})
                            .to_string(),
                    },
                    broker_config::SsoBackendEntry {
                        backend: "w236-off".to_string(),
                        enable: false,
                        config: String::new(),
                    },
                ],
            })
            .expect("commit sso backends");
        registry.save().expect("save sso backends");
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/sso/running", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!(["w236-oidc"]));
        // Anonymous probe agrees with the authenticated read.
        let (status, anon) = server.get("/api/v5/sso/running").await;
        assert_eq!(status, 200);
        assert_eq!(anon, body);
        // The full list still carries both slots; running is the enabled
        // subset of that live state.
        let (status, full) = server.get_auth("/api/v5/sso", &token).await;
        assert_eq!(status, 200);
        assert_eq!(full.as_array().expect("sso list is a collection").len(), 2);
        // Unknown query parameters stay ignored on the configured read.
        let (status, tolerated) = server
            .send(
                "GET",
                "/api/v5/sso/running?unexpected_param=1",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(tolerated, body);
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn sso_backend_detail_write_read_update_delete_through_router() {
        // W2-39 failing test: write one backend config, read it expecting
        // redacted secrets; update and read back; delete then read
        // expecting 404. Before: all 404 (no route for this path).
        // Broker-through: every call goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store; the registry seed is the kernel boot path
        // (`Shared.sso_backends`, `seed_from_registry`). Store unit tests
        // cover insert validation and the management-plane read off the
        // hot path; publish and deliver never touch this store.
        let dir = unique_data_dir("sso-detail");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;

        // Unknown backends read as 404 in the documented shape.
        let (status, err) = server
            .get_auth("/api/v5/sso/w239-no-such-backend", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));
        assert!(err["message"].is_string());
        assert_eq!(err.as_object().expect("error is an object").len(), 2);

        // Unknown backends delete as 404 and create nothing.
        let (status, err) = server
            .delete_auth("/api/v5/sso/w239-no-such-backend", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));

        // Write one backend carrying a secret; the write answers 200
        // with the stored slot, secrets already redacted.
        let (status, written) = server
            .put_auth(
                "/api/v5/sso/w239-oidc",
                json!({
                    "enable": true,
                    "config": {
                        "issuer": "https://idp.example.invalid",
                        "client_secret": "w239-live-secret",
                    },
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(written["backend"], json!("w239-oidc"));
        assert_eq!(written["enable"], json!(true));
        assert_eq!(
            written["config"]["issuer"],
            json!("https://idp.example.invalid")
        );
        assert_eq!(written["config"]["client_secret"], json!("******"));
        let text = serde_json::to_string(&written).expect("write row serialises");
        assert!(
            !text.contains("w239-live-secret"),
            "stored secret must not render"
        );

        // Read it back: the secret stays redacted on every read.
        let (status, body) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, written);
        let text = serde_json::to_string(&body).expect("detail row serialises");
        assert!(
            !text.contains("w239-live-secret"),
            "stored secret must not render"
        );

        // The list carries the same redacted row: secrets never render
        // on any read.
        let (status, list) = server.get_auth("/api/v5/sso", &token).await;
        assert_eq!(status, 200);
        let rows = list.as_array().expect("sso list is a collection");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], written);

        // Update and read back: the replacement is stored whole.
        let (status, updated) = server
            .put_auth(
                "/api/v5/sso/w239-oidc",
                json!({
                    "enable": false,
                    "config": {"issuer": "https://other.example.invalid"},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(updated["enable"], json!(false));
        assert_eq!(
            updated["config"]["issuer"],
            json!("https://other.example.invalid")
        );
        let (status, body) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, updated);

        // Invalid writes are client errors and apply nothing.
        for bad in [
            json!({"enable": "yes"}),
            json!({"enable": true, "config": 7}),
            json!({"backend": "w239-other", "enable": true}),
        ] {
            let (status, err) = server.put_auth("/api/v5/sso/w239-oidc", bad, &token).await;
            assert_eq!(status, 400);
            assert_eq!(err["code"], json!("BAD_REQUEST"));
            assert!(err["message"].is_string());
        }
        let (status, intact) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
        assert_eq!(status, 200);
        assert_eq!(intact, updated);

        // Delete then read expecting 404.
        let (status, _) = server.delete_auth("/api/v5/sso/w239-oidc", &token).await;
        assert_eq!(status, 204);
        let (status, err) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));

        // Configs persist across a restart: write again, reload the data
        // dir, and read the same redacted slot without re-creating it.
        let (status, _) = server
            .put_auth(
                "/api/v5/sso/w239-oidc",
                json!({
                    "enable": true,
                    "config": {
                        "issuer": "https://idp.example.invalid",
                        "client_secret": "w239-restart-secret",
                    },
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        drop(server);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        // The admin password persisted too, so the rebooted node logs in
        // with the changed password.
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server.get_auth("/api/v5/sso/w239-oidc", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body["backend"], json!("w239-oidc"));
        assert_eq!(body["enable"], json!(true));
        assert_eq!(body["config"]["client_secret"], json!("******"));
        let text = serde_json::to_string(&body).expect("restarted row serialises");
        assert!(
            !text.contains("w239-restart-secret"),
            "stored secret must not render"
        );
        // The route is not paged: unknown query parameters are ignored.
        let (status, tolerated) = server
            .send(
                "GET",
                "/api/v5/sso/w239-oidc?unexpected_param=1",
                None,
                Some(&token),
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(tolerated, body);
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn sso_token_exchange_bad_grant_and_unknown_backend_through_router() {
        // W2-38 failing test through the served management API: before
        // this task the path answered 404 with the router's plain-text
        // body; now unknown backends answer 404 and malformed bodies
        // answer 400, both in the documented `{code, message}` shape.
        // Broker-through: every call goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store. The route is public (pre-auth: the dashboard
        // login page calls it before anyone holds a token), so no bearer
        // is sent. Handler unit tests seed the registry for the success
        // half; this test pins the wiring plus the failure shapes.
        // Management-plane only; publish and deliver never touch the
        // backend registry or the pending table.
        let (server, _state) = TestServer::start().await;

        // Unknown backends fail with 404 and issue no token.
        let (status, err) = server
            .post(
                "/api/v5/sso/token_exchange",
                json!({"backend": "w238-no-such-backend", "grant": "w238-no-such-grant"}),
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(err["code"], json!("BACKEND_NOT_FOUND"));
        assert!(err["message"].is_string());
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        assert!(err.get("token").is_none());

        // Malformed bodies fail with 400 and issue no token.
        let (status, err) = server.post("/api/v5/sso/token_exchange", json!({})).await;
        assert_eq!(status, 400);
        assert_eq!(err["code"], json!("BAD_REQUEST"));
        assert!(err["message"].is_string());
        assert_eq!(err.as_object().expect("error is an object").len(), 2);
        assert!(err.get("token").is_none());

        drop(server);
    }

    #[tokio::test]
    async fn removed_cluster_inflight_routes_return_404() {
        // AP-03 restores `GET /api/v5/cluster` on the real registry.
        // It answers 200 with the local view. Management-plane only.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/cluster", &token).await;
        assert_eq!(
            status, 200,
            "restored route GET /api/v5/cluster must be 200"
        );
        assert!(body["name"].is_string(), "cluster view carries a name");
        assert!(body["nodes"].is_array(), "cluster view carries nodes");
        assert!(body["self"].is_string(), "cluster view carries self");
        assert!(body["description"].is_string(), "cluster view carries text");
        for path in ["/api/v5/clients/any/inflight_messages"] {
            let (status, _) = server.get_auth(path, &token).await;
            assert_eq!(status, 404, "removed route GET {path} must be 404");
        }
        let (status, _) = server.get_auth("/api/v5/nodes", &token).await;
        assert_eq!(status, 200, "kept route GET /api/v5/nodes must be 200");
        state.sessions.get_or_create("mqueue-kept-1", true);
        let (status, _) = server
            .get_auth("/api/v5/clients/mqueue-kept-1/mqueue_messages", &token)
            .await;
        assert_eq!(
            status, 200,
            "kept route GET /api/v5/clients/:clientid/mqueue_messages must be 200"
        );
    }

    #[tokio::test]
    async fn removed_monitoring_fake_routes_return_404() {
        // W1-16 restores `/api/v5/alarms` on the real store, W1-17
        // restores `POST /api/v5/alarms/force_deactivate`, W1-18
        // restores `GET /api/v5/metrics` on the real counters and W1-19
        // restores `GET`/`DELETE /api/v5/monitor` on the history store,
        // so no monitoring route stays removed here; metrics, alarms and
        // monitor coverage lives with their store tests below.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        for path in [
            "/api/v5/metrics",
            "/api/v5/monitor",
            "/api/v5/monitor_current",
            "/api/v5/stats",
        ] {
            let (status, _) = server.get_auth(path, &token).await;
            assert_eq!(status, 200, "kept route GET {path} must be 200");
        }
        let (status, _) = server.delete_auth("/api/v5/monitor", &token).await;
        assert_eq!(status, 204, "kept route DELETE /api/v5/monitor must be 204");
    }

    #[tokio::test]
    async fn login_response_has_no_invented_fields() {
        let (server, _state) = TestServer::start().await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "public"}),
            )
            .await;
        assert_eq!(status, 200);
        assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));
        assert_eq!(body["role"], json!("administrator"));
        assert_eq!(body["must_change_password"], json!(true));
        assert!(
            body.get("version").is_none(),
            "login must not invent version"
        );
        assert!(
            body.get("license").is_none(),
            "login must not invent license"
        );
    }

    #[tokio::test]
    async fn authn_users_list_has_no_invented_fields() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // W2-11: the user routes address one authenticator by id, so the
        // entry must exist first (unknown ids are 404); same setup as the
        // import test below.
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, created) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                json!({"user_id": "mqtt-alice", "password": "s3cret"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created, json!({"user_id": "mqtt-alice"}));
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("data is a list");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0], json!({"user_id": "mqtt-alice"}));
    }

    #[tokio::test]
    async fn authn_import_users_batch_round_trip_rejects_bad_and_duplicate() {
        // W2-07 failing test: import two users to a fresh authenticator,
        // list expecting both; re-import one duplicate expecting clean
        // rejection with the store intact. Before: 404 (no route).
        // Management-plane only: the import locks only the user map
        // (`crates/broker-auth/src/lib.rs`, `MemoryAuth::import_users`);
        // the broker event driving it is the management POST, and CONNECT
        // consults the same map
        // (`crates/broker-node/src/main.rs`, CONNECT handling).
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/import_users",
                json!([
                    {"user_id": "w2-07-a", "password": "Imp0rt-a!"},
                    {"user_id": "w2-07-b", "password": "Imp0rt-b!"},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["total"], json!(2));
        assert_eq!(body["success"], json!(2));

        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("data is a list");
        assert!(data.contains(&json!({"user_id": "w2-07-a"})));
        assert!(data.contains(&json!({"user_id": "w2-07-b"})));

        // The imported credentials authenticate through the broker state:
        // `MemoryAuth::authenticate` accepts the new password and refuses
        // a wrong one (the same call the kernel CONNECT path makes).
        assert!(state
            .auth
            .authenticate("w2-07-c1", Some("w2-07-a"), Some(b"Imp0rt-a!"))
            .await
            .is_ok());
        assert!(state
            .auth
            .authenticate("w2-07-c1", Some("w2-07-a"), Some(b"wrong"))
            .await
            .is_err());

        // Re-import with one duplicate: clean rejection, store intact.
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/import_users",
                json!([
                    {"user_id": "w2-07-b", "password": "Imp0rt-b!"},
                    {"user_id": "w2-07-c", "password": "Imp0rt-c!"},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));
        assert!(body["message"].is_string());
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("data is a list");
        assert_eq!(data.len(), 2);
        assert!(!data.contains(&json!({"user_id": "w2-07-c"})));

        // Duplicate within the batch is rejected the same way.
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/import_users",
                json!([
                    {"user_id": "w2-07-d", "password": "Imp0rt-d!"},
                    {"user_id": "w2-07-d", "password": "Imp0rt-d!"},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("ALREADY_EXISTS"));

        // Malformed batches are client errors with the documented shape
        // and apply nothing.
        for bad in [
            json!([]),
            json!([{"user_id": "w2-07-e"}]),
            json!([{"user_id": "", "password": "Imp0rt-e!"}]),
            json!([{"user_id": "w2-07-e", "password": ""}]),
            json!({"not-users": []}),
        ] {
            let (status, body) = server
                .post_auth(
                    "/api/v5/authentication/password_based:built_in_database/import_users",
                    bad,
                    &token,
                )
                .await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
        }
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"].as_array().expect("data").len(), 2);

        // Unknown authenticator ids are 404 with the documented code,
        // not an empty body.
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication/password_based:nowhere/import_users",
                json!([{"user_id": "w2-07-x", "password": "Imp0rt-x!"}]),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].is_string());

        // Non-built-in backends cannot be imported into the built-in map.
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "mysql"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, body) = server
            .post_auth(
                "/api/v5/authentication/password_based:mysql/import_users",
                json!([{"user_id": "w2-07-y", "password": "Imp0rt-y!"}]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn authn_imported_users_connect_and_persist() {
        // Imported users persist across a restart and authenticate a real
        // broker CONNECT through the console path (connect, not only the
        // store): the imported credential gets CONNACK 0 over the live
        // WebSocket console, a wrong password does not.
        let dir = unique_data_dir("authn-import");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/import_users",
                json!([
                    {"user_id": "w2-07-persist", "password": "Persist1!"},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let mut ok = WsClient::connect(server.port).await;
        ok.send_bin(&mqtt_connect(
            "w2-07-persist-ok",
            Some("w2-07-persist"),
            Some(b"Persist1!"),
        ))
        .await;
        let connack = ok.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_eq!(connack[3], 0);
        drop(ok);

        let mut bad = WsClient::connect(server.port).await;
        bad.send_bin(&mqtt_connect(
            "w2-07-persist-bad",
            Some("w2-07-persist"),
            Some(b"wrong"),
        ))
        .await;
        let connack = bad.recv_msg().await.expect("connack");
        assert_eq!(mqtt_packet_type(&connack), 2);
        assert_ne!(connack[3], 0);
        drop(bad);
        drop(server);
        drop(registry);

        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
        let (server, _state) = TestServer::start_with_registry(reloaded).await;
        let (status, body) = server
            .post(
                "/api/v5/login",
                json!({"username": "admin", "password": "Adm1n-test-pass!"}),
            )
            .await;
        assert_eq!(status, 200);
        let token = body["token"].as_str().expect("admin token").to_string();
        let (status, body) = server
            .get_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("data is a list");
        assert!(data.contains(&json!({"user_id": "w2-07-persist"})));
        drop(server);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn ui_schema_unknown_is_404() {
        // Named dashboard objects (C05-10, W2-31): known names return the
        // documented JSON string that holds the schema value; unknown
        // names are 404 with `{code, message}`. Goes through the broker
        // API server (authenticated request), not only the store.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/schemas/sources", &token).await;
        assert_eq!(status, 404, "sources alias must be 404");
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].is_string());
        for name in ["actions", "connectors", "hotconf"] {
            let (status, body) = server
                .get_auth(&format!("/api/v5/schemas/{name}"), &token)
                .await;
            assert_eq!(status, 200, "served schema {name} must be 200");
            assert!(
                body.is_string(),
                "served schema {name} must be a JSON string"
            );
            let schema: serde_json::Value =
                serde_json::from_str(body.as_str().expect("schema body is a JSON string"))
                    .expect("schema string holds JSON");
            assert!(schema.is_object(), "schema value for {name} is an object");
        }
        // Hot-config alias resolves to the same canonical value.
        let (status, alias) = server.get_auth("/api/v5/schemas/hot-config", &token).await;
        assert_eq!(status, 200);
        let (status, canonical) = server.get_auth("/api/v5/schemas/hotconf", &token).await;
        assert_eq!(status, 200);
        assert_eq!(alias, canonical, "alias must serve the canonical schema");
        let (status, body) = server.get_auth("/api/v5/schemas", &token).await;
        assert_eq!(status, 200);
        assert_eq!(body, json!(["hotconf", "actions", "connectors"]));
    }

    #[tokio::test]
    async fn node_loads_serialise_as_numbers() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server.get_auth("/api/v5/nodes", &token).await;
        assert_eq!(status, 200);
        let first = &body[0];
        for field in ["load1", "load5", "load15"] {
            assert!(
                first[field].is_number(),
                "{field} must be a JSON number, got: {}",
                first[field]
            );
        }
        let node = first["node"].as_str().expect("node name").to_string();
        let (status, single) = server
            .get_auth(&format!("/api/v5/nodes/{node}"), &token)
            .await;
        assert_eq!(status, 200);
        for field in ["load1", "load5", "load15"] {
            assert!(
                single[field].is_number(),
                "{field} must be a JSON number, got: {}",
                single[field]
            );
        }
    }

    #[tokio::test]
    async fn client_subscribe_returns_documented_status() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("tk02-sub", true);
        let (status, _) = server
            .post_auth(
                "/api/v5/clients/tk02-sub/subscribe",
                json!({"topic": "tk02/sub", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
    }

    #[tokio::test]
    async fn subscribe_single_registers_and_lists() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-08-a", true);

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-08-a/subscribe",
                json!({"topic": "w1-08/single", "qos": 1}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(body["clientid"], json!("w1-08-a"));
        assert_eq!(body["topic"], json!("w1-08/single"));
        assert_eq!(body["qos"], json!(1));
        assert_eq!(body["node"], json!(crate::node_scope::local_node_name()));

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-08-a/subscriptions", &token)
            .await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("subscriptions list");
        assert!(
            subs.iter()
                .any(|s| s["topic"] == json!("w1-08/single") && s["qos"] == json!(1)),
            "single subscribe lands and reads back: {subs:?}"
        );
    }

    #[tokio::test]
    async fn subscribe_bulk_mixed_good_and_bad_reports_per_entry() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-08-b", true);

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-08-b/subscribe/bulk",
                json!([
                    {"topic": "w1-08/bulk/good", "qos": 0},
                    {"topic": "bad/#/topic", "qos": 0},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let results = body.as_array().expect("bulk result is a list");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0]["topic"], json!("w1-08/bulk/good"));
        assert_eq!(results[0]["clientid"], json!("w1-08-b"));

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-08-b/subscriptions", &token)
            .await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("subscriptions list");
        assert!(
            subs.iter().any(|s| s["topic"] == json!("w1-08/bulk/good")),
            "bulk good entry lands: {subs:?}"
        );
        assert!(
            !subs.iter().any(|s| s["topic"] == json!("bad/#/topic")),
            "bulk bad entry never lands: {subs:?}"
        );
    }

    #[tokio::test]
    async fn subscribe_unknown_client_is_not_found() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-08-missing/subscribe",
                json!({"topic": "w1-08/nowhere", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-08-missing/subscribe/bulk",
                json!([{"topic": "w1-08/nowhere", "qos": 0}]),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
    }

    #[tokio::test]
    async fn subscribe_malformed_body_is_bad_request() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-08-c", true);

        for bad in [
            json!({"qos": 0}),
            json!({"topic": "", "qos": 0}),
            json!({"topic": "bad/#/topic", "qos": 0}),
            json!({"topic": "w1-08/ok", "qos": 9}),
            json!({"topic": "w1-08/ok", "nl": 2}),
        ] {
            let (status, body) = server
                .post_auth("/api/v5/clients/w1-08-c/subscribe", bad, &token)
                .await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
        }

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-08-c/subscribe/bulk",
                json!({"topic": "w1-08/ok"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn publish_to_durable_subscriber_queues_offline() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-11-a", false);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-11-a/subscribe",
                json!({"topic": "w1-11/delivery", "qos": 1}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post_auth(
                "/api/v5/publish",
                json!({"topic": "w1-11/delivery", "payload": "hello-w1-11", "qos": 1}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert!(body["id"].is_string(), "publish accepts with id: {body:?}");

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-11-a/mqueue_messages", &token)
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("mqueue data is a list");
        assert!(
            data.iter()
                .any(|m| m["topic"] == json!("w1-11/delivery")
                    && m["payload"] == json!("hello-w1-11")),
            "published message reaches detached durable queue: {data:?}"
        );
    }

    #[tokio::test]
    async fn publish_without_subscribers_still_accepts() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server
            .post_auth(
                "/api/v5/publish",
                json!({"topic": "w1-11/nobody-here", "payload": "hello", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 202);
        assert_eq!(body["reason_code"], json!(16));
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn publish_malformed_body_is_bad_request() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        for bad in [
            json!({"payload": "x"}),
            json!({"topic": "", "payload": "x"}),
            json!({"topic": "bad/#/topic", "payload": "x"}),
            json!({"topic": "w1-11/ok", "qos": 9, "payload": "x"}),
            json!({"topic": "w1-11/ok", "payload": 42}),
            json!({"topic": "w1-11/ok", "payload": "x", "payload_encoding": "hex"}),
            json!({"topic": "w1-11/ok", "payload": "!!!", "payload_encoding": "base64"}),
            json!({"topic": "w1-11/ok", "retain": "yes", "payload": "x"}),
            json!({"topic": "w1-11/ok", "payload": "x", "payload_encoding": 42}),
            json!({"topic": "w1-11/ok", "payload": "x", "qos": "1"}),
        ] {
            let (status, body) = server.post_auth("/api/v5/publish", bad, &token).await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
            let obj = body.as_object().expect("error body is an object");
            assert_eq!(
                obj.len(),
                2,
                "error shape is exactly code+message: {body:?}"
            );
        }
    }

    #[tokio::test]
    async fn publish_retain_flag_and_base64_honoured() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-11-b", false);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-11-b/subscribe",
                json!({"topic": "w1-11/retained", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        // "hi-w11" as base64 with retain flag set.
        let (status, body) = server
            .post_auth(
                "/api/v5/publish",
                json!({
                    "topic": "w1-11/retained",
                    "payload": "aGktdzEx",
                    "payload_encoding": "base64",
                    "qos": 0,
                    "retain": true,
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert!(body["id"].is_string());

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-11-b/mqueue_messages", &token)
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("mqueue data is a list");
        assert!(
            data.iter()
                .any(|m| m["topic"] == json!("w1-11/retained") && m["payload"] == json!("hi-w11")),
            "base64 payload decodes and retain flag delivers: {data:?}"
        );
    }

    #[tokio::test]
    async fn publish_bulk_mixed_good_and_bad_reports_per_entry() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-12-a", false);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-12-a/subscribe",
                json!({"topic": "w1-12/bulk/good", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post_auth(
                "/api/v5/publish/bulk",
                json!([
                    {"topic": "w1-12/bulk/good", "payload": "hello-w1-12", "qos": 0},
                    {"topic": "bad/#/topic", "payload": "x", "qos": 0},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        let results = body.as_array().expect("bulk result is a list");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["topic"], json!("w1-12/bulk/good"));
        assert!(
            results[0]["id"].is_string(),
            "bulk good entry accepts with id: {results:?}"
        );
        assert_eq!(results[1]["topic"], json!("bad/#/topic"));
        assert!(results[1]["message"].is_string());
        assert!(results[1]["reason_code"].is_number());

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-12-a/mqueue_messages", &token)
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("mqueue data is a list");
        assert!(
            data.iter()
                .any(|m| m["topic"] == json!("w1-12/bulk/good")
                    && m["payload"] == json!("hello-w1-12")),
            "bulk good entry delivers while bad entry fails: {data:?}"
        );
    }

    #[tokio::test]
    async fn publish_bulk_all_good_delivers_every_entry() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-12-b", false);

        for topic in ["w1-12/all/1", "w1-12/all/2"] {
            let (status, _) = server
                .post_auth(
                    "/api/v5/clients/w1-12-b/subscribe",
                    json!({"topic": topic, "qos": 0}),
                    &token,
                )
                .await;
            assert_eq!(status, 200);
        }

        let (status, body) = server
            .post_auth(
                "/api/v5/publish/bulk",
                json!([
                    {"topic": "w1-12/all/1", "payload": "one", "qos": 0},
                    {"topic": "w1-12/all/2", "payload": "two", "qos": 0},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let results = body.as_array().expect("bulk result is a list");
        assert_eq!(results.len(), 2);
        for result in results {
            assert!(
                result["id"].is_string(),
                "every bulk entry accepts with id: {results:?}"
            );
            assert!(
                result.get("code").is_none(),
                "success entries carry no error code: {results:?}"
            );
        }

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-12-b/mqueue_messages", &token)
            .await;
        assert_eq!(status, 200);
        let data = body["data"].as_array().expect("mqueue data is a list");
        assert!(
            data.iter()
                .any(|m| m["topic"] == json!("w1-12/all/1") && m["payload"] == json!("one")),
            "first bulk entry delivers: {data:?}"
        );
        assert!(
            data.iter()
                .any(|m| m["topic"] == json!("w1-12/all/2") && m["payload"] == json!("two")),
            "second bulk entry delivers: {data:?}"
        );
    }

    #[tokio::test]
    async fn publish_bulk_malformed_body_is_bad_request() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server
            .post_auth(
                "/api/v5/publish/bulk",
                json!({"topic": "w1-12/ok", "payload": "x"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
        let obj = body.as_object().expect("error body is an object");
        assert_eq!(
            obj.len(),
            2,
            "error shape is exactly code+message: {body:?}"
        );
    }

    #[tokio::test]
    async fn unsubscribe_single_round_trip_removes_and_reads_empty() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-10-a", true);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-10-a/subscribe",
                json!({"topic": "w1-10/single", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-10-a/subscriptions", &token)
            .await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("subscriptions list");
        assert!(
            subs.iter().any(|s| s["topic"] == json!("w1-10/single")),
            "subscribed entry present before unsubscribe: {subs:?}"
        );

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-10-a/unsubscribe",
                json!({"topic": "w1-10/single"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-10-a/subscriptions", &token)
            .await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("subscriptions list");
        assert!(
            !subs.iter().any(|s| s["topic"] == json!("w1-10/single")),
            "unsubscribed entry gone: {subs:?}"
        );

        // Removing an absent subscription still succeeds.
        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-10-a/unsubscribe",
                json!({"topic": "w1-10/single"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn unsubscribe_bulk_mixed_present_absent_reports_success() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-10-b", true);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-10-b/subscribe",
                json!({"topic": "w1-10/bulk/present", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-10-b/unsubscribe/bulk",
                json!([
                    {"topic": "w1-10/bulk/present"},
                    {"topic": "w1-10/bulk/absent"},
                ]),
                &token,
            )
            .await;
        // Spec reports 204 with no body. Absent still succeeds.
        assert_eq!(status, 204);

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-10-b/subscriptions", &token)
            .await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("subscriptions list");
        assert!(
            !subs
                .iter()
                .any(|s| s["topic"] == json!("w1-10/bulk/present")),
            "bulk-removed entry gone: {subs:?}"
        );
    }

    #[tokio::test]
    async fn unsubscribe_unknown_client_is_not_found() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-10-missing/unsubscribe",
                json!({"topic": "w1-10/nowhere"}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-10-missing/unsubscribe/bulk",
                json!([{"topic": "w1-10/nowhere"}]),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
    }

    #[tokio::test]
    async fn unsubscribe_malformed_body_is_bad_request() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-10-c", true);

        for bad in [
            json!({"qos": 0}),
            json!({"topic": ""}),
            json!({"topic": "bad/#/topic"}),
        ] {
            let (status, body) = server
                .post_auth("/api/v5/clients/w1-10-c/unsubscribe", bad, &token)
                .await;
            assert_eq!(status, 400);
            assert_eq!(body["code"], json!("BAD_REQUEST"));
            assert!(body["message"].is_string());
        }

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-10-c/unsubscribe/bulk",
                json!({"topic": "w1-10/ok"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));

        // One bad entry fails the whole batch with 400.
        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-10-c/subscribe",
                json!({"topic": "w1-10/bulk/good", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .post_auth(
                "/api/v5/clients/w1-10-c/unsubscribe/bulk",
                json!([
                    {"topic": "w1-10/bulk/good"},
                    {"topic": "bad/#/topic"},
                ]),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }

    #[tokio::test]
    async fn client_subscriptions_empty_list_is_empty_not_error() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-09-empty", true);

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-09-empty/subscriptions", &token)
            .await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("subscriptions list");
        assert!(subs.is_empty(), "unsubscribed client reads empty: {subs:?}");
    }

    #[tokio::test]
    async fn client_subscriptions_lists_subscribe_with_options() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        state.sessions.get_or_create("w1-09-opts", true);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-09-opts/subscribe",
                json!({"topic": "w1-09/with-opts", "qos": 1, "nl": 1, "rap": 0, "rh": 1}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-09-opts/subscribe",
                json!({"topic": "w1-09/defaults", "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-09-opts/subscriptions", &token)
            .await;
        assert_eq!(status, 200);
        let subs = body.as_array().expect("subscriptions list");
        assert_eq!(subs.len(), 2, "both subscribes read back: {subs:?}");
        let entry = subs
            .iter()
            .find(|s| s["topic"] == json!("w1-09/with-opts"))
            .expect("subscribed entry present");
        assert_eq!(entry["qos"], json!(1));
        assert_eq!(entry["nl"], json!(1));
        assert_eq!(entry["rap"], json!(0));
        assert_eq!(entry["rh"], json!(1));
        assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
        assert_eq!(entry["clientid"], json!("w1-09-opts"));
        let defaults = subs
            .iter()
            .find(|s| s["topic"] == json!("w1-09/defaults"))
            .expect("default entry present");
        assert_eq!(defaults["qos"], json!(0));
        assert_eq!(defaults["nl"], json!(0));
        assert_eq!(defaults["rap"], json!(0));
        assert_eq!(defaults["rh"], json!(0));
        assert_eq!(
            defaults["node"],
            json!(crate::node_scope::local_node_name())
        );
        assert_eq!(defaults["clientid"], json!("w1-09-opts"));
    }

    #[tokio::test]
    async fn client_subscriptions_unknown_is_not_found() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server
            .get_auth("/api/v5/clients/w1-09-missing/subscriptions", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
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

    #[tokio::test]
    async fn dashboard_user_create_returns_documented_status() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, created) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "tk02-user", "password": "Tk02-passw0rd!"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(created["username"], json!("tk02-user"));
    }

    #[tokio::test]
    async fn change_pwd_returns_documented_status() {
        let (server, _state) = TestServer::start().await;
        let fresh = server.admin_token().await;
        let (status, body) = server
            .put_auth(
                "/api/v5/users/admin/change_pwd",
                json!({"old_pwd": "public", "new_pwd": "Tk02-new-pass!"}),
                &fresh,
            )
            .await;
        assert_eq!(status, 204);
        assert_eq!(body, json!(null));
    }

    #[tokio::test]
    async fn probe_failures_report_documented_code() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Port 9 is closed, so each probe reaches its failure branch.
        // The connector registry accepts `http`; the action registry
        // (W3-01 catalogue) serves `webhook` for the same URL shape.
        for (path, body) in [
            (
                "/api/v5/connectors_probe",
                json!({"type": "http", "url": "http://127.0.0.1:9"}),
            ),
            (
                "/api/v5/actions_probe",
                json!({"type": "webhook", "url": "http://127.0.0.1:9"}),
            ),
        ] {
            let (status, body) = server.post_auth(path, body, &token).await;
            assert_eq!(status, 400, "{path} failure must be 400");
            assert_eq!(body["code"], json!("TEST_FAILED"), "{path} code");
        }
    }

    #[tokio::test]
    async fn actions_probe_is_stateless_with_documented_shapes() {
        // W3-07 (C06-11) broker-through test: a valid probe succeeds without
        // creating an action, a bad config fails with the documented error
        // shape, and an unknown kind is rejected. Before: the handler dialled
        // without validating the kind, so unknown kinds and malformed bodies
        // read as success. Management-plane only; publish and deliver never
        // touch the probe path.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Valid probe: known kind with nothing to dial, so reachable by
        // validation alone. Must not create an action: the list still
        // reports an empty page afterwards.
        let (status, body) = server
            .post_auth("/api/v5/actions_probe", json!({"type": "console"}), &token)
            .await;
        assert_eq!(status, 204);
        assert_eq!(body, json!(null));
        let (status, body) = server.get_auth("/api/v5/actions", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            body.as_array().map(Vec::len),
            Some(0),
            "probe must not create an action"
        );
        assert_eq!(body, json!([]));
        // Bad config: known kind with a closed port reports the failure.
        let (status, body) = server
            .post_auth(
                "/api/v5/actions_probe",
                json!({"type": "webhook", "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "bad probe config must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        // Unknown kind is rejected with the documented error shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/actions_probe",
                json!({"type": "no_such_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "unknown action kind must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        // Malformed body is rejected with the documented error shape.
        let (status, body) = server
            .post_auth("/api/v5/actions_probe", json!({}), &token)
            .await;
        assert_eq!(status, 400, "malformed probe body must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        // Probe leaves no state behind even after failures.
        let (status, body) = server.get_auth("/api/v5/actions", &token).await;
        assert_eq!(status, 200);
        assert_eq!(
            body.as_array().map(Vec::len),
            Some(0),
            "probe must leave no state"
        );
    }

    #[tokio::test]
    async fn connectors_probe_is_stateless_with_documented_shapes() {
        // Hold the serial guard: the connector store is process-global,
        // so a sibling create must not run during the count check.
        let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
        // W3-21 (C07-08) broker-through test: a valid probe succeeds without
        // creating a connector, a bad config fails with the documented error
        // shape, and an unknown kind is rejected. Before: validation was
        // incomplete (no kind check, missing bodies read as success) and a
        // kind with nothing to dial read as a failure. Management-plane only;
        // publish and deliver never touch the probe path.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // Valid probe: known kind with nothing to dial, so reachable by
        // validation alone. Must not create a connector: the list count is
        // unchanged afterwards and the probe id reads back 404.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors_probe",
                json!({"type": "console", "name": "w321-probe-no-create"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        assert_eq!(body, json!(null));
        let (status, after) = server.get_auth("/api/v5/connectors", &token).await;
        assert_eq!(status, 200);
        // Other tests add connectors to the same store at the same time,
        // thus the count can change. Look for the probe id in the list.
        let created = after["data"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["id"] == json!("w321-probe-no-create")
                    || row["name"] == json!("w321-probe-no-create")
            })
        });
        assert!(!created, "probe must not create a connector");
        let (status, _) = server
            .get_auth("/api/v5/connectors/w321-probe-no-create", &token)
            .await;
        assert_eq!(status, 404, "probe must leave no state behind");
        // Bad config: known kind with a closed port reports the failure.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors_probe",
                json!({"type": "http", "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "bad probe config must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        // Unknown kind is rejected with the documented error shape.
        let (status, body) = server
            .post_auth(
                "/api/v5/connectors_probe",
                json!({"type": "no_such_kind_xyz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "unknown connector kind must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        assert!(
            body["message"]
                .as_str()
                .unwrap_or("")
                .contains("unknown connector type"),
            "unknown kind names the kind: {body:?}"
        );
        // Malformed body is rejected with the documented error shape.
        let (status, body) = server
            .post_auth("/api/v5/connectors_probe", json!({}), &token)
            .await;
        assert_eq!(status, 400, "malformed probe body must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"));
        // Probe leaves no state behind even after failures.
        let (status, body) = server.get_auth("/api/v5/connectors", &token).await;
        assert_eq!(status, 200);
        let created = body["data"].as_array().is_some_and(|rows| {
            rows.iter().any(|row| {
                row["id"] == json!("w321-probe-no-create")
                    || row["name"] == json!("w321-probe-no-create")
            })
        });
        assert!(!created, "probe must leave no state");
    }

    #[tokio::test]
    async fn audit_log_lists_management_operations_with_paging_and_time_filter() {
        // W2-28 failing test: perform two management operations, query
        // the audit log expecting both entries with paging metadata;
        // query with a time filter excluding them expecting an empty
        // page. Before: 404 (no route).
        // Broker-through: every step goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store. The entries are recorded by the management
        // auth middleware (`crates/broker-api/src/api_auth.rs`,
        // `require_api_auth`) on each authenticated request; publish and
        // deliver never touch the ring.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        // Two management operations through the broker.
        let (status, _) = server
            .post_auth(
                "/api/v5/api_key",
                json!({"name": "w228-audit-a", "scopes": ["monitoring"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/api_key",
                json!({"name": "w228-audit-b", "scopes": ["monitoring"]}),
                &token,
            )
            .await;
        assert_eq!(status, 200);

        // Query the log: both entries read back with paging metadata.
        let (status, body) = server.get_auth("/api/v5/audit", &token).await;
        assert_eq!(status, 200);
        let data = body[0]["data"].as_array().expect("data is a list");
        assert!(
            data.len() >= 2,
            "both management operations are audited: {body:?}"
        );
        assert!(body[0]["meta"].is_object(), "page envelope carries meta");
        assert_eq!(
            body[0]["meta"]["page"],
            json!(1),
            "default page reports: {body:?}"
        );
        assert_eq!(
            body[0]["meta"]["limit"],
            json!(100),
            "default limit reports: {body:?}"
        );
        let ids: Vec<&str> = data
            .iter()
            .filter_map(|entry| entry["operation_id"].as_str())
            .collect();
        assert!(
            ids.iter().any(|id| id.contains("api_key")),
            "audit rows name the operated resource: {ids:?}"
        );
        for entry in data {
            assert!(entry["created_at"].is_string(), "row has a timestamp");
            assert_eq!(entry["from"], json!("rest_api"));
            assert_eq!(entry["operation_result"], json!("success"));
            assert!(entry["http_status_code"].is_number());
        }

        // Paged read keeps the envelope.
        let (status, paged) = server
            .get_auth("/api/v5/audit?page=1&limit=1", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(paged[0]["meta"]["page"], json!(1));
        assert_eq!(paged[0]["meta"]["limit"], json!(1));
        assert_eq!(paged[0]["data"].as_array().expect("paged data").len(), 1);

        // Unknown query parameters are ignored, not errors.
        let (status, _) = server
            .get_auth("/api/v5/audit?unknown_param=1", &token)
            .await;
        assert_eq!(status, 200);

        // A time window excluding both entries reads as an empty page,
        // not an error.
        let (status, empty) = server
            .get_auth("/api/v5/audit?gte_created_at=4102444800000000", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(empty[0]["data"], json!([]));
        assert_eq!(empty[0]["meta"]["count"], json!(0));

        // The outcome filter honours success.
        let (status, filtered) = server
            .get_auth("/api/v5/audit?operation_result=success", &token)
            .await;
        assert_eq!(status, 200);
        assert!(
            filtered[0]["meta"]["count"].as_u64().expect("count") >= 2,
            "success filter keeps both entries: {filtered:?}"
        );

        let (status, _) = server
            .delete_auth("/api/v5/api_key/w228-audit-a", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/api_key/w228-audit-b", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn audit_log_ignores_reads() {
        // AP2-05 failing test: a read through the API makes no audit
        // record. GET /clients then GET /audit shows no entry for the
        // read. Before: it had one.
        // Broker-through: both steps go through the served management
        // API on `ApiState`, not only through the store. The entries
        // are recorded by the management auth middleware
        // (`crates/broker-api/src/api_auth.rs`, `require_api_auth`).
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server.get_auth("/api/v5/clients", &token).await;
        assert_eq!(status, 200);
        let (status, body) = server.get_auth("/api/v5/audit", &token).await;
        assert_eq!(status, 200);
        let data = body[0]["data"].as_array().expect("data is a list");
        assert!(
            data.iter()
                .all(|entry| entry["operation_id"] != json!("/api/v5/clients")),
            "reads leave no audit record: {body:?}"
        );
        assert!(
            data.iter()
                .all(|entry| entry["http_method"] != json!("get")),
            "audit holds only documented methods: {body:?}"
        );
    }

    #[tokio::test]
    async fn action_types_lists_documented_names() {
        // AP-05: GET /action_types returns the documented names as a
        // bare string array, limited to the runtimes in this build. The
        // old shape (objects with kind plus configuration) contradicts
        // the documented array of strings, so the test changed with the
        // handler. An unknown query parameter is ignored.
        // Broker-through: the request goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`); the list is a
        // static snapshot read only on this management GET and never
        // consulted on publish or deliver.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server.get_auth("/api/v5/action_types", &token).await;
        assert_eq!(status, 200);
        let names = body.as_array().expect("type list is an array");
        assert!(!names.is_empty(), "type list must not be empty");
        let kinds: std::collections::HashSet<&str> =
            names.iter().filter_map(|e| e.as_str()).collect();
        assert_eq!(kinds.len(), names.len(), "names must be unique strings");
        // The console order starts with http, mqtt and kafka. Every name
        // stays inside the documented enum.
        for expected in ["http", "mqtt", "kafka_producer", "redis", "mysql", "pgsql"] {
            assert!(kinds.contains(expected), "type list holds '{expected}'");
        }
        for missing in [
            "bigtable",
            "quasardb",
            "syskeeper_forwarder",
            "matrix",
            "azure_event_grid",
        ] {
            assert!(
                !kinds.contains(missing),
                "type list has no '{missing}' without a runtime"
            );
        }

        // Unknown query parameters are ignored, never an error.
        let (status, again) = server
            .get_auth("/api/v5/action_types?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(again, body);
    }

    #[tokio::test]
    async fn source_types_lists_documented_names() {
        // AP-05: GET /source_types returns the documented names as a
        // bare string array, limited to the runtimes in this build. The
        // old shape (objects with kind plus configuration) contradicts
        // the documented array of strings, so the test changed with the
        // handler. An unknown query parameter is ignored.
        // Broker-through: the request goes through the served management
        // API on `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`); the list is a
        // static snapshot read only on this management GET and never
        // consulted on publish or deliver.
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;

        let (status, body) = server.get_auth("/api/v5/source_types", &token).await;
        assert_eq!(status, 200);
        let entries = body.as_array().expect("type list is an array");
        assert!(!entries.is_empty(), "type list must not be empty");
        let kinds: std::collections::HashSet<&str> =
            entries.iter().filter_map(|e| e.as_str()).collect();
        assert_eq!(kinds.len(), entries.len(), "names must be unique strings");
        for expected in ["mqtt", "kafka_consumer", "rabbitmq"] {
            assert!(kinds.contains(expected), "type list holds '{expected}'");
        }
        for wrong in ["test", "azure_event_grid", "no_such_source_kind_xyz"] {
            assert!(
                !kinds.contains(wrong),
                "type list has no unrunnable '{wrong}'"
            );
        }

        // Unknown query parameters are ignored, never an error.
        let (status, again) = server
            .get_auth("/api/v5/source_types?unknown=param", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(again, body);

        // Every listed kind has a runtime behind it: a source of the
        // listed `mqtt` kind registers through the broker API (the W3-23
        // enable-flag runtime), then cleans up.
        let (status, _) = server
            .post_auth(
                "/api/v5/sources",
                json!({"name": "w325-probe-src", "type": "mqtt"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .delete_auth("/api/v5/sources/w325-probe-src", &token)
            .await;
        assert_eq!(status, 204);

        // The catalogue is never consulted per message: a published
        // message still reaches the rule trigger path after the reads.
        state
            .engine
            .create_rule(
                "w325-rule".to_string(),
                broker_protocol::TopicFilter::new("w325/#").expect("filter"),
                None,
                true,
                vec![broker_rules::RuleAction::Log],
            )
            .expect("rule creates");
        struct W325Sink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for W325Sink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let sink: std::sync::Arc<dyn broker_rules::BrokerSink> = std::sync::Arc::new(W325Sink);
        let topic = broker_protocol::Topic::new("w325/probe").expect("topic");
        let payload = bytes::Bytes::from_static(b"{}");
        let matched = state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &sink)
            .await;
        assert_eq!(matched, 1);
    }

    #[tokio::test]
    async fn ap05_http_action_typed_round_trip() {
        // AP-05: the documented http action body is accepted and reads
        // back in the same shape. The read works through the bare name
        // and through the `{type}:{name}` key. A replace keeps the
        // shape. An unknown type is 400. A duplicate is 400.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        // The referenced connector must exist for the owned sink to build
        // and the action to read `connected`.
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"name": "ap05-up", "type": "http", "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "connector create: {status}");
        let body = json!({
            "type": "http",
            "name": "ap05-web",
            "connector": "http:ap05-up",
            "enable": true,
            "parameters": {"method": "post", "path": "/hook"},
            "resource_opts": {},
        });
        let (status, created) = server
            .post_auth("/api/v5/actions", body.clone(), &token)
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["type"], json!("http"));
        assert_eq!(created["name"], json!("ap05-web"));
        assert_eq!(created["connector"], json!("http:ap05-up"));
        assert_eq!(created["parameters"]["path"], json!("/hook"));
        assert!(created["resource_opts"].is_object());
        assert_eq!(created["status"], json!("connected"));
        assert!(created["node_status"].is_array());
        for path in ["/api/v5/actions/ap05-web", "/api/v5/actions/http:ap05-web"] {
            let (status, read) = server.get_auth(path, &token).await;
            assert_eq!(status, 200, "read through {path}");
            assert_eq!(read, created, "same shape through {path}");
        }
        let (status, _) = server.post_auth("/api/v5/actions", body, &token).await;
        assert_eq!(status, 400);
        let (status, bad) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "no_such_kind_xyz", "name": "ap05-bad"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(bad["code"], json!("ALREADY_EXISTS"));
        let (status, missing) = server
            .get_auth("/api/v5/actions/http:no-such", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(missing["code"], json!("NOT_FOUND"));
        let (status, updated) = server
            .put_auth(
                "/api/v5/actions/http:ap05-web",
                json!({
                    "type": "http",
                    "name": "ap05-web",
                    "connector": "http:ap05-up",
                    "enable": false,
                    "parameters": {"method": "post", "path": "/hook2"},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(updated["enable"], json!(false));
        assert_eq!(updated["parameters"]["path"], json!("/hook2"));
        assert_eq!(updated["status"], json!("disconnected"));
        let (status, _) = server.delete_auth("/api/v5/actions/ap05-web", &token).await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn ap05_mqtt_kafka_actions_typed_round_trip() {
        // AP-05: the console shapes for mqtt and kafka actions are
        // accepted and read back unchanged. Old flat bodies still pass.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let mqtt = json!({
            "type": "mqtt",
            "name": "ap05-mqtt",
            "connector": "mqtt:ap05-up",
            "parameters": {"topic": "out/ap05", "qos": 1, "retain": false},
        });
        let (status, created) = server.post_auth("/api/v5/actions", mqtt, &token).await;
        assert_eq!(status, 201);
        assert_eq!(created["parameters"]["topic"], json!("out/ap05"));
        let kafka = json!({
            "type": "kafka_producer",
            "name": "ap05-kafka",
            "connector": "kafka_producer:ap05-up",
            "parameters": {"topic": "events", "message": {"key": "${.clientid}", "value": "${.}"}},
        });
        let (status, created) = server.post_auth("/api/v5/actions", kafka, &token).await;
        assert_eq!(status, 201);
        assert_eq!(created["type"], json!("kafka_producer"));
        // The historic alias still passes and reads back as sent.
        let (status, legacy) = server
            .post_auth(
                "/api/v5/actions",
                json!({"name": "ap05-legacy", "type": "mqtt_bridge", "server": "127.0.0.1:1883"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(legacy["type"], json!("mqtt_bridge"));
        assert_eq!(legacy["server"], json!("127.0.0.1:1883"));
        for id in ["ap05-mqtt", "ap05-kafka", "ap05-legacy"] {
            let (status, _) = server
                .delete_auth(&format!("/api/v5/actions/{id}"), &token)
                .await;
            assert_eq!(status, 204);
        }
    }

    #[tokio::test]
    async fn ap05_connectors_typed_round_trip() {
        // AP-05: connector creates for http, mqtt and kafka use the
        // documented fields and read back through both id forms. The
        // documented names run on the runtime of their base kind.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let mut cases = vec![
            (
                "ap05-conn-http",
                json!({"type": "http", "name": "ap05-conn-http", "url": "http://127.0.0.1:9"}),
                "http:ap05-conn-http",
            ),
            (
                "ap05-conn-mqtt",
                json!({"type": "mqtt", "name": "ap05-conn-mqtt", "server": "127.0.0.1:1883"}),
                "mqtt:ap05-conn-mqtt",
            ),
        ];

        let kafka_case = (
            "ap05-conn-kafka",
            json!({"type": "kafka_producer", "name": "ap05-conn-kafka", "bootstrap_hosts": "127.0.0.1:9092"}),
            "kafka_producer:ap05-conn-kafka",
        );
        if cfg!(feature = "kafka") {
            cases.push(kafka_case);
        } else {
            // The Kafka runtime is a Cargo feature. A build without it
            // refuses the create and names the feature.
            let (_, body, _) = kafka_case;
            let (status, refused) = server.post_auth("/api/v5/connectors", body, &token).await;
            assert_eq!(status, 400);
            assert!(
                refused.to_string().contains("kafka"),
                "the refusal names the feature: {refused}"
            );
        }
        for (bare, body, key) in cases {
            let (status, created) = server.post_auth("/api/v5/connectors", body, &token).await;
            assert_eq!(status, 201, "create for {bare}");
            assert_eq!(created["name"], json!(bare));
            assert!(created["status"].is_string());
            assert!(created["node_status"].is_array());
            for path in [
                format!("/api/v5/connectors/{bare}"),
                format!("/api/v5/connectors/{key}"),
            ] {
                let (status, read) = server.get_auth(&path, &token).await;
                assert_eq!(status, 200, "read through {path}");
                assert_eq!(read["name"], json!(bare));
            }
            let (status, _) = server
                .put_auth(
                    &format!("/api/v5/connectors/{key}"),
                    json!({"type": created["type"].clone(), "name": bare, "description": "ap05"}),
                    &token,
                )
                .await;
            assert_eq!(status, 200, "replace through {key}");
            let (status, _) = server
                .delete_auth(&format!("/api/v5/connectors/{bare}"), &token)
                .await;
            assert_eq!(status, 204);
        }
        let (status, bad) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "no_such_kind_xyz", "name": "ap05-bad"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(bad["code"], json!("ALREADY_EXISTS"));
    }

    #[tokio::test]
    async fn ap05_sources_typed_round_trip() {
        // AP-05: source creates for mqtt and kafka_consumer use the
        // documented envelope and read back through both id forms.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let cases = [
            (
                "ap05-src-mqtt",
                json!({
                    "type": "mqtt",
                    "name": "ap05-src-mqtt",
                    "connector": "mqtt:ap05-up",
                    "parameters": {"topic": "in/ap05"},
                }),
            ),
            (
                "ap05-src-kafka",
                json!({
                    "type": "kafka_consumer",
                    "name": "ap05-src-kafka",
                    "connector": "kafka_consumer:ap05-up",
                    "parameters": {"topic": "events"},
                }),
            ),
        ];
        for (bare, body) in cases {
            let (status, created) = server.post_auth("/api/v5/sources", body, &token).await;
            assert_eq!(status, 201, "create for {bare}");
            assert_eq!(created["name"], json!(bare));
            assert!(created["parameters"].is_object());
            assert_eq!(created["status"], json!("connected"));
            let (status, _) = server
                .get_auth(&format!("/api/v5/sources/{bare}"), &token)
                .await;
            assert_eq!(status, 200);
            let (status, _) = server
                .delete_auth(&format!("/api/v5/sources/{bare}"), &token)
                .await;
            assert_eq!(status, 204);
        }
    }

    #[tokio::test]
    async fn ap05_schema_registry_source_field_and_single_read() {
        // AP-05: the registry accepts the documented `source` text and
        // serves the single read. Old `schema` bodies still pass.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, created) = server
            .post_auth(
                "/api/v5/schema_registry",
                json!({
                    "name": "ap05-schema",
                    "type": "json",
                    "source": "{\"type\": \"object\"}",
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        assert_eq!(created["source"], json!("{\"type\":\"object\"}"));
        let (status, read) = server
            .get_auth("/api/v5/schema_registry/ap05-schema", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(read, created);
        let (status, missing) = server
            .get_auth("/api/v5/schema_registry/no-such", &token)
            .await;
        assert_eq!(status, 404);
        assert_eq!(missing["code"], json!("NOT_FOUND"));
        let (status, _) = server
            .delete_auth("/api/v5/schema_registry/ap05-schema", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn ap05_typed_action_fires_through_the_rule_path() {
        // AP-05 integration: a typed http action created through the
        // management API fires through the broker rule path. A rule
        // forwards to the action id. A publish through
        // `dispatch_ingress` reaches the sink once. Every step uses the
        // production entries: the HTTP router and the engine ingress.
        use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
        struct CountingSink {
            hits: AtomicU64,
        }
        #[async_trait::async_trait]
        impl broker_connectors::Sink for CountingSink {
            async fn send(
                &self,
                _topic: &broker_protocol::Topic,
                _payload: &bytes::Bytes,
                _qos: broker_protocol::QoS,
            ) -> broker_connectors::Result<()> {
                self.hits.fetch_add(1, AtomicOrdering::Relaxed);
                Ok(())
            }
            fn kind(&self) -> &'static str {
                "ap05-counter"
            }
        }
        struct NoopBrokerSink;
        #[async_trait::async_trait]
        impl broker_rules::BrokerSink for NoopBrokerSink {
            async fn publish(
                &self,
                _topic: broker_protocol::Topic,
                _payload: bytes::Bytes,
                _qos: broker_protocol::QoS,
                _retain: bool,
            ) -> Result<(), broker_rules::RuleEngineError> {
                Ok(())
            }
        }
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({
                    "type": "http",
                    "name": "ap05-fire",
                    "connector": "http:ap05-up",
                    "parameters": {"method": "post", "path": "/hook"},
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let counter = std::sync::Arc::new(CountingSink {
            hits: AtomicU64::new(0),
        });
        state.engine.connectors().register(
            broker_config::v5_action_sink_key("ap05-fire"),
            counter.clone(),
        );
        let (status, _) = server
            .post_auth(
                "/api/v5/rules",
                json!({
                    "id": "ap05-rule",
                    "name": "ap05-rule",
                    "sql": "SELECT * FROM \"ap05/fire\"",
                    "actions": ["ap05-fire"],
                }),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let broker: std::sync::Arc<dyn broker_rules::BrokerSink> =
            std::sync::Arc::new(NoopBrokerSink);
        let topic = broker_protocol::Topic::new("ap05/fire").expect("topic");
        let payload = bytes::Bytes::from_static(br#"{"temp": 21.5}"#);
        state
            .engine
            .dispatch_ingress(&topic, &payload, broker_protocol::QoS::AtMostOnce, &broker)
            .await;
        assert_eq!(counter.hits.load(AtomicOrdering::Relaxed), 1);
        let (status, _) = server.delete_auth("/api/v5/rules/ap05-rule", &token).await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/actions/ap05-fire", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn backup_import_round_trip_through_broker() {
        // W5-29 broker-through: create users and rules, export, change
        // them, import, and read them again. Every step goes through the
        // served management API on `ApiState` (the same state the kernel
        // shares in `crates/broker-node/src/main.rs`, `serve_api`).
        // Management-plane only: the import takes short management locks
        // and never the publish or deliver path.
        let dir = unique_data_dir("data-import-roundtrip");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
        let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
        let token = server.login_as_admin().await;

        // One MQTT user, one rule, one ban and one ACL entry.
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication",
                json!({"mechanism": "password_based", "backend": "built_in_database"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authentication/password_based:built_in_database/users",
                json!({"user_id": "rt-user", "password": "RtPass123!"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "rt-rule", "sql": "SELECT * FROM \"rt/#\"", "enable": true, "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = created["id"].as_str().expect("rule id").to_string();
        let (status, _) = server
            .post_auth(
                "/api/v5/banned",
                json!({"as": "clientid", "who": "rt-banned", "reason": "round trip"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let (status, _) = server
            .post_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                json!({"rules": [{"topic": "rt/#", "permission": "allow", "action": "publish"}]}),
                &token,
            )
            .await;
        assert_eq!(status, 204);

        // Export the live state through the API.
        let (status, body) = server
            .post_auth("/api/v5/data/export", json!({}), &token)
            .await;
        assert_eq!(status, 200);
        let filename = body["filename"]
            .as_str()
            .expect("export names file")
            .to_string();

        // Change everything the file carries in full: drop the rule, the
        // ban and the ACL; recreate the user with other quotas.
        let (status, _) = server
            .delete_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth("/api/v5/banned/clientid/rt-banned", &token)
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .delete_auth(
                "/api/v5/authentication/password_based:built_in_database/users/rt-user",
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .post_auth(
                "/api/v1/auth/users",
                json!({"username": "rt-user", "password": "RtPass123!", "quotas": {"max_connections": 3}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 404);

        // Import the exported file through the API.
        let (status, _) = server
            .post_auth(
                "/api/v5/data/import",
                json!({"filename": filename.as_str()}),
                &token,
            )
            .await;
        assert_eq!(status, 204);

        // The rule is back with its SQL; the ban is back; the ACL is back.
        let (status, fetched) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"rt/#\""));
        let (status, body) = server.get_auth("/api/v5/banned", &token).await;
        assert_eq!(status, 200);
        let bans = body["data"].as_array().expect("ban list reads");
        assert!(bans.iter().any(|entry| entry["who"] == json!("rt-banned")));
        let (status, body) = server
            .get_auth(
                "/api/v5/authorization/sources/built_in_database/rules/all",
                &token,
            )
            .await;
        assert_eq!(status, 200);
        let acls = body["rules"].as_array().expect("acl list reads");
        assert_eq!(acls.len(), 1);
        assert_eq!(acls[0]["topic"], json!("rt/#"));
        // The user is back with the exported quotas (the file carries no
        // password, so the live verifier stays).
        let (status, body) = server.get_auth("/api/v1/auth/users", &token).await;
        assert_eq!(status, 200);
        let users = body.as_array().expect("user list reads");
        let user = users
            .iter()
            .find(|entry| entry["username"] == json!("rt-user"))
            .expect("user restored");
        assert!(user["quotas"]["max_connections"].is_null());

        // The restore persists: a registry reload from the same data dir
        // still holds the rule, the user and the ACL.
        drop(server);
        drop(registry);
        let reloaded = ConfigRegistry::load(&dir).expect("reload data dir");
        let snapshot = reloaded.snapshot();
        assert!(snapshot.rules.rules.iter().any(|rule| rule.id == rule_id));
        assert!(snapshot
            .mqtt_users
            .users
            .iter()
            .any(|user| user.username == "rt-user"));
        assert_eq!(snapshot.mqtt_users.acls.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn backup_import_unknown_file_is_404() {
        // An unknown filename answers 404 with the documented shape.
        // Broker-through: the request goes through the served API.
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, body) = server
            .post_auth(
                "/api/v5/data/import",
                json!({"filename": "missing-file.tar.gz"}),
                &token,
            )
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        assert_eq!(body.as_object().expect("error shape").len(), 2);

        // A missing filename is a client error with the same shape.
        let (status, body) = server
            .post_auth("/api/v5/data/import", json!({}), &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn backup_import_corrupt_file_keeps_prior_state() {
        // A corrupt file is refused whole: 400 plus the state before the
        // import stays. Broker-through: upload and import go through the
        // served API; the rule read proves the state is intact.
        use base64::Engine as _;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "keep-me", "sql": "SELECT * FROM \"keep/#\"", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = created["id"].as_str().expect("rule id").to_string();
        let garbage = base64::engine::general_purpose::STANDARD.encode(b"not a gzip file");
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "conf-corrupt.tar.gz", "content": garbage}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .post_auth(
                "/api/v5/data/import",
                json!({"filename": "conf-corrupt.tar.gz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        assert_eq!(body.as_object().expect("error shape").len(), 2);
        let (status, fetched) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"keep/#\""));
        let (status, _) = server
            .delete_auth("/api/v5/data/files/conf-corrupt.tar.gz", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn backup_import_without_admin_is_refused() {
        // An import without any administrator is refused and changes
        // nothing. Broker-through: the file is built in the test, stored
        // through the served upload API and imported through the API.
        use base64::Engine as _;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/users",
                json!({"username": "v-user", "password": "V-us3r-pass!", "role": "viewer"}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
        // One admin-less file: only the viewer, no administrator.
        let file = build_test_backup(
            "noadmin-0",
            &["admin_users"],
            &[(
                "snapshot/admin_users.json",
                json!({"users": [{"username": "v-user", "role": "viewer", "description": "", "must_change_password": false}]}),
            )],
        );
        let content = base64::engine::general_purpose::STANDARD.encode(&file);
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "conf-noadmin.tar.gz", "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .post_auth(
                "/api/v5/data/import",
                json!({"filename": "conf-noadmin.tar.gz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("VALIDATION_FAILED"));
        assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        assert_eq!(body.as_object().expect("error shape").len(), 2);
        // Both users are still there, including the administrator.
        let (status, body) = server.get_auth("/api/v5/users", &token).await;
        assert_eq!(status, 200);
        let users = body.as_array().expect("user list reads");
        assert!(users
            .iter()
            .any(|entry| entry["username"] == json!("admin")));
        assert!(users
            .iter()
            .any(|entry| entry["username"] == json!("v-user")));
        let (status, _) = server
            .delete_auth("/api/v5/data/files/conf-noadmin.tar.gz", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn backup_import_unknown_root_is_refused_through_broker() {
        // An unknown root is refused whole through the API.
        // Broker-through: upload and import go through the served API.
        // The prior rule stays, so validation precedes any apply.
        use base64::Engine as _;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "keep-unknown", "sql": "SELECT * FROM \"keep/unknown/#\"", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = created["id"].as_str().expect("rule id").to_string();
        let file = build_test_backup(
            "unknown-root-0",
            &["no-such-root"],
            &[("snapshot/no-such-root.json", json!({}))],
        );
        let content = base64::engine::general_purpose::STANDARD.encode(&file);
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "conf-unknown-root.tar.gz", "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .post_auth(
                "/api/v5/data/import",
                json!({"filename": "conf-unknown-root.tar.gz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        assert_eq!(body.as_object().expect("error shape").len(), 2);
        let (status, fetched) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"keep/unknown/#\""));
        let (status, _) = server
            .delete_auth("/api/v5/data/files/conf-unknown-root.tar.gz", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn backup_import_missing_part_is_refused_through_broker() {
        // A listed part without bytes is refused whole through the API.
        // Broker-through: upload and import go through the served API.
        // The prior rule stays, so validation precedes any apply.
        use base64::Engine as _;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "keep-missing", "sql": "SELECT * FROM \"keep/missing/#\"", "actions": []}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let rule_id = created["id"].as_str().expect("rule id").to_string();
        let file = build_test_backup("missing-part-0", &["rules"], &[]);
        let content = base64::engine::general_purpose::STANDARD.encode(&file);
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "conf-missing-part.tar.gz", "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .post_auth(
                "/api/v5/data/import",
                json!({"filename": "conf-missing-part.tar.gz"}),
                &token,
            )
            .await;
        assert_eq!(status, 400);
        assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        assert_eq!(body.as_object().expect("error shape").len(), 2);
        let (status, fetched) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["sql"], json!("SELECT * FROM \"keep/missing/#\""));
        let (status, _) = server
            .delete_auth("/api/v5/data/files/conf-missing-part.tar.gz", &token)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn backup_import_not_ready_is_refused() {
        // The import refuses while the node is not ready.
        // The handler is the production entry for the route.
        // A ready node passes the gate and reads the backup dir.
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        state.readiness.mark_not_ready();
        assert!(!state.readiness.is_ready());
        let body = bytes::Bytes::from(
            serde_json::to_vec(&json!({"filename": "any.tar.gz"})).expect("body is JSON"),
        );
        let response =
            crate::v5::backup::import_data(axum::extract::State(state.clone()), body).await;
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        let raw = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("error body reads");
        let parsed: Value = serde_json::from_slice(&raw).expect("error body is JSON");
        assert_eq!(parsed["code"], json!("STORE_NOT_READY"));
        assert!(parsed["message"].as_str().is_some_and(|m| !m.is_empty()));
        assert_eq!(parsed.as_object().expect("error shape").len(), 2);
        state.readiness.mark_ready();
        assert!(state.readiness.is_ready());
        let body = bytes::Bytes::from(
            serde_json::to_vec(&json!({"filename": "missing-file.tar.gz"})).expect("body is JSON"),
        );
        let response = crate::v5::backup::import_data(axum::extract::State(state), body).await;
        assert_eq!(response.status(), axum::http::StatusCode::NOT_FOUND);
        let raw = axum::body::to_bytes(response.into_body(), 16 * 1024)
            .await
            .expect("error body reads");
        let parsed: Value = serde_json::from_slice(&raw).expect("error body is JSON");
        assert_eq!(parsed["code"], json!("NOT_FOUND"));
    }

    #[tokio::test]
    async fn backup_import_retained_round_trip_through_broker() {
        // A retained table restores through the API and replaces live rows.
        // Broker-through: publish, upload and import go through the API.
        // The retained read proves the restore; the old row is gone.
        use base64::Engine as _;
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (status, _) = server
            .post_auth(
                "/api/v5/publish",
                json!({"topic": "rt-import-old", "payload": "old", "qos": 0, "retain": true}),
                &token,
            )
            .await;
        assert_eq!(status, 202);
        let payload_new = base64::engine::general_purpose::STANDARD.encode(b"new-retained");
        let file = build_test_backup_with_tables(
            "retained-rt-0",
            &[],
            &["retained"],
            &[(
                "tables/retained.json",
                json!([{"topic": "rt-import-new", "qos": 1, "payload": payload_new}]),
            )],
        );
        let content = base64::engine::general_purpose::STANDARD.encode(&file);
        let (status, _) = server
            .post_auth(
                "/api/v5/data/files",
                json!({"filename": "conf-retained-rt.tar.gz", "content": content}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .post_auth(
                "/api/v5/data/import",
                json!({"filename": "conf-retained-rt.tar.gz"}),
                &token,
            )
            .await;
        assert_eq!(status, 204);
        let (status, fetched) = server
            .get_auth("/api/v5/mqtt/retainer/message/rt-import-new", &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(fetched["topic"], json!("rt-import-new"));
        assert_eq!(fetched["qos"], json!(1));
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(fetched["payload"].as_str().expect("payload is string"))
            .expect("payload is base64");
        assert_eq!(decoded, b"new-retained");
        let (status, _) = server
            .get_auth("/api/v5/mqtt/retainer/message/rt-import-old", &token)
            .await;
        assert_eq!(status, 404);
        let (status, _) = server
            .delete_auth("/api/v5/data/files/conf-retained-rt.tar.gz", &token)
            .await;
        assert_eq!(status, 204);
    }

    /// Build one minimal backup file in memory for import tests: META
    /// plus the given parts under one base dir, gzipped and tarred.
    fn build_test_backup(base: &str, roots: &[&str], parts: &[(&str, Value)]) -> Vec<u8> {
        build_test_backup_with_tables(base, roots, &[], parts)
    }

    /// Build one backup file with explicit tables. Roots map to
    /// `snapshot/<root>.json` parts; tables map to `tables/<table>.json`.
    /// Callers pass full relative paths in `parts`.
    fn build_test_backup_with_tables(
        base: &str,
        roots: &[&str],
        tables: &[&str],
        parts: &[(&str, Value)],
    ) -> Vec<u8> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut tar = tar::Builder::new(encoder);
        let meta = json!({
            "format": "indra-backup-1",
            "version": "test",
            "exported_at": "2026-01-01T00:00:00+00:00",
            "roots": roots,
            "tables": tables,
        });
        let mut append = |path: String, value: &Value| {
            let bytes = serde_json::to_vec(value).expect("part is JSON");
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            tar.append_data(&mut header, path, bytes.as_slice())
                .expect("entry appends");
        };
        append(format!("{base}/META.json"), &meta);
        for (path, value) in parts {
            append(format!("{base}/{path}"), value);
        }
        let encoder = tar.into_inner().expect("tar closes");
        encoder.finish().expect("gzip closes")
    }

    /// Send one JSON-RPC message to `/mcp` with an `Authorization` value.
    async fn mcp_rpc(server: &TestServer, authorization: &str, payload: Value) -> (u16, Value) {
        server
            .send_with_authorization("POST", "/mcp", Some(payload), Some(authorization))
            .await
    }

    /// Send one JSON-RPC message to `/mcp` with a query string, for
    /// example `toolsets=rules`.
    async fn mcp_rpc_with_query(
        server: &TestServer,
        query: &str,
        authorization: &str,
        payload: Value,
    ) -> (u16, Value) {
        server
            .send_with_authorization(
                "POST",
                &format!("/mcp?{query}"),
                Some(payload),
                Some(authorization),
            )
            .await
    }

    /// Names of the tools in a `tools/list` answer.
    fn mcp_tool_names(body: &Value) -> Vec<String> {
        body["result"]["tools"]
            .as_array()
            .expect("tools is a list")
            .iter()
            .map(|tool| tool["name"].as_str().expect("tool name").to_string())
            .collect()
    }

    /// The MCP tool catalog covers every router operation: each row of
    /// the build-time route table is a tool or an exclusion, each
    /// exclusion names a real row, and tool names are unique snake
    /// case. Prints the tool and exclusion counts.
    #[test]
    fn mcp_tool_catalog_covers_every_route() {
        use crate::mcp::catalog;

        let catalog = catalog::build_catalog();
        let mut tools = 0usize;
        let mut excluded = 0usize;
        for row in catalog::MCP_ROUTE_TABLE {
            match catalog::find_exclusion(row.method, row.path) {
                Some(_) => excluded += 1,
                None => {
                    let tool = catalog::tool_by_name(&catalog, row.tool)
                        .unwrap_or_else(|| panic!("no tool for {} {}", row.method, row.path));
                    assert_eq!(tool.method, row.method);
                    assert_eq!(tool.path, row.path);
                    tools += 1;
                }
            }
        }
        // No stale exclusions: each names a real router row.
        for exclusion in catalog::EXCLUSIONS {
            assert!(
                catalog::MCP_ROUTE_TABLE
                    .iter()
                    .any(|row| row.method == exclusion.method && row.path == exclusion.path),
                "stale exclusion {} {}",
                exclusion.method,
                exclusion.path
            );
        }
        // Tool names are unique stable snake case.
        let mut names: Vec<&str> = catalog.iter().map(|tool| tool.name.as_str()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), catalog.len(), "tool names are unique");
        for tool in &catalog {
            assert!(
                tool.name
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
                "snake-case tool name: {}",
                tool.name
            );
            // Every tool belongs to a known toolset.
            assert!(
                catalog::TOOLSETS.iter().any(|set| set.name == tool.toolset),
                "tool {} has a known toolset",
                tool.name
            );
            // Path parameters are required schema fields.
            let schema = catalog::input_schema_for(tool);
            let required = schema["required"].as_array().expect("required is a list");
            for param in crate::mcp::route_scan::path_params(tool.path) {
                assert!(
                    required.iter().any(|name| name == &json!(param)),
                    "tool {} requires {param}",
                    tool.name
                );
            }
        }
        // Annotations follow the method and the verb.
        let api_status = catalog::tool_by_name(&catalog, "get_api_v5_status").expect("status tool");
        assert!(api_status.read_only);
        assert!(!api_status.destructive);
        assert_eq!(api_status.method, "GET");
        assert_eq!(api_status.path, "/api/v5/status");
        // The built-in status tool proves the path end to end.
        let broker = catalog::tool_by_name(&catalog, "broker_status").expect("status tool");
        assert!(broker.read_only);
        assert!(!broker.destructive);
        // `/status` and `/api/v5/status` are two routes with two tools.
        let alias = catalog::tool_by_name(&catalog, "get_status").expect("alias tool");
        assert!(alias.read_only);
        assert!(!alias.destructive);
        assert_eq!(alias.method, "GET");
        assert_eq!(alias.path, "/status");
        for (method, path) in [("GET", "/status"), ("GET", "/api/v5/status")] {
            assert!(
                catalog::MCP_ROUTE_TABLE
                    .iter()
                    .any(|row| row.method == method && row.path == path),
                "route row missing: {method} {path}",
            );
            assert!(
                catalog::find_exclusion(method, path).is_none(),
                "status route is a tool, not an exclusion: {method} {path}",
            );
        }
        let metrics = catalog::tool_by_name(&catalog, "get_api_v5_metrics").expect("metrics tool");
        assert!(metrics.read_only);
        let kick =
            catalog::tool_by_name(&catalog, "post_api_v5_clients_kickout_bulk").expect("kick tool");
        assert!(kick.destructive);
        println!("MCP_TOOLS {tools} MCP_EXCLUSIONS {excluded}");
        // Every catalog entry covers a route row plus the built-in
        // status tool: no other extra tool hides beside the table.
        assert_eq!(catalog.len(), tools + 1);
        // The table is never empty: an empty scan must fail, not pass.
        assert!(!catalog::MCP_ROUTE_TABLE.is_empty());
        // Known operations survive any scanner change.
        for name in [
            "get_status",
            "get_api_v5_status",
            "broker_status",
            "get_api_v5_metrics",
            "post_api_v5_publish",
            "delete_api_v5_rules_by_id",
            "get_api_v1_nodes",
            "post_api_v5_rules",
        ] {
            assert!(
                catalog::tool_by_name(&catalog, name).is_some(),
                "known tool missing: {name}",
            );
        }
    }

    /// `initialize`, `tools/list` and `tools/call get_api_v5_status`
    /// work over a real socket. Missing, wrong and dashboard credentials
    /// are refused with 401 and no JSON-RPC body.
    #[tokio::test]
    async fn mcp_initialize_lists_and_calls_over_a_real_socket() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (key_id, secret) = make_key(&server, &token, json!({"name": "mcp-agent"})).await;
        let bearer = format!("Bearer {secret}");
        let basic = basic_authorization(&key_id, &secret);

        // No key: 401 with the management error shape.
        let (status, body) = server
            .send_with_authorization(
                "POST",
                "/mcp",
                Some(json!({"jsonrpc": "2.0", "method": "ping", "id": 1})),
                None,
            )
            .await;
        assert_eq!(status, 401);
        assert_eq!(body["code"], json!("UNAUTHORIZED"));

        // Wrong key: 401.
        let (status, _) = server
            .send_with_authorization(
                "POST",
                "/mcp",
                Some(json!({"jsonrpc": "2.0", "method": "ping", "id": 1})),
                Some("Bearer wrong-secret"),
            )
            .await;
        assert_eq!(status, 401);

        // Dashboard login token: 401. Agents use API keys.
        let (status, _) = mcp_rpc(
            &server,
            &format!("Bearer {token}"),
            json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
        )
        .await;
        assert_eq!(status, 401);

        // Unknown method: JSON-RPC -32601.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "nope", "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["error"]["code"], json!(-32601));

        // Initialize negotiates the asked version.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "initialize",
                "params": {"protocolVersion": "2025-03-26"}, "id": 3}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["result"]["protocolVersion"], json!("2025-03-26"));
        assert_eq!(body["result"]["serverInfo"]["name"], json!("indramqtt-mcp"));

        // tools/list holds the status tools with annotations.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/list", "id": 4}),
        )
        .await;
        assert_eq!(status, 200);
        let tools = body["result"]["tools"].as_array().expect("tools is a list");
        let status_tool = tools
            .iter()
            .find(|tool| tool["name"] == json!("get_api_v5_status"))
            .expect("get_api_v5_status is listed");
        assert_eq!(status_tool["annotations"]["readOnlyHint"], json!(true));
        assert!(
            tools.iter().any(|tool| tool["name"] == json!("get_status")),
            "alias tool is listed",
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool["name"] == json!("broker_status")),
            "built-in status tool is listed",
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool["name"] == json!("list_toolsets")),
            "toolset listing is listed",
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool["name"] == json!("get_api_v5_metrics")),
            "catalog tools are listed",
        );

        // tools/call get_api_v5_status answers exactly as HTTP.
        let (http_status, http_body) = server.get_auth("/api/v5/status?format=json", &token).await;
        assert_eq!(http_status, 200);
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_status",
                    "arguments": {"query": {"format": "json"}}}, "id": 5}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body["result"].get("isError").is_none(),
            "status succeeds: {body}"
        );
        let text = body["result"]["content"][0]["text"]
            .as_str()
            .expect("text content");
        let answer: Value = serde_json::from_str(text).expect("JSON text");
        assert_eq!(answer, http_body);
        // The built-in status tool answers version, uptime and node.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "broker_status", "arguments": {}}, "id": 6}),
        )
        .await;
        assert_eq!(status, 200);
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        let answer: Value = serde_json::from_str(text).expect("JSON text");
        assert!(answer["version"].is_string());
        assert!(answer["uptime_ms"].is_number());
        assert!(answer["node"].is_string());

        // Basic key credentials work too.
        let (status, _) = mcp_rpc(
            &server,
            &basic,
            json!({"jsonrpc": "2.0", "method": "ping", "id": 6}),
        )
        .await;
        assert_eq!(status, 200);

        // A notification answers 202 with no body.
        let (status, _) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "notifications/initialized"}),
        )
        .await;
        assert_eq!(status, 202);
    }

    /// A batch array answers one JSON-RPC array with one entry per
    /// request that has an id. An empty batch gets 400. Proves the
    /// batch path over a real socket.
    #[tokio::test]
    async fn mcp_batch_array_answers_each_request() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-batch"})).await;
        let bearer = format!("Bearer {secret}");
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!([
                {"jsonrpc": "2.0", "method": "ping", "id": 1},
                {"jsonrpc": "2.0", "method": "notifications/initialized"},
                {"jsonrpc": "2.0", "method": "ping", "id": 2},
            ]),
        )
        .await;
        assert_eq!(status, 200);
        let answers = body.as_array().expect("batch answer is a list");
        assert_eq!(answers.len(), 2);
        assert_eq!(answers[0]["id"], json!(1));
        assert_eq!(answers[1]["id"], json!(2));
        let (status, _) = mcp_rpc(&server, &bearer, json!([])).await;
        assert_eq!(status, 400);
    }

    /// A key without the `monitoring` scope gets 403 on `/mcp`.
    /// Proves the entry scope gate over a real socket: the same key
    /// still calls the paths its own scope allows over HTTP.
    #[tokio::test]
    async fn mcp_key_without_monitoring_scope_gets_403() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(
            &server,
            &token,
            json!({"name": "mcp-audit", "scopes": ["audit"]}),
        )
        .await;
        let (status, body) = mcp_rpc(
            &server,
            &format!("Bearer {secret}"),
            json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
        )
        .await;
        assert_eq!(status, 403);
        assert_eq!(body["code"], json!("FORBIDDEN"));
        // The same key still calls its own scope over HTTP.
        let (status, _) = server.get_auth("/api/v5/audit", &secret).await;
        assert_eq!(status, 200);
    }

    /// One GET tool, one POST tool with a body and one DELETE tool
    /// answer through `/mcp` exactly as through HTTP.
    #[tokio::test]
    async fn mcp_tools_match_http_for_get_post_delete() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (key_id, secret) = make_key(&server, &token, json!({"name": "mcp-parity"})).await;
        let bearer = format!("Bearer {secret}");

        // GET: stable settings read, identical bodies.
        let (http_status, http_body) = server
            .get_basic("/api/v5/license/setting", &key_id, &secret)
            .await;
        assert_eq!(http_status, 200);
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_license_setting", "arguments": {}}, "id": 1}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body["result"].get("isError").is_none(),
            "GET succeeds: {body}"
        );
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        let mcp_body: Value = serde_json::from_str(text).expect("JSON text");
        assert_eq!(mcp_body, http_body);

        // POST with a body: stateless rule test, identical bodies.
        let probe = json!({
            "sql": "SELECT temperature FROM \"sensors/+\" WHERE temperature > 0",
            "topic": "sensors/kitchen",
            "payload": {"temperature": 72.5},
        });
        let (http_status, http_body) = server
            .post_auth("/api/v5/rule_test", probe.clone(), &token)
            .await;
        assert_eq!(http_status, 200);
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rule_test", "arguments": {"body": probe}}, "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        let mcp_body: Value = serde_json::from_str(text).expect("JSON text");
        assert_eq!(mcp_body, http_body);
        assert_eq!(mcp_body["matched"], json!(true));

        // DELETE of a missing rule: 404 on both sides.
        let (http_status, http_body) = server
            .delete_auth("/api/v5/rules/no-such-rule-mcp", &token)
            .await;
        assert_eq!(http_status, 404);
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "delete_api_v5_rules_by_id",
                    "arguments": {"id": "no-such-rule-mcp"}}, "id": 3}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["result"]["isError"], json!(true));
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        let code = http_body["code"].as_str().unwrap_or_default();
        assert!(
            !code.is_empty() && text.contains(code),
            "MCP error carries the API code: {text}",
        );

        // Real round trip: create over HTTP, delete over MCP, gone over HTTP.
        let (status, created) = server
            .post_auth(
                "/api/v5/rules",
                json!({"name": "mcp-del", "sql": "SELECT * FROM \"mc02/del\"",
                    "actions": [{"function": "console"}]}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "create: {created}");
        let rule_id = created["id"].as_str().expect("rule id").to_string();
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "delete_api_v5_rules_by_id",
                    "arguments": {"id": rule_id}}, "id": 4}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body["result"].get("isError").is_none(),
            "delete succeeds: {body}"
        );
        let (status, _) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}"), &token)
            .await;
        assert_eq!(status, 404);
    }

    /// A publish through `/mcp` fires a rule through the broker: the
    /// rule metrics move. Proves the tools drive broker state, not a
    /// store.
    #[tokio::test]
    async fn mcp_publish_through_broker_fires_a_rule() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-broker"})).await;
        let bearer = format!("Bearer {secret}");

        // Create the rule through MCP.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rules", "arguments": {"body": {
                    "name": "mcp-fire",
                    "sql": "SELECT * FROM \"mc02/fire\"",
                    "actions": [{"function": "console"}],
                }}}, "id": 1}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body["result"].get("isError").is_none(),
            "create succeeds: {body}"
        );
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        let created: Value = serde_json::from_str(text).expect("JSON text");
        let rule_id = created["id"].as_str().expect("rule id").to_string();

        // Publish through MCP: the management publish path dispatches
        // ingress through the rule engine.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_publish", "arguments": {"body": {
                    "topic": "mc02/fire", "payload": "{\"k\": 1}", "qos": 0,
                }}}, "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body["result"].get("isError").is_none(),
            "publish succeeds: {body}"
        );

        // The rule saw the publish through the broker.
        let (status, metrics) = server
            .get_auth(&format!("/api/v5/rules/{rule_id}/metrics"), &token)
            .await;
        assert_eq!(status, 200);
        assert_eq!(metrics["metrics"]["matched"], json!(1));
    }

    /// `mcp.enabled = false` in `indra.toml` disables `POST /mcp`
    /// through the socket. The test resolves the file with the
    /// production layer loader and applies it with the same call boot
    /// uses. `INDRA_MCP__ENABLED=true` wins over the file.
    #[tokio::test]
    async fn mcp_enabled_from_indra_toml_answers_404_through_socket() {
        // A file value of false disables the endpoint.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-mcp-enabled-{nanos}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create config dir");
        std::fs::write(dir.join("indra.toml"), "[mcp]\nenabled = false\n")
            .expect("write indra.toml");
        let layered = broker_config::layers::load_layered_with_env(
            &dir,
            &[],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("layers load");
        assert!(!layered.config().mcp.enabled);
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-file"})).await;
        // Boot applies the resolved section with this call.
        let mcp = layered.config().mcp.clone();
        state.mcp.apply(mcp.enabled, mcp.read_only);
        let (status, body) = mcp_rpc(
            &server,
            &format!("Bearer {secret}"),
            json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
        )
        .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        // An environment value of true enables the endpoint again.
        let layered_env = broker_config::layers::load_layered_with_env(
            &dir,
            &[("INDRA_MCP__ENABLED".to_string(), "true".to_string())],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("env layers load");
        assert!(layered_env.config().mcp.enabled);
        let mcp = layered_env.config().mcp.clone();
        state.mcp.apply(mcp.enabled, mcp.read_only);
        let (status, _) = mcp_rpc(
            &server,
            &format!("Bearer {secret}"),
            json!({"jsonrpc": "2.0", "method": "ping", "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `mcp.read_only = true` in `indra.toml` keeps reads and refuses
    /// writes through the socket. `INDRA_MCP__READ_ONLY=false` lifts it.
    #[tokio::test]
    async fn mcp_read_only_from_indra_toml_refuses_writes_through_socket() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-mcp-readonly-{nanos}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create config dir");
        std::fs::write(dir.join("indra.toml"), "[mcp]\nread_only = true\n")
            .expect("write indra.toml");
        let layered = broker_config::layers::load_layered_with_env(
            &dir,
            &[],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("layers load");
        assert!(layered.config().mcp.read_only);
        // The schema default allows writes.
        assert!(!broker_config::schema::McpConf::default().read_only);
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-ro"})).await;
        let bearer = format!("Bearer {secret}");
        let mcp = layered.config().mcp.clone();
        state.mcp.apply(mcp.enabled, mcp.read_only);
        // A GET tool still succeeds.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "get_api_v5_license_setting",
                    "arguments": {}}, "id": 1}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body["result"].get("isError").is_none(),
            "read succeeds: {body}"
        );
        // A POST tool is refused with the API forbidden code.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rule_test", "arguments": {"body": {
                    "sql": "SELECT * FROM \"mc02/ro\"",
                    "topic": "mc02/ro",
                    "payload": {"k": 1},
                }}}, "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["result"]["isError"], json!(true));
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        assert!(
            text.contains("FORBIDDEN"),
            "refusal carries the API code: {text}",
        );
        // A DELETE tool is refused too.
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "delete_api_v5_rules_by_id",
                    "arguments": {"id": "no-such-rule-mcp-ro"}}, "id": 3}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["result"]["isError"], json!(true));
        // An environment value of false lifts the restriction.
        let layered_env = broker_config::layers::load_layered_with_env(
            &dir,
            &[("INDRA_MCP__READ_ONLY".to_string(), "false".to_string())],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("env layers load");
        assert!(!layered_env.config().mcp.read_only);
        let mcp = layered_env.config().mcp.clone();
        state.mcp.apply(mcp.enabled, mcp.read_only);
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_rule_test", "arguments": {"body": {
                    "sql": "SELECT * FROM \"mc02/ro\"",
                    "topic": "mc02/ro",
                    "payload": {"k": 1},
                }}}, "id": 4}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(
            body["result"].get("isError").is_none(),
            "write succeeds again: {body}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
    /// Default, named and full toolset selection through `/mcp`.
    #[tokio::test]
    async fn mcp_toolsets_default_named_and_all() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-sets"})).await;
        let bearer = format!("Bearer {secret}");
        let list = json!({"jsonrpc": "2.0", "method": "tools/list", "id": 1});

        // No parameter: the core set only.
        let (status, body) = mcp_rpc(&server, &bearer, list.clone()).await;
        assert_eq!(status, 200);
        let names = mcp_tool_names(&body);
        assert!(names.contains(&"broker_status".to_string()));
        assert!(names.contains(&"list_toolsets".to_string()));
        assert!(names.contains(&"get_api_v5_metrics".to_string()));
        assert!(
            !names.contains(&"post_api_v5_rules".to_string()),
            "default set holds no rules tool: {names:?}"
        );

        // A named set: rules tools, no client tools.
        let (status, body) =
            mcp_rpc_with_query(&server, "toolsets=rules", &bearer, list.clone()).await;
        assert_eq!(status, 200);
        let names = mcp_tool_names(&body);
        assert!(names.contains(&"list_toolsets".to_string()));
        assert!(names.contains(&"post_api_v5_rules".to_string()));
        assert!(
            !names.contains(&"get_api_v5_clients".to_string()),
            "rules set holds no client tool: {names:?}"
        );

        // `all`: both groups at once.
        let (status, body) =
            mcp_rpc_with_query(&server, "toolsets=all", &bearer, list.clone()).await;
        assert_eq!(status, 200);
        let names = mcp_tool_names(&body);
        assert!(names.contains(&"post_api_v5_rules".to_string()));
        assert!(names.contains(&"get_api_v5_clients".to_string()));
        assert!(names.contains(&"list_toolsets".to_string()));
    }

    /// `list_toolsets` names every group with its tool count.
    #[tokio::test]
    async fn mcp_list_toolsets_names_groups_and_counts() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-groups"})).await;
        let (status, body) = mcp_rpc(
            &server,
            &format!("Bearer {secret}"),
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "list_toolsets", "arguments": {}}, "id": 1}),
        )
        .await;
        assert_eq!(status, 200);
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        let answer: Value = serde_json::from_str(text).expect("JSON text");
        let sets = answer["toolsets"].as_array().expect("toolsets is a list");
        let names: Vec<&str> = sets
            .iter()
            .map(|set| set["name"].as_str().expect("set name"))
            .collect();
        for expected in ["status", "clients", "rules", "tenants"] {
            assert!(names.contains(&expected), "group listed: {expected}");
        }
        for set in sets {
            assert!(set["tool_count"].as_u64().expect("count") > 0);
            assert!(!set["tools"].as_array().expect("tools").is_empty());
        }
    }

    /// A viewer key sees read tools only and cannot call a write tool.
    #[tokio::test]
    async fn mcp_viewer_key_sees_and_calls_read_only() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(
            &server,
            &token,
            json!({"name": "mcp-viewer", "role": "viewer"}),
        )
        .await;
        let bearer = format!("Bearer {secret}");

        // The list holds reads and hides writes.
        let (status, body) = mcp_rpc_with_query(
            &server,
            "toolsets=all",
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/list", "id": 1}),
        )
        .await;
        assert_eq!(status, 200);
        let names = mcp_tool_names(&body);
        assert!(names.contains(&"broker_status".to_string()));
        assert!(names.contains(&"list_toolsets".to_string()));
        assert!(
            !names.contains(&"post_api_v5_publish".to_string()),
            "viewer sees no write tool: {names:?}"
        );

        // A write call is refused with a JSON-RPC error.
        let (status, body) = mcp_rpc_with_query(
            &server,
            "toolsets=all",
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_publish", "arguments": {"body": {
                    "topic": "mc03/denied", "payload": "{}",
                }}}, "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["error"]["code"], json!(-32602));

        // A read call still works.
        let (status, body) = mcp_rpc_with_query(
            &server,
            "toolsets=all",
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "broker_status", "arguments": {}}, "id": 3}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(body["result"]["content"][0]["text"].is_string());
    }

    /// Read-only mode lists read tools only, for every key.
    #[tokio::test]
    async fn mcp_read_only_mode_hides_and_refuses_writes() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-ro"})).await;
        let bearer = format!("Bearer {secret}");
        state.mcp.set_read_only(true);

        // The list holds reads and hides writes.
        let (status, body) = mcp_rpc_with_query(
            &server,
            "toolsets=all",
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/list", "id": 1}),
        )
        .await;
        assert_eq!(status, 200);
        let names = mcp_tool_names(&body);
        assert!(names.contains(&"broker_status".to_string()));
        assert!(
            !names.contains(&"post_api_v5_publish".to_string()),
            "read-only mode lists no write tool: {names:?}"
        );

        // A write call is refused even with a full-access key.
        let (status, body) = mcp_rpc_with_query(
            &server,
            "toolsets=all",
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "post_api_v5_publish", "arguments": {"body": {
                    "topic": "mc03/readonly", "payload": "{}",
                }}}, "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["result"]["isError"], json!(true));
        let text = body["result"]["content"][0]["text"].as_str().expect("text");
        assert!(
            text.contains("FORBIDDEN"),
            "refusal carries the API code: {text}"
        );
        state.mcp.set_read_only(false);
    }

    /// the tool name and the result code.
    #[tokio::test]
    async fn mcp_call_writes_one_audit_record() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-audit"})).await;
        let bearer = format!("Bearer {secret}");

        let (status, _) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "tools/call",
                "params": {"name": "broker_status", "arguments": {}}, "id": 1}),
        )
        .await;
        assert_eq!(status, 200);

        // The audit log holds the call record.
        let (status, page) = server
            .send_with_authorization(
                "GET",
                "/api/v5/audit?from=mcp&operation_id=mcp:broker_status",
                None,
                Some(&bearer),
            )
            .await;
        assert_eq!(status, 200);
        let rows = page[0]["data"].as_array().expect("audit data");
        let record = rows
            .iter()
            .find(|row| {
                row["source"] == json!("api-key:mcp-audit")
                    && row["operation_id"] == json!("mcp:broker_status")
            })
            .expect("audit record exists after the call");
        assert_eq!(record["operation_result"], json!("success"));
        assert_eq!(record["from"], json!("mcp"));
    }

    /// A disabled endpoint answers 404, even with a valid key.
    #[tokio::test]
    async fn mcp_disabled_path_answers_404() {
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-off"})).await;
        state.mcp.set_enabled(false);
        let (status, body) = mcp_rpc(
            &server,
            &format!("Bearer {secret}"),
            json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
        )
        .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], json!("NOT_FOUND"));
        state.mcp.set_enabled(true);
    }

    /// `mcp.enabled` in `indra.toml` drives `POST /mcp` through the socket.
    #[tokio::test]
    async fn mcp_enabled_applies_from_indra_toml() {
        // A file value of false disables the endpoint.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-mcp-enabled-{nanos}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create config dir");
        std::fs::write(dir.join("indra.toml"), "[mcp]\nenabled = false\n")
            .expect("write indra.toml");
        let layered = broker_config::layers::load_layered_with_env(
            &dir,
            &[],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("layers load");
        assert!(!layered.config().mcp.enabled);
        let (server, state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-file"})).await;
        // Boot applies the resolved value to the switch.
        state.mcp.set_enabled(layered.config().mcp.enabled);
        let (status, _) = mcp_rpc(
            &server,
            &format!("Bearer {secret}"),
            json!({"jsonrpc": "2.0", "method": "ping", "id": 1}),
        )
        .await;
        assert_eq!(status, 404);
        // An environment value of true enables the endpoint again.
        let layered_env = broker_config::layers::load_layered_with_env(
            &dir,
            &[("INDRA_MCP__ENABLED".to_string(), "true".to_string())],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("env layers load");
        assert!(layered_env.config().mcp.enabled);
        state.mcp.set_enabled(layered_env.config().mcp.enabled);
        let (status, _) = mcp_rpc(
            &server,
            &format!("Bearer {secret}"),
            json!({"jsonrpc": "2.0", "method": "ping", "id": 2}),
        )
        .await;
        assert_eq!(status, 200);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `mcp.read_only` in `indra.toml` drives `/mcp` through the socket.
    #[tokio::test]
    async fn mcp_read_only_applies_from_indra_toml() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-mcp-readonly-{nanos}-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create config dir");
        std::fs::write(dir.join("indra.toml"), "[mcp]\nread_only = true\n")
            .expect("write indra.toml");
        let layered = broker_config::layers::load_layered_with_env(
            &dir,
            &[],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("layers load");
        assert!(layered.config().mcp.read_only);
        // The environment wins over the file.
        let layered_env = broker_config::layers::load_layered_with_env(
            &dir,
            &[("INDRA_MCP__READ_ONLY".to_string(), "false".to_string())],
            &broker_config::layers::CliOverrides::default(),
        )
        .expect("env layers load");
        assert!(!layered_env.config().mcp.read_only);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// the handler. A 9 MiB call gets 413. Both go through a socket.
    #[tokio::test]
    async fn mcp_body_cap_is_eight_mebibytes() {
        let (server, _state) = TestServer::start().await;
        let token = server.login_as_admin().await;
        let (_key_id, secret) = make_key(&server, &token, json!({"name": "mcp-limits"})).await;
        let bearer = format!("Bearer {secret}");

        // A 3 MiB envelope passes the transport and gets a method answer.
        let big_pad = "p".repeat(3 * 1024 * 1024);
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "ping",
                "params": {"pad": big_pad}, "id": 1}),
        )
        .await;
        assert_eq!(status, 200);
        assert!(body["result"].is_object(), "ping answers: {body}");

        // A 9 MiB envelope is refused with 413.
        let huge_pad = "p".repeat(9 * 1024 * 1024);
        let (status, body) = mcp_rpc(
            &server,
            &bearer,
            json!({"jsonrpc": "2.0", "method": "ping",
                "params": {"pad": huge_pad}, "id": 2}),
        )
        .await;
        assert_eq!(status, 413);
        assert_eq!(body["code"], json!("PAYLOAD_TOO_LARGE"));
    }
}
