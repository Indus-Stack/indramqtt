//! v5 REST API compatibility router for IndraMQTT.

pub mod actions;
pub mod alarms;
pub mod api_keys;
pub mod audit;
pub mod auth;
pub mod authn_cache;
pub mod authn_chain;
pub mod authn_settings;
pub mod authn_status;
pub mod authz_cache;
pub mod authz_clients;
pub mod authz_node_cache;
pub mod authz_node_status;
pub mod authz_settings;
pub mod authz_sources;
pub mod auto_subscribe;
pub mod backup;
pub mod banned;
pub mod certs;
pub mod clients;
pub mod cluster;
pub mod configs;
pub mod data;
pub mod delayed;
pub mod empty_states;
pub mod error_codes;
pub mod features;
pub mod gateways;
pub mod license;
pub mod listeners;
pub mod mfa;
pub mod monitor;
pub mod monitoring;
pub mod mt;
pub mod nodes;
pub mod prometheus;
pub mod retainer;
pub mod rewrite;
pub mod rule_engine;
pub mod rule_events;
pub mod rule_test;
pub mod rules;
pub mod schema_registry;
pub mod schemas;
pub mod slow_subscriptions;
pub mod sources;
pub mod sso;
pub mod status;
pub mod topic_metrics;
pub mod trace;
pub mod tracing;
pub mod transformations;
pub mod typed;

use crate::ApiState;
use axum::{
    routing::{delete, get, post, put},
    Router,
};

/// Public `/api/v5/*` endpoints: login only. These stay outside the
/// authentication middleware; everything else requires a bearer token.
pub fn public_router() -> Router<ApiState> {
    Router::new()
        .route("/login/challenge", post(auth::scram_challenge))
        .route("/login/verify", post(auth::scram_verify))
        .route("/login", post(auth::direct_login))
        // Health check. It needs no authentication, as the reference API
        // documents. One relaxed atomic load per request; publish and
        // deliver never touch it.
        .route("/status", get(status::get_status))
        // Load rebalance availability check (AP-01): ready while the node
        // evacuates nothing. Public by definition: the reference API
        // requires no credentials here. Management-plane only; publish
        // and deliver never touch it.
        .route(
            "/load_rebalance/availability_check",
            get(empty_states::get_rebalance_availability),
        )
        // Single-sign-on backend login (W2-33, C05-12): pre-auth by
        // definition (the dashboard login page calls it before anyone
        // holds a token). Management-plane only; publish and deliver
        // never touch the backend registry or the pending-flow table.
        .route("/sso/login/:backend", post(sso::login))
        // Single-sign-on OIDC callback (W2-35, C05-16): completes the
        // code flow against the stored backend config and the bounded
        // single-use pending-flow table from the login task. Pre-auth by
        // definition (the provider redirects the browser here before
        // anyone holds a token). Management-plane only; publish and
        // deliver never touch the backend registry or the pending table.
        .route("/sso/oidc/callback", get(sso::oidc_callback))
        // Single-sign-on running state (W2-36, C05-17): enabled backend
        // names over the real registry. Pre-auth by definition (the
        // login page probes it before anyone holds a token). The route
        // is not paged, reflects live state, and carries only names.
        // Management-plane only; publish and deliver never touch it.
        .route("/sso/running", get(sso::list_running))
        // Single-sign-on SAML assertion consumer (W2-37, C05-18):
        // consumes the provider response against the stored backend and
        // the bounded single-use pending-flow table from the login task,
        // issuing a live bearer on success. Pre-auth by definition (the
        // provider posts the browser here before anyone holds a token).
        // Management-plane only; publish and deliver never touch the
        // backend registry or the pending table.
        .route("/sso/saml/acs", post(sso::saml_acs))
        // Single-sign-on SAML service-provider metadata (W2-37, C05-19):
        // bounded static document rendered from the stored SAML slot.
        // Pre-auth by definition (provider operators fetch it before
        // anyone holds a token). The route is not paged.
        // Management-plane only; publish and deliver never touch it.
        .route("/sso/saml/metadata", get(sso::saml_metadata))
        // Single-sign-on token exchange (W2-38, C05-20): exchanges one
        // backend-issued grant (a single-use pending login flow from the
        // login task) for a live bearer against the stored backend config.
        // Pre-auth by definition (the dashboard login page calls it before
        // anyone holds a token). The route is not paged.
        // Management-plane only; publish and deliver never touch the
        // backend registry or the pending table.
        .route("/sso/token_exchange", post(sso::token_exchange))
}

/// Authenticated `/api/v5/*` endpoints. Served behind the
/// [`crate::api_auth::require_api_auth`] middleware.
pub fn protected_router() -> Router<ApiState> {
    Router::new()
        // Auth & Identity
        .route("/logout", post(auth::logout))
        .route("/current_user", get(auth::current_user))
        .route("/user_scopes", get(auth::user_scopes))
        .route("/users", get(auth::list_users).post(auth::create_user))
        .route(
            "/users/:username",
            delete(auth::delete_user).put(auth::update_user),
        )
        .route(
            "/users/:username/change_pwd",
            put(auth::change_user_password).post(auth::change_user_password),
        )
        // Dashboard per-user MFA enrol and disable (W2-44, C05-30/C05-31):
        // TOTP enrol returning the provisioning fields plus duplicate
        // rejection, and a disable that clears the enrolment, both on the
        // real admin-user MFA state shared with the setup/verify routes.
        // Neither route is paged; misses fail closed with the documented
        // `{code, message}` shape. Management-plane only; publish and
        // deliver never touch the user map or the attempt counters.
        .route(
            "/users/:username/mfa",
            post(mfa::enrol).delete(mfa::disable),
        )
        // Authentication (AuthN) chain: ordered list plus append, plus
        // whole-order replacement. Management-plane only; the CONNECT
        // path reads a lock-free snapshot and publish/deliver never
        // touch this store.
        .route(
            "/authentication",
            get(authn_chain::list_authn_chain).post(authn_chain::create_authn_chain),
        )
        .route(
            "/authentication/order",
            put(authn_chain::replace_authn_order),
        )
        .route(
            "/authentication/:id",
            get(authn_chain::get_authn_entry)
                .put(authn_chain::put_authn_entry)
                .delete(authn_chain::delete_authn_entry),
        )
        // Authentication (AuthN) position move: bounds-checked splice of
        // one entry to a zero-based position on the real chain store.
        // Management-plane only; the CONNECT path reads a lock-free
        // snapshot and publish/deliver never touch this store.
        .route(
            "/authentication/:id/position/:position",
            put(authn_chain::move_authn_position),
        )
        // Authentication (AuthN) per-id status: liveness plus live
        // counters over the real chain entry. Management-plane only; the
        // CONNECT path attributes one verdict per connect and
        // publish/deliver never touch this store.
        .route(
            "/authentication/:id/status",
            get(authn_status::get_authn_status),
        )
        // Authentication (AuthN) per-id metrics reset: zeroes the live
        // atomic counters for one authenticator. Management-plane only;
        // increments stay lock-free atomics and publish/deliver never
        // touch these counters.
        .route(
            "/authentication/:id/metrics/reset",
            post(authn_status::reset_authn_metrics),
        )
        // Authentication (AuthN) node cache: status read plus reset that
        // actually evicts entries. Management-plane only; the CONNECT
        // path records successes, publish and deliver never touch it.
        .route(
            "/authentication/node_cache/status",
            get(authn_cache::node_cache_status),
        )
        .route(
            "/authentication/node_cache/reset",
            post(authn_cache::node_cache_reset),
        )
        // Global authentication settings: validated full replace over
        // the registry-backed store. Management-plane only; the CONNECT
        // path reads a lock-free snapshot and publish/deliver never
        // touch this store.
        .route(
            "/authentication/settings",
            get(authn_settings::get_authn_settings).put(authn_settings::put_authn_settings),
        )
        // Authentication (AuthN) built-in-database users plus bulk
        // import. Management-plane only; CONNECT reads the live user map
        // under short read locks and publish/deliver never touch it.
        .route(
            "/authentication/:id/users",
            get(auth::list_authn_users).post(auth::create_authn_user),
        )
        .route(
            "/authentication/:id/users/:user_id",
            get(auth::get_authn_user)
                .put(auth::update_authn_user)
                .delete(auth::delete_authn_user),
        )
        .route(
            "/authentication/:id/import_users",
            post(auth::import_authn_users),
        )
        // Authorization (AuthZ / ACL) decision-cache global clear
        // (W2-13, C04-01): evicts every per-session decision entry on
        // this node so subsequent decisions are recomputed.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/authorization/cache",
            delete(authz_cache::clear_authz_cache),
        )
        // Authorization (AuthZ / ACL) node-cache config read/write plus
        // node reset (W2-14, C04-02..C04-04): validated policy replace
        // persisted via the config registry, and an eviction that clears
        // every per-session decision entry on this node so subsequent
        // decisions are recomputed. Management-plane only; publish and
        // deliver never touch it.
        .route(
            "/authorization/node_cache",
            get(authz_node_cache::get_authz_node_cache).put(authz_node_cache::put_authz_node_cache),
        )
        .route(
            "/authorization/node_cache/reset",
            post(authz_node_cache::reset_authz_node_cache),
        )
        // Authorization (AuthZ / ACL) node-cache status (W2-15, C04-05):
        // live health over the real policy plus the per-session decision
        // state: enabled flag, entry count, hit/miss/insert counters and
        // bounds. Management-plane only; publish and deliver never touch
        // it.
        .route(
            "/authorization/node_cache/status",
            get(authz_node_status::get_authz_node_cache_status),
        )
        // Authorization (AuthZ / ACL) global settings (W2-16, C04-06/07):
        // validated replace with documented defaults filled, persisted
        // via the config registry. Management-plane only; publish and
        // deliver never touch it.
        .route(
            "/authorization/settings",
            get(authz_settings::get_authz_settings).put(authz_settings::put_authz_settings),
        )
        // Authorization (AuthZ / ACL) source registry plus the built-in
        // catch-all clear (W2-17, C04-08..C04-10): ordered list with the
        // standard page envelope, single-source register with
        // validate-all-before-apply, and a clear that empties the
        // built-in rule table. Management-plane only; publish and deliver
        // never touch the registry (the built-in rule consult stays on
        // the publish/subscribe path).
        .route(
            "/authorization/sources",
            get(authz_sources::list_authz_sources).post(authz_sources::create_authz_source),
        )
        .route(
            "/authorization/sources/built_in_database/rules",
            get(authz_sources::list_built_in_rules)
                .post(authz_sources::create_built_in_rule)
                .delete(authz_sources::clear_built_in_rules),
        )
        // Authorization (AuthZ / ACL) built-in catch-all table (W2-18,
        // C04-11..C04-13): unpaged read, whole-list replace with
        // validate-all-before-apply, and a clear that empties the same
        // live rule table. Management-plane only; publish and deliver
        // never touch the registry (the built-in rule consult stays on
        // the publish/subscribe path).
        .route(
            "/authorization/sources/built_in_database/rules/all",
            get(authz_sources::list_catch_all_rules)
                .post(authz_sources::replace_catch_all_rules)
                .delete(authz_sources::clear_catch_all_rules),
        )
        // Authorization (AuthZ / ACL) built-in per-client rule table
        // (W2-19, C04-14..C04-16; W2-20, C04-17/C04-18): paged list,
        // single-rule add with duplicate rejection, per-id detail read,
        // whole-set replace with validate-all-before-apply, and a per-id
        // delete that removes every rule for one client id.
        // Management-plane only; publish and deliver never touch the
        // registry (the built-in rule consult stays on the
        // publish/subscribe path).
        .route(
            "/authorization/sources/built_in_database/rules/clients",
            get(authz_clients::list_client_rules).post(authz_clients::create_client_rule),
        )
        .route(
            "/authorization/sources/built_in_database/rules/clients/:clientid",
            get(authz_clients::get_client_rules)
                .put(authz_clients::replace_client_rules)
                .delete(authz_clients::delete_client_rules),
        )
        // Authorization (AuthZ / ACL) built-in per-user rule list (W2-20,
        // C04-19) plus per-user add, per-username read/replace/delete
        // (W2-21, C04-20..C04-22; W2-22, C04-23): paged `data` plus `meta`
        // over the shared built-in table, single-rule add with duplicate
        // rejection, per-username detail read, whole-set replace with
        // validate-all-before-apply, and whole-set delete.
        // Management-plane only; publish and deliver never touch the
        // registry (the built-in rule consult stays on the
        // publish/subscribe path).
        .route(
            "/authorization/sources/built_in_database/rules/users",
            get(authz_clients::list_user_rules).post(authz_clients::create_user_rule),
        )
        .route(
            "/authorization/sources/built_in_database/rules/users/:username",
            get(authz_clients::get_user_rules)
                .put(authz_clients::replace_user_rules)
                .delete(authz_clients::delete_user_rules),
        )
        // Authorization (AuthZ / ACL) source evaluation order plus
        // per-type delete (W2-22, C04-24/C04-25): whole-order replace
        // with validate-all-before-apply, and a delete that removes one
        // source while refusing to delete the last one. Management-plane
        // only; publish and deliver never touch the registry (the
        // built-in rule consult stays on the publish/subscribe path).
        .route(
            "/authorization/sources/order",
            put(authz_sources::replace_authz_order),
        )
        // Authorization (AuthZ / ACL) single-source read, update and
        // metrics reset (W2-23, C04-26..C04-28): per-type config plus
        // live health, validated whole-entry replace, and a reset that
        // zeroes the per-source atomic counters. Management-plane only;
        // publish and deliver never touch the registry (the built-in
        // rule consult stays on the publish/subscribe path).
        .route(
            "/authorization/sources/:type",
            get(authz_sources::get_authz_source)
                .put(authz_sources::put_authz_source)
                .delete(authz_sources::delete_authz_source),
        )
        .route(
            "/authorization/sources/:type/metrics/reset",
            post(authz_sources::reset_authz_source_metrics),
        )
        // Authorization (AuthZ / ACL) single-source move plus status
        // (W2-24, C04-29/C04-30): bounds-checked reposition over the real
        // registry reporting the new order, and a live liveness plus
        // counter read over the real entry. Management-plane only;
        // publish and deliver never touch the registry (the built-in
        // rule consult stays on the publish/subscribe path).
        .route(
            "/authorization/sources/:type/move",
            post(authz_sources::move_authz_source),
        )
        .route(
            "/authorization/sources/:type/status",
            get(authz_sources::get_authz_source_status),
        )
        // Authorization (AuthZ / ACL) generic per-type rule helpers.
        .route(
            "/authorization/sources/:type/rules",
            get(auth::list_authz_rules).post(auth::create_authz_rule),
        )
        .route(
            "/authorization/sources/:type/rules/:index",
            delete(auth::delete_authz_rule),
        )
        // API keys (W2-25, C05-01..C05-03; W2-26, C05-04/C05-05):
        // paged metadata list, mint-with-single-secret-return, revoke
        // with 404s, plus single-key metadata read and scopes/description
        // replace with 404s, all on real state. Management-plane only; key
        // checks use constant-time comparison off the hot path and
        // publish/deliver never touch this store. Bearer/Basic enforcement
        // itself is owned elsewhere; this task only manages the key records.
        .route(
            "/api_key",
            get(api_keys::list_api_keys).post(api_keys::create_api_key),
        )
        .route(
            "/api_key/:name",
            get(api_keys::get_api_key)
                .put(api_keys::update_api_key)
                .delete(api_keys::delete_api_key),
        )
        // Fixed API-key scope catalogue (W2-27, C05-06): static list
        // served from the registry shared with key create/update
        // validation. Management-plane only; publish and deliver never
        // touch it. The route is not paged and takes no parameters;
        // unknown query parameters are ignored.
        .route("/api_key_scopes", get(api_keys::list_api_key_scopes))
        // The licence endpoints of the reference API. The broker is open
        // source: the document is fixed and no limit is applied.
        .route(
            "/license",
            get(license::get_license).post(license::update_license),
        )
        .route(
            "/license/setting",
            get(license::get_license_setting).put(license::put_license_setting),
        )
        .route(
            "/license/session_hwm_history",
            get(license::get_session_hwm_history),
        )
        // Management audit log (W2-28, C05-07): paged query over the
        // bounded management ring. Management-plane only; writes are
        // fire-and-forget from the auth middleware and publish/deliver
        // never touch the ring.
        .route("/audit", get(audit::list_audit))
        // Configured single-sign-on backends (W2-32, C05-11): bare
        // ordered list over the real registry. An empty set reads as an
        // empty collection, never an error. The route is not paged and
        // takes no parameters; unknown query parameters are ignored.
        // Management-plane only; publish and deliver never touch it.
        .route("/sso", get(sso::list_sso))
        // Single single-sign-on backend read, write and delete (W2-39,
        // C05-21..C05-23): per-backend detail read with secret redaction,
        // create-or-replace write with validate-all-before-apply, and a
        // removal that reports success, all on the real registry with
        // persistence across a restart. None of these routes is paged.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/sso/:backend",
            get(sso::get_sso_backend)
                .put(sso::put_sso_backend)
                .delete(sso::delete_sso_backend),
        )
        // Dashboard multi-factor enrolment and verification (W2-34,
        // C05-13..C05-15): TOTP setup, enrolment-state read and
        // single-use code verification on the real admin-user MFA state.
        // All three are POST with JSON bodies, none is paged, and all
        // fail closed with the documented `{code, message}` shape.
        // Management-plane only; publish and deliver never touch the
        // user map or the attempt counters.
        .route("/sso/mfa/setup", post(mfa::setup_route))
        .route("/sso/mfa/setup_info", post(mfa::setup_info_route))
        .route("/sso/mfa/verify", post(mfa::verify_route))
        // Live Monitoring & Rates
        .route("/metrics", get(monitoring::get_metrics))
        // Exporter settings plus text exposition (AP-09): the stored
        // settings round-trip through the config registry, and every
        // exposition route serves the same true counters as the JSON
        // rows above. Management-plane only; publish and deliver never
        // touch the store.
        .route(
            "/prometheus",
            get(prometheus::get_prometheus).put(prometheus::put_prometheus),
        )
        .route("/prometheus/stats", get(prometheus::get_prometheus_stats))
        .route("/prometheus/auth", get(prometheus::get_prometheus_auth))
        .route(
            "/prometheus/data_integration",
            get(prometheus::get_prometheus_data_integration),
        )
        .route(
            "/prometheus/message_transformation",
            get(prometheus::get_prometheus_message_transformation),
        )
        .route(
            "/prometheus/namespaced_stats",
            get(prometheus::get_prometheus_namespaced_stats),
        )
        .route(
            "/prometheus/schema_validation",
            get(prometheus::get_prometheus_schema_validation),
        )
        .route(
            "/prometheus/topic_metrics",
            get(prometheus::get_prometheus_topic_metrics),
        )
        .route(
            "/monitor",
            get(monitor::list_monitor).delete(monitor::clear_monitor),
        )
        .route("/monitor/nodes/:node", get(monitor::get_monitor_node))
        .route("/monitor_current", get(monitoring::monitor_current))
        .route(
            "/monitor_current/nodes/:node",
            get(monitoring::monitor_current_node),
        )
        .route("/stats", get(monitoring::get_stats))
        .route(
            "/mqtt/auto_subscribe",
            get(auto_subscribe::get_auto_subscribe).put(auto_subscribe::put_auto_subscribe),
        )
        .route(
            "/mqtt/retainer",
            get(retainer::get_retainer_config).put(retainer::put_retainer_config),
        )
        .route(
            "/mqtt/retainer/message/:topic",
            get(retainer::get_retainer_message).delete(retainer::delete_retainer_message),
        )
        .route(
            "/mqtt/retainer/messages",
            get(retainer::list_retainer_messages).delete(retainer::clear_retainer_messages),
        )
        // Delayed messages (AP-04): switch plus pending cap, paged
        // list, single detail read and two removals, all on the shared
        // scheduler the kernel fires from. Management-plane only; the
        // publish path pays one atomic load for the switch.
        .route(
            "/mqtt/delayed",
            get(delayed::get_delayed_config).put(delayed::put_delayed_config),
        )
        .route(
            "/mqtt/delayed/messages",
            get(delayed::list_delayed_messages),
        )
        .route(
            "/mqtt/delayed/messages/:node/:msgid",
            get(delayed::get_delayed_message).delete(delayed::delete_delayed_message),
        )
        .route(
            "/mqtt/delayed/messages/:topic",
            delete(delayed::delete_delayed_by_topic),
        )
        // Topic rewrite (AP-04): ordered list plus validated full
        // replacement on the shared router the kernel rewrites with.
        // Management-plane only; the publish path takes the same short
        // rule lock it already took.
        .route(
            "/mqtt/topic_rewrite",
            get(rewrite::list_rewrites).put(rewrite::put_rewrites),
        )
        // Topic-metric collections (AP-04): honest empty states. The
        // broker keeps no per-filter counters: the list reads empty,
        // single reads miss, creates and resets fail, and the bulk
        // delete reports success. Management-plane only; nothing runs
        // on the publish path.
        .route(
            "/mqtt/topic_metrics2",
            get(topic_metrics::list_collections)
                .post(topic_metrics::create_collection)
                .delete(topic_metrics::delete_all_collections),
        )
        .route(
            "/mqtt/topic_metrics2/:name",
            get(topic_metrics::get_collection).delete(topic_metrics::delete_collection),
        )
        .route(
            "/mqtt/topic_metrics2/:name/reset",
            put(topic_metrics::reset_collection),
        )
        .route(
            "/slow_subscriptions",
            get(slow_subscriptions::list_slow_subscriptions)
                .delete(slow_subscriptions::clear_slow_subscriptions),
        )
        .route(
            "/slow_subscriptions/settings",
            get(slow_subscriptions::get_slow_subs_settings)
                .put(slow_subscriptions::put_slow_subs_settings),
        )
        .route(
            "/tracing",
            get(tracing::get_tracing).put(tracing::put_tracing),
        )
        .route(
            "/trace",
            get(trace::list_traces)
                .post(trace::create_trace)
                .delete(trace::clear_traces),
        )
        .route("/trace/:name", delete(trace::delete_trace))
        .route("/trace/:name/stop", put(trace::stop_trace))
        .route("/trace/:name/download", get(trace::download_trace))
        .route("/trace/:name/log", get(trace::get_trace_log))
        .route("/trace/:name/log_detail", get(trace::get_trace_log_detail))
        // Cluster & Nodes
        // Cluster view (AP-03): single-node name, nodes and self plus
        // the stored operator description, with a validated description
        // replace and a core-only topology. Management-plane only;
        // publish and deliver never touch this root.
        .route(
            "/cluster",
            get(cluster::get_cluster).put(cluster::put_cluster),
        )
        .route("/cluster/topology", get(cluster::get_topology))
        // Configuration groups (AP-03): generic key read plus merge or
        // replace, the live default-zone knobs, one honest read per
        // remaining documented root, and a reset to defaults.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/configs",
            get(configs::get_configs).put(configs::put_configs),
        )
        .route(
            "/configs/global_zone",
            get(configs::get_global_zone).put(configs::put_global_zone),
        )
        .route(
            "/configs/broker",
            get(configs::get_broker).put(configs::put_broker),
        )
        .route("/configs/log", get(configs::get_log).put(configs::put_log))
        .route(
            "/configs/sysmon",
            get(configs::get_sysmon).put(configs::put_sysmon),
        )
        .route(
            "/configs/sys_topics",
            get(configs::get_sys_topics).put(configs::put_sys_topics),
        )
        .route(
            "/configs/alarm",
            get(configs::get_alarm).put(configs::put_alarm),
        )
        .route(
            "/configs/dashboard",
            get(configs::get_dashboard).put(configs::put_dashboard),
        )
        .route("/configs_reset/:rootname", post(configs::post_reset))
        .route("/nodes", get(nodes::list_nodes))
        .route("/nodes/:node", get(nodes::get_node))
        .route("/nodes/:node/metrics", get(monitoring::get_metrics_node))
        .route("/nodes/:node/stats", get(monitoring::get_stats_node))
        .route("/nodes/:node/clients/:clientid", get(clients::get_client))
        // Listeners (AP-02): live listener inventory on the kernel
        // listener store seeded from the boot configuration. Reads
        // report stored configuration plus the running flag the kernel
        // controls; lifecycle calls open and close kernel sockets.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/listeners",
            get(listeners::list_listeners).post(listeners::create_listener),
        )
        .route(
            "/listeners/:id",
            get(listeners::get_listener)
                .put(listeners::update_listener)
                .post(listeners::create_listener_with_id)
                .delete(listeners::delete_listener),
        )
        .route("/listeners/:id/start", post(listeners::start_listener))
        .route("/listeners/:id/stop", post(listeners::stop_listener))
        .route("/listeners/:id/restart", post(listeners::restart_listener))
        .route("/listeners_status", get(listeners::listeners_status))
        // Clients & Subscriptions
        .route("/clients", get(clients::list_clients))
        .route("/clients_v2", get(clients::list_clients))
        .route("/clients/kickout/bulk", post(clients::batch_kick_clients))
        .route(
            "/clients/:clientid",
            get(clients::get_client).delete(clients::kick_client),
        )
        .route(
            "/clients/:clientid/subscriptions",
            get(clients::get_client_subscriptions),
        )
        .route(
            "/clients/:clientid/subscribe",
            post(clients::client_subscribe),
        )
        .route(
            "/clients/:clientid/subscribe/bulk",
            post(clients::bulk_subscribe),
        )
        .route(
            "/clients/:clientid/unsubscribe",
            post(clients::client_unsubscribe),
        )
        .route(
            "/clients/:clientid/unsubscribe/bulk",
            post(clients::bulk_unsubscribe),
        )
        .route(
            "/clients/:clientid/mqueue_messages",
            get(clients::get_client_mqueue),
        )
        .route(
            "/clients/:clientid/authorization/cache",
            get(clients::get_client_authz_cache).delete(clients::clear_client_authz_cache),
        )
        .route(
            "/clients/:clientid/keepalive",
            put(clients::put_client_keepalive),
        )
        .route(
            "/clients/:clientid/inflight_messages",
            get(clients::get_client_inflight),
        )
        .route(
            "/banned",
            get(banned::list_banned)
                .post(banned::create_banned)
                .delete(banned::clear_banned),
        )
        .route("/banned/:as/:who", delete(banned::delete_banned_one))
        // Managed certificate bundles (AP-15): list, read, upload and
        // delete for the global namespace plus one managed namespace.
        // Files persist under the data directory. Management-plane only;
        // publish and deliver never touch them.
        .route("/certs/global/list", get(certs::list_global_bundles))
        .route(
            "/certs/global/name/:name",
            get(certs::get_global_bundle)
                .post(certs::upload_global_bundle)
                .delete(certs::delete_global_bundle),
        )
        .route("/certs/ns/:namespace/list", get(certs::list_ns_bundles))
        .route(
            "/certs/ns/:namespace/name/:name",
            get(certs::get_ns_bundle)
                .post(certs::upload_ns_bundle)
                .delete(certs::delete_ns_bundle),
        )
        .route(
            "/alarms",
            get(alarms::list_alarms).delete(alarms::clear_alarms),
        )
        .route("/alarms/force_deactivate", post(alarms::force_deactivate))
        .route("/subscriptions", get(clients::list_subscriptions))
        .route("/topics", get(clients::list_topics))
        .route("/topics/:topic", get(clients::get_topic))
        .route("/sessions_count", get(clients::get_sessions_count))
        .route("/publish", post(clients::publish_message))
        .route("/publish/bulk", post(clients::publish_bulk))
        // Rules, Flow Designer & Connectors
        // Rule-engine settings (W3-10, C06-14/C06-15): live read plus
        // validated full replace on the real engine-config store, persisted
        // across a restart with hot-apply to the fire-path snapshot. Not
        // paged and not per-id, so no 404 path; unknown query keys on
        // writes fail closed. Management-plane only; publish and deliver
        // never touch the store.
        .route(
            "/rule_engine",
            get(rule_engine::get_rule_engine).put(rule_engine::put_rule_engine),
        )
        // Rule-event catalogue (W3-11, C06-16): bare list over the real
        // producer registry in `broker-rules`. Not paged; unknown query
        // keys are ignored. Management-plane only; publish and deliver
        // never touch the registry.
        .route("/rule_events", get(rule_events::list_rule_events))
        // Stateless rule test (W3-12, C06-17): offline evaluation of a
        // rule body against a sample message reusing the real rule
        // compiler, side-effect-free by construction (no store writes, no
        // publish, no action firing). Management-plane only; publish and
        // deliver never touch it.
        .route("/rule_test", post(rule_test::test_rule_offline))
        .route("/rules", get(rules::list_rules).post(rules::create_rule))
        .route(
            "/rules/:id",
            get(rules::get_rule)
                .put(rules::update_rule)
                .delete(rules::delete_rule),
        )
        .route("/rules/:id/metrics", get(rules::get_rule_metrics))
        .route("/rules/:id/metrics/reset", put(rules::reset_rule_metrics))
        // Per-rule offline test (W3-16, C06-25): offline evaluation of
        // the stored rule against a sample message reusing the real rule
        // compiler, side-effect-free by construction (no store writes, no
        // publish, no action firing). Management-plane only; publish and
        // deliver never touch it.
        .route("/rules/:id/test", post(rule_test::test_stored_rule))
        .route(
            "/connectors",
            get(rules::list_connectors).post(rules::create_connector),
        )
        .route(
            "/connectors/:id",
            get(rules::get_connector)
                .put(rules::update_connector)
                .delete(rules::delete_connector),
        )
        .route("/connectors_probe", post(rules::probe_connector))
        // Connector enable flag (W3-19, C07-06): flips one connector's
        // enable bit with the flag honoured by rule dispatch via the
        // engine's lock-free snapshot. Management-plane only; publish and
        // deliver never touch the store.
        .route(
            "/connectors/:id/enable/:enable",
            put(rules::set_connector_enabled),
        )
        // Connector lifecycle operation (W3-20, C07-07): `start`/`stop`
        // dispatch on the W3-17 connector store wired to the engine's
        // lock-free dispatch snapshot. Only supported names are served;
        // unsupported names are rejected, never claimed.
        // Management-plane only; publish and deliver never touch the
        // store.
        .route(
            "/connectors/:id/:operation",
            post(rules::run_connector_operation),
        )
        // Node-scoped connector lifecycle operation (W3-22, C07-09):
        // single-node execution of the W3-20 operation path guarded by
        // the shared node-scope helper (unknown nodes are NOT_FOUND).
        // Management-plane only; publish and deliver never touch the
        // store.
        .route(
            "/nodes/:node/connectors/:id/:operation",
            post(rules::run_node_connector_operation),
        )
        // Actions (W3-02, C06-02..C06-04; W3-03, C06-05/C06-06):
        // paged list, validated create and delete with rule-reference
        // refusal, plus single read and validated replace on the real
        // action directory. Management-plane only; rule evaluation reads
        // a snapshot, never this store directly, and publish and deliver
        // never touch it.
        .route(
            "/actions",
            get(actions::list_actions).post(actions::create_action),
        )
        .route(
            "/actions/:id",
            get(actions::get_action)
                .put(actions::update_action)
                .delete(actions::delete_action),
        )
        // Action enable flag (W3-04, C06-07): flips one action's enable
        // bit with the flag honoured by rule dispatch via the engine's
        // lock-free snapshot. Management-plane only; publish and deliver
        // never touch the store.
        .route(
            "/actions/:id/enable/:enable",
            put(actions::set_action_enabled),
        )
        // Per-action metrics (W3-05, C06-08/C06-09): live lock-free
        // counters attributed on the rule-fire path, plus a reset that
        // zeroes them. Unknown ids are 404 with the documented shape;
        // neither route is paged and unknown query keys are ignored.
        // Management-plane only; increments stay lock-free atomics and
        // publish/deliver never touch the action store.
        .route("/actions/:id/metrics", get(actions::get_action_metrics))
        .route(
            "/actions/:id/metrics/reset",
            put(actions::reset_action_metrics),
        )
        // Action lifecycle operation (W3-06, C06-10): `start`/`stop`
        // dispatch on the W3-02 action store wired to the connector
        // lifecycle underneath (the engine's lock-free dispatch snapshot
        // gating `ForwardConnector` forwards). Only supported names are
        // served; unsupported names are rejected, never claimed.
        // Management-plane only; publish and deliver never touch the
        // store.
        .route(
            "/actions/:id/:operation",
            post(actions::run_action_operation),
        )
        // Node-scoped action lifecycle operation (W3-09, C06-13):
        // single-node execution of the W3-06 operation path guarded by
        // the shared node-scope helper (unknown nodes are NOT_FOUND).
        // Management-plane only; publish and deliver never touch the
        // store.
        .route(
            "/nodes/:node/actions/:id/:operation",
            post(actions::run_node_action_operation),
        )
        // Aggregated action summary (W3-08, C06-12): counts computed on
        // demand from the live W3-02 store snapshot (zeroes when empty,
        // never an error). Not paged and not per-id, so no 404 path;
        // unknown query parameters are ignored. Management-plane only;
        // publish and deliver never touch the store.
        .route("/actions_summary", get(actions::get_actions_summary))
        // Actions & Sources
        .route("/action_types", get(rules::get_action_types))
        .route("/actions_probe", post(rules::probe_action))
        // Stateless source probe (W3-30, C07-21): validates the config
        // against the named kind and dials where the kind supports it.
        // Management-plane only; publish and deliver never touch it.
        .route("/sources_probe", post(rules::probe_source))
        // Source-type catalogue (W3-25, C07-11): bare list over the real
        // source-type registry in `broker-connectors`. Not paged; unknown
        // query keys are ignored. Management-plane only; publish and
        // deliver never touch the registry.
        .route("/source_types", get(sources::list_source_types))
        // Source instances plus the aggregated summary (W3-26 store,
        // W3-27 C07-15/C07-16, W3-28 C07-17, W3-31 C07-22): paged list with the standard page envelope,
        // validated register with duplicate rejection, a single read with
        // a 404 for unknown ids, a validated replace with a 404 that
        // creates nothing, a delete with a
        // 404 for unknown ids, and counts computed on demand from the
        // live snapshot (zeroes when empty, never an error).
        // Management-plane only; publish and deliver never touch the
        // store.
        .route(
            "/sources",
            get(sources::list_sources).post(sources::create_source),
        )
        .route(
            "/sources/:id",
            get(sources::get_source)
                .put(sources::update_source)
                .delete(sources::delete_source),
        )
        // Source lifecycle operation (W3-23, C07-20): `start`/`stop`
        // dispatch on the source runtime core (enable flip in the live
        // store). Only supported names are served; unsupported names
        // are rejected, never claimed. Management-plane only; publish
        // and deliver never touch the store.
        .route(
            "/sources/:id/:operation",
            post(sources::run_source_operation),
        )
        // Source enable flag (W3-28, C07-17): flips one source's enable
        // bit on the same W3-23 runtime core (disabled sources stop
        // ingress). Management-plane only; publish and deliver never
        // touch the store.
        .route(
            "/sources/:id/enable/:enable",
            put(sources::set_source_enabled),
        )
        // Per-source metrics (W3-29, C07-18/C07-19): live lock-free
        // counters attributed on the ingress path, plus a reset that
        // zeroes them. Unknown ids are 404 with the documented shape;
        // neither route is paged and unknown query keys are ignored.
        // Management-plane only; increments stay lock-free atomics and
        // publish/deliver never touch the source store.
        .route("/sources/:id/metrics", get(sources::get_source_metrics))
        .route(
            "/sources/:id/metrics/reset",
            put(sources::reset_source_metrics),
        )
        // Node-scoped source lifecycle operation (W3-24, C07-10):
        // single-node execution of the W3-23 operation path guarded by
        // the shared node-scope helper (unknown nodes are NOT_FOUND).
        // Management-plane only; publish and deliver never touch the
        // store.
        .route(
            "/nodes/:node/sources/:id/:operation",
            post(sources::run_node_source_operation),
        )
        .route("/sources_summary", get(sources::get_sources_summary))
        // Schema API for dynamic UI forms
        .route("/schemas", get(schemas::list_schemas))
        .route("/schemas/:name", get(schemas::get_schema))
        // Fixed error-code catalogue (W5-22, C15-01/C15-02): static
        // code-defined list with the standard page envelope, plus a
        // per-code read with a 404 for unknown codes. Management-plane
        // only; publish and deliver never touch it.
        .route("/error_codes", get(error_codes::list_error_codes))
        .route("/error_codes/:code", get(error_codes::get_error_code))
        // Feature registry read (W5-11, C13-15): fixed bounded set
        // filtered by the installed licence on the real licence store.
        // Management-plane only; publish and deliver never touch it.
        .route("/features", get(features::list_features))
        // Data Backup (W5-26, C18-01): export snapshot of config and data.
        // Management-plane only; runs off the message path.
        .route("/data/export", post(data::export_data))
        // Backup file collection (W5-27, C18-02..C18-04) plus file
        // download (W5-28, C18-05): paged list of stored files, upload
        // of one file, download of one file, delete of one file. All
        // four serve the same backup dir as the export route, so files
        // persist across a restart within the same bounds.
        // Management-plane only; runs off the message path.
        .route(
            "/data/files",
            get(data::list_backup_files).post(data::upload_backup_file),
        )
        .route(
            "/data/files/:filename",
            get(data::download_backup_file).delete(data::delete_backup_file),
        )
        // Data import (W5-29, C18-06): restore a named backup file into
        // the live stores with validate-all-before-apply semantics.
        // Management-plane only; publish and deliver never touch it.
        .route("/data/import", post(backup::import_data))
        // Message transformation chain (W3-32, C08-01..C08-03): ordered
        // list, validated append and whole-chain replace on the real
        // chain store with execution hooked into ingress via the
        // engine's lock-free snapshot. List reads the bare chain in
        // execution order (`[]` when empty, never an error, never
        // paged); unknown query keys are ignored. Management-plane
        // only; publish loads the engine snapshot and never takes the
        // store lock.
        .route(
            "/message_transformations",
            get(transformations::list_transformations)
                .post(transformations::create_transformation)
                .put(transformations::replace_transformations),
        )
        // Transformation dry-run (W3-33, C08-04): stateless probe of
        // the supplied chain against one sample message through the
        // real executor. Writes nothing and publishes nothing.
        // Management-plane only; publish loads the engine snapshot
        // and never takes the store lock.
        .route(
            "/message_transformations/dryrun",
            post(transformations::dryrun_transformations),
        )
        // Transformation reorder (W3-34, C08-05): replace the stored
        // chain order with the supplied name order, validated before
        // anything applies. Answers 204; the new order shows on the
        // next list read and drives execution order. Nothing is
        // created or deleted. Management-plane only; publish loads
        // the engine snapshot and never takes the store lock.
        .route(
            "/message_transformations/reorder",
            post(transformations::reorder_transformations),
        )
        // Single transformation read and delete (W3-35, C08-06/C08-07):
        // per-name read with 404 for unknown names, and per-name
        // delete with 204 on success and 404 for unknown names. Neither
        // route is paged; unknown query keys are ignored.
        // Management-plane only; publish loads the engine snapshot
        // and never takes the store lock.
        .route(
            "/message_transformations/transformation/:name",
            get(transformations::get_transformation).delete(transformations::delete_transformation),
        )
        // Single transformation enable flag (W3-35, C08-08): flips one
        // entry flag with 404 for unknown names and 400 for a malformed
        // flag. Disabled steps skip at execution time through the engine
        // snapshot. Management-plane only; publish loads the snapshot
        // and never takes the store lock.
        .route(
            "/message_transformations/transformation/:name/enable/:enable",
            post(transformations::set_transformation_enabled),
        )
        // Per-transformation metrics (W3-36, C08-09/C08-10): live
        // lock-free counters attributed on the ingress path, plus a
        // reset that zeroes them. Unknown names are 404 with the
        // documented shape; neither route is paged and unknown query
        // keys are ignored. Management-plane only; increments stay
        // lock-free atomics and publish/deliver never touch the chain
        // store.
        .route(
            "/message_transformations/transformation/:name/metrics",
            get(transformations::get_transformation_metrics),
        )
        .route(
            "/message_transformations/transformation/:name/metrics/reset",
            post(transformations::reset_transformation_metrics),
        )
        // Schema registry (W3-37, C08-11..C08-13): paged list, checked
        // create and drop on a real store. List uses the standard page
        // envelope. Create parses the schema. Delete misses are 404.
        // Management plane only. Publish and deliver take no lock here.
        .route(
            "/schema_registry",
            get(schema_registry::list_schemas_registry)
                .post(schema_registry::create_schema_registry),
        )
        .route(
            "/schema_registry/:name",
            get(schema_registry::get_schema_registry)
                .delete(schema_registry::delete_schema_registry),
        )
        // Honest empty states for functions the broker does not have
        // (AP-01): AI providers and completion profiles, message queues,
        // message streams, telemetry, load rebalance and node eviction.
        // Reads answer the documented shape with an empty or disabled
        // state. Writes answer 400 `BAD_REQUEST`. Nothing is stored.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/ai/providers",
            get(empty_states::list_ai_providers).post(empty_states::create_ai_provider),
        )
        .route(
            "/ai/providers/:name",
            get(empty_states::get_ai_provider)
                .put(empty_states::update_ai_provider)
                .delete(empty_states::delete_ai_provider),
        )
        .route(
            "/ai/providers/:name/models",
            get(empty_states::list_ai_provider_models),
        )
        .route("/ai/models", post(empty_states::list_ai_models))
        .route(
            "/ai/completion_profiles",
            get(empty_states::list_ai_completion_profiles)
                .post(empty_states::create_ai_completion_profile),
        )
        .route(
            "/ai/completion_profiles/:name",
            get(empty_states::get_ai_completion_profile)
                .put(empty_states::update_ai_completion_profile)
                .delete(empty_states::delete_ai_completion_profile),
        )
        .route(
            "/queues",
            get(empty_states::list_queues).post(empty_states::create_queue),
        )
        .route(
            "/queues/config",
            get(empty_states::get_queue_config).put(empty_states::put_queue_config),
        )
        .route(
            "/queue/:name",
            get(empty_states::get_queue)
                .put(empty_states::update_queue)
                .delete(empty_states::delete_queue),
        )
        .route(
            "/streams",
            get(empty_states::list_streams).post(empty_states::create_stream),
        )
        .route(
            "/streams/config",
            get(empty_states::get_stream_config).put(empty_states::put_stream_config),
        )
        .route(
            "/stream/:name",
            get(empty_states::get_stream)
                .put(empty_states::update_stream)
                .delete(empty_states::delete_stream),
        )
        .route(
            "/telemetry/status",
            get(empty_states::get_telemetry_status).put(empty_states::put_telemetry_status),
        )
        .route("/telemetry/data", get(empty_states::get_telemetry_data))
        .route(
            "/load_rebalance/status",
            get(empty_states::get_rebalance_status),
        )
        .route(
            "/load_rebalance/global_status",
            get(empty_states::get_rebalance_global_status),
        )
        .route(
            "/load_rebalance/:node/start",
            post(empty_states::start_rebalance),
        )
        .route(
            "/load_rebalance/:node/stop",
            post(empty_states::stop_rebalance),
        )
        .route(
            "/load_rebalance/:node/evacuation/start",
            post(empty_states::start_evacuation),
        )
        .route(
            "/load_rebalance/:node/evacuation/stop",
            post(empty_states::stop_evacuation),
        )
        .route(
            "/node_eviction/status",
            get(empty_states::get_node_eviction_status),
        )
        // Tenant namespaces (TM-02, C17): the 16 `/mt/...` operations.
        // Reads serve live broker state per tenant; writes touch only the
        // managed registry plus the tenant credential rows. Auth and the
        // viewer read-only rule come from the shared middleware.
        // Management-plane only; publish and deliver never touch it.
        .route("/mt/bulk_delete_ns", delete(mt::bulk_delete_ns))
        .route(
            "/mt/bulk_export_ns_configs",
            post(mt::bulk_export_ns_configs),
        )
        .route("/mt/bulk_import_configs", post(mt::bulk_import_configs))
        .route(
            "/mt/bulk_import_ns_configs",
            post(mt::bulk_import_ns_configs),
        )
        .route("/mt/managed_ns_list", get(mt::list_managed_ns))
        .route(
            "/mt/managed_ns_list_details",
            get(mt::list_managed_ns_details),
        )
        .route("/mt/ns_list", get(mt::list_ns))
        .route("/mt/ns_list_details", get(mt::list_ns_details))
        .route("/mt/ns/:ns", post(mt::create_ns).delete(mt::delete_ns))
        .route("/mt/ns/:ns/client_count", get(mt::ns_client_count))
        .route("/mt/ns/:ns/client_list", get(mt::ns_client_list))
        .route(
            "/mt/ns/:ns/config",
            get(mt::get_ns_config).put(mt::put_ns_config),
        )
        .route("/mt/ns/:ns/kick_all_clients", post(mt::kick_ns_clients))
        .route("/mt/ns/:ns/metrics", get(mt::ns_metrics))
}
