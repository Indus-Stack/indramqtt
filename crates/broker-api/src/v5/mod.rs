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
pub mod banned;
pub mod clients;
pub mod error_codes;
pub mod features;
pub mod gateways;
pub mod license;
pub mod mfa;
pub mod monitor;
pub mod monitoring;
pub mod nodes;
pub mod retainer;
pub mod rules;
pub mod schemas;
pub mod slow_subscriptions;
pub mod sources;
pub mod sso;
pub mod status;
pub mod trace;
pub mod tracing;

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
        .route("/sso/mfa/setup", post(mfa::setup))
        .route("/sso/mfa/setup_info", post(mfa::setup_info))
        .route("/sso/mfa/verify", post(mfa::verify))
        // Live Monitoring & Rates
        .route("/metrics", get(monitoring::get_metrics))
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
        .route("/status", get(status::get_status))
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
        .route("/nodes", get(nodes::list_nodes))
        .route("/nodes/:node", get(nodes::get_node))
        .route("/nodes/:node/metrics", get(monitoring::get_metrics_node))
        .route("/nodes/:node/stats", get(monitoring::get_stats_node))
        .route("/nodes/:node/clients/:clientid", get(clients::get_client))
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
        .route("/rules", get(rules::list_rules).post(rules::create_rule))
        .route(
            "/rules/:id",
            get(rules::get_rule)
                .put(rules::update_rule)
                .delete(rules::delete_rule),
        )
        .route("/rules/:id/metrics", get(rules::get_rule_metrics))
        .route("/rules/:id/metrics/reset", put(rules::reset_rule_metrics))
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
        // Actions & Sources
        .route("/action_types", get(rules::get_action_types))
        .route("/actions_probe", post(rules::probe_action))
        // Stateless source probe (W3-30, C07-21): validates the config
        // against the named kind and dials where the kind supports it.
        // Management-plane only; publish and deliver never touch it.
        .route("/sources_probe", post(rules::probe_source))
        // Source instances plus the aggregated summary (W3-26 store,
        // W3-31 C07-22): validated register with duplicate rejection, a
        // delete with a 404 for unknown ids, and counts computed on
        // demand from the live snapshot (zeroes when empty, never an
        // error). Management-plane only; publish and deliver never
        // touch the store.
        .route("/sources", post(sources::create_source))
        .route("/sources/:id", delete(sources::delete_source))
        .route("/sources_summary", get(sources::get_sources_summary))
        // Schema API for dynamic UI forms
        .route("/schemas", get(schemas::list_schemas))
        .route("/schemas/:name", get(schemas::get_schema))
        // License state and install (W5-10, C13-16/C13-17): live read
        // plus validated install on the real licence store.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/license",
            get(license::get_license).post(license::install_license),
        )
        // Session HWM history (W5-12, C13-18): paged newest-first read
        // over the bounded recorder on real session counts.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/license/session_hwm_history",
            get(license::get_session_hwm_history),
        )
        // License settings (W5-13, C13-19/C13-20): validated read plus
        // validated update over the persisted single-record store.
        // Management-plane only; publish and deliver never touch it.
        .route(
            "/license/setting",
            get(license::get_license_setting).put(license::put_license_setting),
        )
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
}
