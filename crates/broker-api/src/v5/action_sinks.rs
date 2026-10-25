//! Action-owned runtime sinks (MV-01, ticket SB-03).
//!
//! A v5 action owns its data path: the sink is built once per action
//! from the referenced connector document (connection settings only)
//! plus the action's own parameters, and registers in the connector
//! manager under the reserved action dispatch key
//! ([`broker_config::v5_action_sink_key`]). One action is one key, so
//! two actions on one connector with different statements route
//! independently and stop sharing one sink.
//!
//! Bound (stated here and by construction): `pgsql` and the
//! `webhook`/`http` pair build their sinks here (the two kinds the
//! family ticket fully corrects). Every other action kind owns no
//! runtime yet: it stores opaquely, registers nothing, reads
//! `disconnected` with a reason, and a fire counts one `failed`
//! delivery against its per-action metrics. The kind families accept
//! the same surface once they plot their data paths (KC-00).
//!
//! No probe, no dial: a build never opens a connection. The `pgsql`
//! transport is lazy (the first send dials), the `webhook` client is
//! constructed per rule body. Persistence failures keep the old
//! generation on every write path that uses this module.

use std::sync::Arc;

use super::actions::{ActionRecord, ActionStore, ActionWiring};
use super::rules::ConnectorStore;

/// True when the action kind builds an owned sink here. The
/// documented `http` action kind and the historic `webhook` base kind
/// share one builder; `pgsql` covers `postgres`-filed rows.
pub(crate) fn action_owned_kind(kind: &str) -> bool {
    matches!(kind, "pgsql" | "webhook" | "http")
}

/// Every connector reference an action record carries: the documented
/// `connector` field plus the legacy flat extras
/// (`connector`/`connector_id` in `params`).
fn action_connector_references(record: &ActionRecord) -> Vec<String> {
    let mut references = Vec::new();
    for raw in [
        record.connector.as_str(),
        record
            .params
            .get("connector")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
        record
            .params
            .get("connector_id")
            .and_then(|v| v.as_str())
            .unwrap_or(""),
    ] {
        let trimmed = raw.trim();
        if !trimmed.is_empty() && !references.contains(&trimmed.to_string()) {
            references.push(trimmed.to_string());
        }
    }
    references
}

/// True when the record targets the connector stored under `bare`:
/// every reference field resolves through the same matching the
/// connector read paths use (bare id or `{type}:{name}` key), so an
/// action can never hide a live reference in one of the legacy fields.
pub(crate) fn action_targets_connector(
    connectors: &ConnectorStore,
    record: &ActionRecord,
    bare: &str,
) -> bool {
    action_connector_references(record).iter().any(|reference| {
        super::rules::resolve_action_connector(connectors, reference)
            .map(|(resolved, _)| resolved == bare)
            .unwrap_or(false)
    })
}

/// One staged action-sink build: the sink (when this kind owns one
/// and the build succeeded) plus the wiring state the caller reports.
/// `Live` wiring always comes with a built `sink`.
pub(crate) struct ActionSinkStage {
    /// The built sink (`None` when the kind owns no runtime or its
    /// target connector is missing).
    pub sink: Option<Arc<dyn broker_connectors::Sink>>,
    /// The wiring state to report.
    pub wiring: ActionWiring,
}

/// Resolve the connector an action references and stage its owned
/// sink (built, never registered).
///
/// - A kind with no owned runtime stages nothing with
///   [`ActionWiring::None`].
/// - A dangling connector reference is not a build failure: the
///   accepted conformance contract keeps such an action storable
///   (KC-00, `POST /actions` case with an unknown connector), so it
///   stages nothing and reports [`ActionWiring::Failed`] with the
///   reason. Nothing is dialled, no value invented.
/// - Everything else (missing action field, missing connector field,
///   invalid parameter) is a build failure: the caller REFUSES with
///   the returned message, which names the action and the field.
pub(crate) fn stage_action_sink(
    connectors: &ConnectorStore,
    record: &ActionRecord,
) -> Result<ActionSinkStage, String> {
    if !action_owned_kind(&record.action_type) {
        return Ok(ActionSinkStage {
            sink: None,
            wiring: ActionWiring::None,
        });
    }
    required_action_parameters(record)?;
    let reference = action_connector_references(record)
        .into_iter()
        .next()
        .ok_or_else(|| {
            format!(
                "action {:?} is missing required field: connector",
                record.id
            )
        })?;
    let Some((_, doc)) = super::rules::resolve_action_connector(connectors, &reference) else {
        return Ok(ActionSinkStage {
            sink: None,
            wiring: ActionWiring::Failed(format!(
                "connector {reference:?} of the action not found (field `connector`)"
            )),
        });
    };
    let sink = build_action_sink(record, &doc)?;
    Ok(ActionSinkStage {
        wiring: if sink.is_some() {
            ActionWiring::Live
        } else {
            ActionWiring::None
        },
        sink,
    })
}

/// Build (never register) one action's owned sink from the referenced
/// connector document plus this action's own parameters. `Ok(None)`
/// marks a kind with no action-owned runtime. Every refusal names the
/// action and the offending field, so a refused create or update can
/// carry the reason straight to the operator.
pub(crate) fn build_action_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Option<Arc<dyn broker_connectors::Sink>>, String> {
    match record.action_type.as_str() {
        "pgsql" => build_pgsql_sink(record, connector_doc).map(Some),
        "webhook" | "http" => build_webhook_sink(record, connector_doc).map(Some),
        // Other kinds own no runtime yet: no sink, no registration.
        // Delivery stays refused-by-default (the fire counts one
        // failure) until the kind plots its own data path (KC-00).
        _ => Ok(None),
    }
}

/// True when every connector the record references participates
/// (`enable` absent reads enabled; a dangling reference reads enabled —
/// its delivery cannot resolve either way). Delivery requires the
/// action's own enable flag AND this: a connector disable stops every
/// dependent action, a connector enable restores them (R2).
pub(crate) fn action_connector_enable(connectors: &ConnectorStore, record: &ActionRecord) -> bool {
    action_connector_references(record).iter().all(|reference| {
        super::rules::resolve_action_connector(connectors, reference)
            .is_none_or(|(_, doc)| doc.get("enable").and_then(|v| v.as_bool()).unwrap_or(true))
    })
}

/// First present non-empty string among the alternative keys of one
/// document (connector bodies accept historic aliases).
fn first_string<'a>(
    doc: &'a serde_json::Value,
    keys: &[&str],
) -> Option<std::borrow::Cow<'a, str>> {
    keys.iter().find_map(|key| {
        doc.get(key)
            .and_then(|v| v.as_str().map(str::trim))
            .filter(|s| !s.is_empty())
            .map(std::borrow::Cow::Borrowed)
    })
}

/// Read one JSON parameter of the action record: the documented
/// `parameters` object first, the legacy flat extras (`params`) as
/// fallback. `None` when absent from both (an empty value reads as
/// unconfigured, never as a target).
fn param_value<'a>(record: &'a ActionRecord, key: &str) -> Option<&'a serde_json::Value> {
    record
        .parameters
        .get(key)
        .filter(|value| !value.is_null())
        .or_else(|| record.params.get(key))
        .filter(|value| !value.is_null())
}

/// Read one string parameter of the action record. `None` when absent
/// or blank (an empty value reads as unconfigured, never as a target).
fn param_string(record: &ActionRecord, key: &str) -> Option<String> {
    param_value(record, key)
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// Check the action-owned required parameters before the connector is
/// resolved. Thus a dangling connector does not accept an action that
/// is missing its own required field.
fn required_action_parameters(record: &ActionRecord) -> Result<(), String> {
    if record.action_type == "pgsql" && param_string(record, "sql").is_none() {
        return Err(format!(
            "action {:?} is missing required field: sql (field `parameters.sql`)",
            record.id
        ));
    }
    Ok(())
}

/// Strip a connection target down to `host:port` the way the
/// connector builders do (`scheme://host:port/path` → `host:port`).
fn clean_server(server: &str) -> String {
    let server = server.split("://").last().unwrap_or(server);
    let server = server.split('/').next().unwrap_or(server);
    server.trim().trim_end_matches(':').to_string()
}

/// Build the action's `pgsql` sink: the connection string comes from
/// the connector (server, database, username, password — password
/// optional), the statement and the pool shape from this action.
///
/// Values and defaults:
/// - `connection_url`: `postgres://{user}:{password}@{server}/{db}`
///   from the connector document. No invented fallback: a missing
///   `server`, `database` or `username` fails the build (fail closed,
///   S1-01) and the write that introduced it is refused with the
///   field named.
/// - `sql_template`: this action's `parameters.sql`.
/// - `pool_size`: the connector setting, with the reference schema
///   default `8`; it bounds the action's concurrent batch connections.
/// - `batch_size` and `batch_timeout_ms`: the action's `resource_opts`.
///   The defaults are `1` row and `10` ms because this sink has no
///   background flusher: one row per send flushes immediately, and the
///   `10` ms linger is the only stale-batch bound. A larger default
///   would hold rows with no timer to flush them.
pub(crate) fn build_pgsql_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    let raw_server = first_string(connector_doc, &["server", "host", "servers"])
        .ok_or_else(|| named("server"))?;
    let database = first_string(connector_doc, &["database"]).ok_or_else(|| named("database"))?;
    let username =
        first_string(connector_doc, &["username", "user"]).ok_or_else(|| named("username"))?;
    let password =
        first_string(connector_doc, &["password"]).unwrap_or(std::borrow::Cow::Borrowed(""));
    let server = clean_server(&raw_server);
    if server.is_empty() {
        return Err(named("server"));
    }
    let conn_url = format!(
        "postgres://{}:{}@{}/{}",
        username, password, server, database
    );
    let sql = param_string(record, "sql").ok_or_else(|| {
        format!(
            "action {:?} is missing required field: sql (field `parameters.sql`)",
            record.id
        )
    })?;
    let opts = &record.resource_opts;
    let pool_size = connector_doc
        .get("pool_size")
        .and_then(serde_json::Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(8);
    let batch_size = opts
        .get("batch_size")
        .and_then(serde_json::Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(1);
    let batch_timeout_ms = opts
        .get("batch_timeout_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(10);
    let transport = broker_connectors::postgres::DriverPgTransport::new(&conn_url, pool_size)
        .map_err(|e| {
            format!(
                "action {:?} cannot build its pgsql transport: {e}",
                record.id
            )
        })?;
    let config = broker_connectors::postgres::PostgreSqlSinkConfig {
        connection_url: conn_url,
        sql_template: sql,
        pool_size,
        batch_size,
        batch_timeout_ms,
    };
    let sink = broker_connectors::postgres::PostgreSqlSink::new(config, Arc::new(transport))
        .map_err(|e| format!("action {:?} cannot build its pgsql sink: {e}", record.id))?;
    Ok(Arc::new(sink))
}

/// Build the action's `webhook`/`http` sink: the base URL comes from
/// the connector document, this action's `parameters` adds the path,
/// method and headers.
///
/// Values and defaults:
/// - URL: the connector's `url` (alias `endpoint`), required; a
///   missing or unresolvable (`file:`/`env:`) reference fails closed.
/// - `path`: optional URL path joined to the connector base (a
///   leading slash is added when absent).
/// - `method`: optional, `post` only (absent means `post`); the
///   action delivers with HTTP POST (the sink shape this runtime
///   implements).
/// - `headers`: optional object of string entries added to the POST.
pub(crate) fn build_webhook_sink(
    record: &ActionRecord,
    connector_doc: &serde_json::Value,
) -> Result<Arc<dyn broker_connectors::Sink>, String> {
    let connector = record.connector.trim().to_string();
    let named = |field: &str| {
        format!(
            "connector {connector:?} is missing required field: {field} (action {:?})",
            record.id
        )
    };
    let raw_url = first_string(connector_doc, &["url", "endpoint"]).ok_or_else(|| named("url"))?;
    // Connector URLs may be secret references; resolve at use time so
    // the store keeps the reference. An unresolvable reference fails
    // closed instead of dialling a redacted placeholder.
    let url = broker_connectors::resolve_field_secret(&raw_url);
    if url.trim().is_empty() {
        return Err(format!(
            "connector {connector:?} url reference is not resolvable (field `url`, action {:?})",
            record.id
        ));
    }
    // TODO(parity): does the reference `http` action schema document
    // methods beyond `post` (for example `get`)? The rulebook does not
    // decide; the current choice serves exactly `post` (the delivery
    // shape this runtime implements) and refuses every other value with
    // `BAD_REQUEST` until the checker pins it.
    if let Some(method) = param_string(record, "method") {
        if !method.eq_ignore_ascii_case("post") {
            return Err(format!(
                "action {:?} parameter method {method:?} unsupported: this action delivers with `post`",
                record.id
            ));
        }
    }
    let mut target = url.trim().trim_end_matches('/').to_string();
    if let Some(path) = param_string(record, "path") {
        if !path.starts_with('/') {
            target.push('/');
        }
        target.push_str(&path);
    }
    let mut headers = reqwest::header::HeaderMap::new();
    if let Some(value) = param_value(record, "headers") {
        let Some(map) = value.as_object() else {
            return Err(format!(
                "action {:?} field headers must be an object (field `parameters.headers`)",
                record.id
            ));
        };
        for (key, entry) in map {
            let Some(text) = entry.as_str() else {
                return Err(format!(
                    "action {:?} field headers.{key} must be a string",
                    record.id
                ));
            };
            let name = reqwest::header::HeaderName::from_bytes(key.as_bytes())
                .map_err(|_| format!("action {:?} header name {key:?} is invalid", record.id))?;
            let value = reqwest::header::HeaderValue::from_str(text)
                .map_err(|_| format!("action {:?} header value {text:?} is invalid", record.id))?;
            headers.insert(name, value);
        }
    }
    Ok(Arc::new(broker_connectors::HttpWebhookSink::new(
        target,
        headers,
        reqwest::Client::new(),
    )))
}

/// Bring every stored action's owned runtime sink in step with the
/// current stores: register the freshly built sink under the action
/// dispatch key when the build succeeds, raise the build-failure
/// alarm and report `disconnected` with the reason when it does not,
/// and drop the key when the target connector is gone or the kind
/// owns no runtime. Boot replay, config reload and restore all use
/// this same replay; every rebuild starts from the stored inputs, so
/// nothing keeps an in-memory copy between writes.
///
/// A rebuild failure keeps any previously registered sink delivering
/// (the old generation; the same fail-open-then-report contract the
/// connector replay follows) and reports through the wiring state
/// plus one alarm — never an invented status value.
pub fn rebuild_all_action_sinks(
    engine: &broker_rules::RuleEngine,
    connectors: &ConnectorStore,
    actions: &ActionStore,
    alarms: &crate::v5::alarms::AlarmStore,
) {
    for record in actions.snapshot() {
        let key = broker_config::v5_action_sink_key(&record.id);
        // MV-01/R2: the dispatch flag is the AND of the action's own
        // enable and its connector's (a connector disable stops every
        // dependent action; a connector enable restores them). Mirrored
        // on every replay so a disable survives restart and reload.
        engine.set_action_enabled(
            &key,
            record.enable && action_connector_enable(connectors, &record),
        );
        if !action_owned_kind(&record.action_type) {
            // No runtime for this kind yet (or the kind changed): drop
            // a stale registration so the key stops claiming delivery.
            if engine.connectors().get(&key).is_some() {
                engine.connectors().unregister(&key);
            }
            actions.set_wiring(&record.id, ActionWiring::None);
            alarms.deactivate(&format!("action_build_failed:{}", record.id));
            continue;
        }
        let stage = match stage_action_sink(connectors, &record) {
            Ok(stage) => stage,
            Err(message) => {
                set_failed_wiring(engine, actions, alarms, &record, message);
                continue;
            }
        };
        match stage.wiring {
            ActionWiring::Live => {
                // Live wiring always carries its sink by construction.
                if let Some(sink) = stage.sink {
                    engine.connectors().register(key, sink);
                }
                actions.set_wiring(&record.id, ActionWiring::Live);
                alarms.deactivate(&format!("action_build_failed:{}", record.id));
            }
            ActionWiring::Failed(message) => {
                set_failed_wiring(engine, actions, alarms, &record, message);
            }
            ActionWiring::None => {
                // No runtime for this kind (or the reference dangles
                // with no resolution): drop a stale registration so
                // the key stops claiming delivery.
                if engine.connectors().get(&key).is_some() {
                    engine.connectors().unregister(&key);
                }
                actions.set_wiring(&record.id, ActionWiring::None);
                alarms.deactivate(&format!("action_build_failed:{}", record.id));
            }
        }
    }
}

/// Fail one action's wiring into the stored, alarm-raising, honest
/// `disconnected` state: drop the key, keep the record readable with
/// the reason and raise `action_build_failed:{id}`.
fn set_failed_wiring(
    engine: &broker_rules::RuleEngine,
    actions: &ActionStore,
    alarms: &crate::v5::alarms::AlarmStore,
    record: &ActionRecord,
    message: String,
) {
    let key = broker_config::v5_action_sink_key(&record.id);
    if engine.connectors().get(&key).is_some() {
        engine.connectors().unregister(&key);
    }
    actions.set_wiring(&record.id, ActionWiring::Failed(message.clone()));
    alarms.activate(
        &format!("action_build_failed:{}", record.id),
        &message,
        serde_json::json!({
            "kind": "action",
            "id": record.id,
            "type": record.action_type,
        }),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(id: &str, action_type: &str, connector: &str) -> ActionRecord {
        ActionRecord {
            id: id.to_string(),
            name: id.to_string(),
            action_type: action_type.to_string(),
            enable: true,
            connector: connector.to_string(),
            parameters: serde_json::json!({}),
            resource_opts: serde_json::json!({}),
            params: serde_json::json!({}),
        }
    }

    fn webhook_record(id: &str, connector: &str, parameters: serde_json::Value) -> ActionRecord {
        ActionRecord {
            parameters,
            ..record(id, "http", connector)
        }
    }

    #[tokio::test]
    async fn stage_non_owned_kind_wires_nothing() {
        let connectors = ConnectorStore::new();
        let stage = stage_action_sink(&connectors, &record("act", "mqtt_bridge", "mqtt:up"))
            .expect("staging a kind with no runtime stages nothing");
        assert!(stage.sink.is_none());
        assert_eq!(stage.wiring, ActionWiring::None);
    }

    #[tokio::test]
    async fn stage_missing_connector_field_refuses_the_write() {
        let connectors = ConnectorStore::new();
        let error = stage_action_sink(&connectors, &record("act", "http", ""))
            .err()
            .expect("an owned kind without a connector is refused");
        assert!(
            error.contains("act") && error.contains("connector"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn stage_dangling_connector_stores_disconnected_without_dialling() {
        let connectors = ConnectorStore::new();
        let stage = stage_action_sink(&connectors, &record("act", "http", "webhook:gone-away"))
            .expect("a dangling reference is not a build failure");
        assert!(stage.sink.is_none());
        let ActionWiring::Failed(reason) = stage.wiring else {
            panic!("a dangling reference reports failed wiring");
        };
        assert!(reason.contains("gone-away"), "{reason}");
        assert!(reason.contains("connector"), "{reason}");
    }

    #[tokio::test]
    async fn stage_resolved_connector_builds_the_sink_live() {
        let connectors = ConnectorStore::new();
        connectors.upsert(serde_json::json!({
            "id": "webhook:up",
            "name": "up",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }));
        let stage = stage_action_sink(
            &connectors,
            &webhook_record("act", "webhook:up", serde_json::json!({"path": "/p"})),
        )
        .expect("a resolvable webhook connector builds");
        assert_eq!(stage.wiring, ActionWiring::Live);
        let sink = stage.sink.expect("live wiring carries the sink");
        assert_eq!(sink.kind(), "webhook");
    }

    #[tokio::test]
    async fn targets_connector_matches_every_reference_field() {
        let connectors = ConnectorStore::new();
        assert!(!action_targets_connector(
            &connectors,
            &record("act", "http", "webhook:up"),
            "up"
        ));
        connectors.upsert(serde_json::json!({
            "id": "webhook:up",
            "name": "up",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }));
        assert!(action_targets_connector(
            &connectors,
            &record("act", "http", "webhook:up"),
            "up"
        ));
        assert!(action_targets_connector(
            &connectors,
            &record("act", "http", "up"),
            "up"
        ));
        // The legacy flat extras count as references too.
        let mut legacy = record("act", "http", "");
        legacy.params = serde_json::json!({"connector_id": "webhook:up"});
        assert!(action_targets_connector(&connectors, &legacy, "up"));
    }

    #[tokio::test]
    async fn webhook_sink_joins_the_action_path_and_refuses_other_methods() {
        let connectors = ConnectorStore::new();
        connectors.upsert(serde_json::json!({
            "id": "webhook:up",
            "name": "up",
            "type": "webhook",
            "url": "http://127.0.0.1:9/base",
        }));
        // An explicit non-post method refuses with the shape named.
        let error = build_action_sink(
            &webhook_record(
                "act",
                "webhook:up",
                serde_json::json!({"path": "/p", "method": "get"}),
            ),
            &connectors.get("up").expect("connector stored"),
        )
        .err()
        .expect("a non-post method refuses");
        assert!(error.contains("act") && error.contains("get"), "{error}");
        let sink = build_action_sink(
            &webhook_record("act", "webhook:up", serde_json::json!({"path": "/p"})),
            &connectors.get("up").expect("connector stored"),
        )
        .expect("a plain path builds")
        .expect("an owned kind builds a sink");
        assert_eq!(sink.kind(), "webhook");
        // A POST renders the joined path through the real connector builder.
        let rendered = stage_action_sink(
            &connectors,
            &webhook_record("act", "webhook:up", serde_json::json!({"path": "/p"})),
        )
        .expect("stage builds");
        assert!(rendered.sink.is_some());
    }

    #[tokio::test]
    async fn webhook_sink_fails_closed_on_the_invalid_headers() {
        let connectors = ConnectorStore::new();
        connectors.upsert(serde_json::json!({
            "id": "webhook:up",
            "name": "up",
            "type": "webhook",
            "url": "http://127.0.0.1:9",
        }));
        let doc = connectors.get("up").expect("connector stored");
        let error = build_action_sink(
            &webhook_record(
                "act",
                "webhook:up",
                serde_json::json!({"headers": {"good header": 1}}),
            ),
            &doc,
        )
        .err()
        .expect("a non-string header value refuses");
        assert!(error.contains("headers"), "{error}");
        let error = build_action_sink(
            &webhook_record(
                "act",
                "webhook:up",
                serde_json::json!({"headers": {"bad name": "x"}}),
            ),
            &doc,
        )
        .err()
        .expect("an invalid header name refuses");
        assert!(error.contains("header name"), "{error}");
        // A legacy flat `headers` extra covers the same field too.
        let mut legacy = record("act", "http", "webhook:up");
        legacy.params = serde_json::json!({"headers": {"X-Mv01": "v"}, "path": "/legacy"});
        assert!(
            build_action_sink(&legacy, &doc).is_ok(),
            "string headers build"
        );
    }

    #[tokio::test]
    async fn pgsql_sink_uses_the_connector_settings_and_refuses_the_missing_fields() {
        let loud = record("act", "pgsql", "postgres:up");
        // A missing connector field refuses with the field named.
        let error = build_pgsql_sink(&loud, &serde_json::json!({"username": "u"}))
            .err()
            .expect("a missing server refuses");
        assert!(error.contains("server"), "{error}");
        // A missing action sql refuses with the action parameter named.
        let error = build_pgsql_sink(
            &loud,
            &serde_json::json!({
                "server": "127.0.0.1:9", "database": "db", "username": "u",
            }),
        )
        .err()
        .expect("a missing sql refuses");
        assert!(error.contains("sql"), "{error}");
        // A legacy flat sql extra covers it as well.
        let mut legacy = record("act", "pgsql", "postgres:up");
        legacy.params = serde_json::json!({"sql": "INSERT $1"});
        let sink = build_pgsql_sink(
            &legacy,
            &serde_json::json!({"server": "127.0.0.1:9", "database": "db", "username": "u"}),
        )
        .expect("a valid connector plus legacy sql builds");
        assert_eq!(sink.kind(), "postgres");
    }
}
