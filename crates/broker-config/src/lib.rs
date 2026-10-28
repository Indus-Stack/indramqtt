//! Kernel-owned configuration registry.
//!
//! The registry holds one validated struct per configuration root
//! (admin users, MQTT users/ACLs, rules, connectors) inside a single
//! [`FullSnapshot`]. Readers take lock-free snapshots through
//! `arc_swap::ArcSwap`; writers go through an explicit `commit` that
//! validates the incoming root before it can become visible, so invalid
//! state is never observable. Persistence is a TOML document
//! (`state.toml`) written atomically (write-temp-then-rename with an
//! fsync before the rename).

use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use arc_swap::ArcSwap;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod layers;
pub mod runtime;
pub mod schema;
pub mod secrets;

pub use layers::{
    load_layered, load_layered_with_env, CliOverrides, LayeredConfig, DEFAULT_CONFIG_DIR,
};
pub use runtime::{ConfigDiff, ConfigHistory, ConfigVersion};
pub use schema::BrokerConfig;

/// Name of the persistence file inside the state directory.
pub const STATE_FILE_NAME: &str = "state.toml";

/// Connector types the kernel knows how to instantiate.
///
/// Mirrors the sink kinds registered in `broker-connectors` (`kind()`
/// values plus the `webhook`/`console`/`mqtt_bridge` management aliases).
/// Unknown types are rejected at validation time so a typo can never be
/// persisted.
pub const KNOWN_CONNECTOR_TYPES: &[&str] = &[
    "alloydb",
    "aws_iot",
    "azure_blob",
    "azure_eventhubs",
    "azure_iot",
    "bigquery",
    "cassandra",
    "clickhouse",
    "cockroachdb",
    "confluent",
    "console",
    "couchbase",
    "databricks",
    "datalayers",
    "disk_log",
    "doris",
    "dynamodb",
    "elasticsearch",
    "gcp_iot",
    "gcp_pubsub",
    "greptimedb",
    "http",
    "influxdb",
    "iotdb",
    "kafka",
    "kinesis",
    "mongodb",
    "mqtt",
    "mqtt_bridge",
    "mssql",
    "mysql",
    "oci_streaming",
    "opc_ua",
    "opentsdb",
    "oracle",
    "postgres",
    "pulsar",
    "rabbitmq",
    "redis",
    "redshift",
    "rocketmq",
    "s3",
    "s3_tables",
    "snowflake",
    "sparkplug_b",
    "tablestore",
    "tdengine",
    "test",
    "timescaledb",
    "timestream",
    "webhook",
    // Documented names for the same backends. The console sends these.
    // Each alias maps to the runtime of its base kind.
    "azure_blob_storage",
    "azure_event_hub_producer",
    "aws_timestream",
    "confluent_producer",
    "gcp_pubsub_consumer",
    "gcp_pubsub_producer",
    "kafka_consumer",
    "kafka_producer",
    "opents",
    "pgsql",
    "s3tables",
    "sqlserver",
    "dynamo",
    "timescale",
];

/// Returns true for a connector type the kernel can instantiate.
#[must_use]
pub fn is_known_connector_type(kind: &str) -> bool {
    KNOWN_CONNECTOR_TYPES.contains(&kind)
}

/// Action kinds the kernel can execute as rule-engine actions.
///
/// Must match the action-types catalogue (W3-01 kind registry): every
/// entry here names an executable connector kind, and every catalogue
/// entry is listed here. The connector registry additionally accepts
/// `http`, `mqtt` and `test`, which are not executable action kinds
/// (management/compat aliases and a unit-test helper), so they are
/// excluded here; `ActionsConf::validate` uses this list so a persisted
/// action is always creatable through the API and vice versa.
pub const KNOWN_ACTION_TYPES: &[&str] = &[
    "alloydb",
    "aws_iot",
    "azure_blob",
    "azure_eventhubs",
    "azure_iot",
    "bigquery",
    "cassandra",
    "clickhouse",
    "cockroachdb",
    "confluent",
    "console",
    "couchbase",
    "databricks",
    "datalayers",
    "disk_log",
    "doris",
    "dynamodb",
    "elasticsearch",
    "gcp_iot",
    "gcp_pubsub",
    "greptimedb",
    "influxdb",
    "iotdb",
    "kafka",
    "kinesis",
    "mongodb",
    "mqtt_bridge",
    "mssql",
    "mysql",
    "oci_streaming",
    "opc_ua",
    "opentsdb",
    "oracle",
    "postgres",
    "pulsar",
    "rabbitmq",
    "redis",
    "redshift",
    "rocketmq",
    "s3",
    "s3_tables",
    "snowflake",
    "sparkplug_b",
    "tablestore",
    "tdengine",
    "timescaledb",
    "timestream",
    "webhook",
    // Documented names for the same backends. The console sends these.
    // Each alias maps to the runtime of its base kind.
    "http",
    "mqtt",
    "kafka_producer",
    "kafka_consumer",
    "confluent_producer",
    "pgsql",
    "timescale",
    "opents",
    "sqlserver",
    "dynamo",
    "s3tables",
    "gcp_pubsub_producer",
    "azure_event_hub_producer",
    "azure_blob_storage",
    "azure_event_grid",
    "aws_timestream",
    "snowflake_streaming",
];

/// Returns true for an action kind the kernel can execute.
#[must_use]
pub fn is_known_action_type(kind: &str) -> bool {
    KNOWN_ACTION_TYPES.contains(&kind)
}

/// Source kinds the kernel can instantiate as ingress.
///
/// Mirrors the source-type registry: every executable action kind plus
/// the `http`/`mqtt` ingress aliases. The unit-test `test` helper has no
/// runtime and is excluded here; `SourcesConf::validate` uses this list
/// so a persisted source is always creatable through the API and vice
/// versa.
pub const KNOWN_SOURCE_TYPES: &[&str] = &[
    "alloydb",
    "aws_iot",
    "azure_blob",
    "azure_eventhubs",
    "azure_iot",
    "bigquery",
    "cassandra",
    "clickhouse",
    "cockroachdb",
    "confluent",
    "console",
    "couchbase",
    "databricks",
    "datalayers",
    "disk_log",
    "doris",
    "dynamodb",
    "elasticsearch",
    "gcp_iot",
    "gcp_pubsub",
    "greptimedb",
    "http",
    "influxdb",
    "iotdb",
    "kafka",
    "kinesis",
    "mongodb",
    "mqtt",
    "mqtt_bridge",
    "mssql",
    "mysql",
    "oci_streaming",
    "opc_ua",
    "opentsdb",
    "oracle",
    "postgres",
    "pulsar",
    "rabbitmq",
    "redis",
    "redshift",
    "rocketmq",
    "s3",
    "s3_tables",
    "snowflake",
    "sparkplug_b",
    "tablestore",
    "tdengine",
    "timescaledb",
    "timestream",
    "webhook",
    // Documented names for the same backends. The console sends these.
    // Each alias maps to the runtime of its base kind.
    "kafka_consumer",
    "kafka_producer",
    "gcp_pubsub_consumer",
    "gcp_pubsub_producer",
    "azure_event_hub_producer",
    "azure_event_grid",
];

/// Returns true for a source kind the kernel can instantiate.
#[must_use]
pub fn is_known_source_type(kind: &str) -> bool {
    KNOWN_SOURCE_TYPES.contains(&kind)
}

/// Returns true for an ACL action the kernel can evaluate
/// (`publish`, `subscribe` or `all`, case-insensitive).
#[must_use]
pub fn is_known_acl_action(action: &str) -> bool {
    matches!(
        action.to_ascii_lowercase().as_str(),
        "publish" | "subscribe" | "all"
    )
}

/// Upper bound for stored authenticator-chain entries. Writes past this
/// size are rejected instead of growing the list without limit. Reason:
/// the CONNECT path snapshots the whole chain once per connect, so the
/// chain must stay small and bounded; 8 covers the documented backend
/// families with headroom while keeping per-connect clones cheap.
// The chain holds at most 8 entries.
// Writes past the cap fail closed.
pub const MAX_AUTHN_CHAIN_ENTRIES: usize = 8;

/// Authenticator backends the kernel can store. Only `built_in_database`
/// executes in the parity waves; every other backend's config is stored
/// and reported, never claimed as live. The set covers the password, JWT,
/// LDAP, HTTP, Redis, SQL, Mongo and file families named for the
/// authenticator registries.
pub const KNOWN_AUTHN_BACKENDS: &[&str] = &[
    "built_in_database",
    "mysql",
    "postgresql",
    "mongodb",
    "redis",
    "http",
    "jwt",
    "scram",
    "ldap",
    "file",
    "kerberos",
    "cinfo",
];

/// Returns true for an authenticator backend the kernel can store.
#[must_use]
pub fn is_known_authn_backend(backend: &str) -> bool {
    KNOWN_AUTHN_BACKENDS.contains(&backend)
}

/// Credential mechanisms the authenticator chain addresses.
pub const KNOWN_AUTHN_MECHANISMS: &[&str] = &["password_based", "jwt", "scram", "cinfo", "gssapi"];

/// Returns true for an authenticator mechanism the kernel can store.
#[must_use]
pub fn is_known_authn_mechanism(mechanism: &str) -> bool {
    KNOWN_AUTHN_MECHANISMS.contains(&mechanism)
}

/// Errors produced by validation, loading and saving configuration.
#[derive(Debug, Error)]
pub enum ConfigError {
    /// A root failed validation. The message always names the field.
    #[error("{0}")]
    Invalid(String),
    /// The state file could not be read, parsed or validated.
    /// Always names the file and the underlying field/reason.
    #[error("config file {file}: {reason}")]
    Load { file: String, reason: String },
    /// The snapshot could not be serialised or written.
    #[error("config save failed for {file}: {reason}")]
    Save { file: String, reason: String },
}

/// One dashboard/API administrator.
///
/// `password_hash` holds the SCRAM-SHA-256 verifier written by the
/// management API store (never plaintext); the registry treats it as an
/// opaque non-empty string. `role` is `administrator` or `viewer`
/// (missing roles from pre-persistence files load as `viewer`, the least
/// privilege). `must_change_password` forces a password change on next
/// login, matching the in-memory store flag.
///
/// `mfa_secret` holds the TOTP enrolment secret as standard base64 of the
/// raw bytes (empty = no enrolment; the secret must stay recoverable so
/// the management plane can verify one-time codes, unlike the password
/// verifier which is a one-way hash). `mfa_enabled` flips from false to
/// true on the first successful code verification. `mfa_last_step` is the
/// most recently verified 30 s counter for single-use (replay) rejection.
/// All three default for pre-MFA files (no enrolment, disabled, no use).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminUser {
    /// Login name; must be non-empty and unique within the root.
    pub username: String,
    /// Password hash; must be non-empty.
    pub password_hash: String,
    /// Management role (`administrator` or `viewer`).
    #[serde(default = "default_admin_user_role")]
    pub role: String,
    /// Free-form description shown by the user list.
    #[serde(default)]
    pub description: String,
    /// Whether the next login must change the password.
    #[serde(default)]
    pub must_change_password: bool,
    /// TOTP enrolment secret, standard base64 of the raw bytes;
    /// empty means no enrolment.
    #[serde(default)]
    pub mfa_secret: String,
    /// Whether MFA is enabled (set on first successful verification).
    #[serde(default)]
    pub mfa_enabled: bool,
    /// Most recently verified 30 s TOTP counter, for replay rejection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mfa_last_step: Option<u64>,
}

impl Default for AdminUser {
    fn default() -> Self {
        Self {
            username: String::new(),
            password_hash: String::new(),
            description: String::new(),
            role: default_admin_user_role(),
            must_change_password: false,
            mfa_secret: String::new(),
            mfa_enabled: false,
            mfa_last_step: None,
        }
    }
}

/// Serde default for a missing admin role: least privilege.
fn default_admin_user_role() -> String {
    "viewer".to_string()
}

/// Root: dashboard/API administrators.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AdminUsersConf {
    #[serde(default)]
    pub users: Vec<AdminUser>,
}

impl AdminUsersConf {
    /// Validates usernames (non-empty, unique), password hashes and roles.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut seen = HashSet::with_capacity(self.users.len());
        for (index, user) in self.users.iter().enumerate() {
            if user.username.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "admin_users.users[{index}].username must not be empty (field `username`)"
                )));
            }
            if user.password_hash.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "admin_users.users[{index}].password_hash must not be empty (field `password_hash`)"
                )));
            }
            if user.role != "administrator" && user.role != "viewer" {
                return Err(ConfigError::Invalid(format!(
                    "admin_users.users[{index}].role {:?} is unknown (field `role`)",
                    user.role
                )));
            }
            if !seen.insert(user.username.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "admin_users.users[{index}].username {:?} is duplicated (field `username`)",
                    user.username
                )));
            }
            // MFA state: an enabled record must carry a decodable secret;
            // a record with no secret must not claim to be enabled.
            if !user.mfa_secret.is_empty() {
                use base64::Engine as _;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(&user.mfa_secret)
                    .map_err(|_| {
                        ConfigError::Invalid(format!(
                            "admin_users.users[{index}].mfa_secret is not valid base64 (field `mfa_secret`)"
                        ))
                    })?;
                if decoded.len() < 10 || decoded.len() > 64 {
                    return Err(ConfigError::Invalid(format!(
                        "admin_users.users[{index}].mfa_secret must decode to 10..=64 bytes (field `mfa_secret`)"
                    )));
                }
            } else if user.mfa_enabled {
                return Err(ConfigError::Invalid(format!(
                    "admin_users.users[{index}].mfa_enabled must not be true without a secret (field `mfa_enabled`)"
                )));
            }
        }
        Ok(())
    }
}

/// One MQTT credential.
///
/// Quota bounds are optional per field (`None` = unlimited); missing
/// fields in pre-persistence files load as unlimited, and unset bounds
/// are omitted from the persisted file so quota-free records keep their
/// historical shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MqttUser {
    /// Client username; must be non-empty and unique within the root.
    pub username: String,
    /// Password hash; must be non-empty.
    pub password_hash: String,
    /// Maximum concurrent connections for this user (`None` = unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,
    /// Maximum sustained publishes per second (`None` = unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_publish_rate: Option<u32>,
    /// Burst allowance for the publish rate limiter (`None` = unlimited).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_publish_burst: Option<u32>,
}

/// One ACL entry attached to an MQTT user.
///
/// `username` is the client-id pattern (`*` matches every client),
/// `topic` the MQTT topic filter, `action` one of `publish`,
/// `subscribe` or `all` (case-insensitive), and `allow` selects an
/// allow (`true`) or deny (`false`) entry. The four fields map 1:1 onto
/// `broker_auth::AclRule`, so a snapshot round-trip preserves the exact
/// first-match-wins decision. Entries written before `allow` existed
/// load as `true` (allow), preserving their historical meaning.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AclConf {
    /// Username this entry applies to; must name a non-empty user.
    pub username: String,
    /// Topic filter; must be non-empty.
    pub topic: String,
    /// Action (`publish`, `subscribe` or `all`); must be non-empty.
    pub action: String,
    /// Whether the entry allows (`true`) or denies (`false`) access.
    #[serde(default = "default_acl_allow")]
    pub allow: bool,
}

/// Serde default for a missing ACL permission: historical entries only
/// expressed allows, so they keep meaning allow.
fn default_acl_allow() -> bool {
    true
}

/// Upper bound for persisted per-tenant credential tables (TM-01).
/// Reason: one table is one map header plus short strings, so 1024
/// entries bound the persisted file near tens of KiB while covering
/// multi-tenant installs far past real deployments; writes past the cap
/// are rejected instead of growing the file without limit. Mirrors the
/// credential-store tenant cap.
pub const MAX_MQTT_TENANT_TABLES: usize = 1024;

/// Longest persisted tenant id (TM-01). Reason: the tenant id joins
/// session keys, credential keys and log lines, so 128 chars bound key
/// memory while covering hierarchical names. Mirrors the session bound.
pub const MAX_MQTT_TENANT_ID_LEN: usize = 128;

/// Upper bound for MQTT users across all persisted tenants (TM-01).
/// Reason: each entry holds a verifier plus map overhead, so an
/// unbounded file lets one write exhaust the kernel; 100,000 entries
/// cover fleet provisioning while keeping per-CONNECT lookups cheap.
/// Mirrors the credential-store user cap.
pub const MAX_MQTT_USERS_TOTAL: usize = 100_000;

/// Upper bound for ACL rules in one persisted tenant table (TM-01).
/// Reason: authorisation scans first-match-wins linearly, so 10,000
/// entries bound per-check work to a small constant while covering
/// large fleets; writes past the cap are rejected instead of growing
/// the scan without limit. Mirrors the credential-store rule cap.
pub const MAX_MQTT_ACL_PER_TENANT: usize = 10_000;

/// One tenant credential table inside the persisted snapshot (TM-01).
///
/// `tenant` names a non-default tenant; the default tenant lives in the
/// root `users`/`acls` fields so files written before per-tenant storage
/// load unchanged as the default tenant. `users` and `acls` follow the
/// same shape and validation as the root fields, scoped to this tenant:
/// the same username in two tenants is two independent identities.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TenantMqttConf {
    /// Tenant id; must be non-empty, unique within the root, and must not
    /// name the default tenant (field `tenant`).
    pub tenant: String,
    /// Tenant users; usernames must be unique within this table.
    #[serde(default)]
    pub users: Vec<MqttUser>,
    /// Tenant ACLs in evaluation order (first match wins).
    #[serde(default)]
    pub acls: Vec<AclConf>,
}

/// Root: MQTT users and their ACLs.
///
/// `users`/`acls` hold the default tenant, exactly as before per-tenant
/// storage: files written before this change load with an empty
/// `tenants` list and behave identically. `tenants` holds one table per
/// non-default tenant; every table persists in `state.toml`, survives a
/// restart and a reload, and ships in the export and the backup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MqttUsersConf {
    #[serde(default)]
    pub users: Vec<MqttUser>,
    #[serde(default)]
    pub acls: Vec<AclConf>,
    /// Per-tenant credential tables (TM-01; empty for pre-change files).
    #[serde(default)]
    pub tenants: Vec<TenantMqttConf>,
}

/// True when `id` may name a persisted tenant table: non-empty, at most
/// [`MAX_MQTT_TENANT_ID_LEN`] chars and free of control characters.
#[must_use]
pub fn is_valid_mqtt_tenant_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_MQTT_TENANT_ID_LEN && !id.chars().any(|c| c.is_control())
}

impl MqttUsersConf {
    /// Validates users (non-empty, unique, password present) and ACLs.
    pub fn validate(&self) -> Result<(), ConfigError> {
        Self::validate_users("mqtt_users.users", &self.users)?;
        Self::validate_acls("mqtt_users.acls", &self.acls)?;
        if self.acls.len() > MAX_MQTT_ACL_PER_TENANT {
            return Err(ConfigError::Invalid(format!(
                "mqtt_users.acls holds {} entries, at most {MAX_MQTT_ACL_PER_TENANT} (field `acls`)",
                self.acls.len(),
            )));
        }
        if self.tenants.len() > MAX_MQTT_TENANT_TABLES {
            return Err(ConfigError::Invalid(format!(
                "mqtt_users.tenants holds {} entries, at most {MAX_MQTT_TENANT_TABLES} (field `tenants`)",
                self.tenants.len(),
            )));
        }
        let mut seen_tenants = HashSet::with_capacity(self.tenants.len());
        let mut total_users = self.users.len();
        for (index, table) in self.tenants.iter().enumerate() {
            if !is_valid_mqtt_tenant_id(&table.tenant) {
                return Err(ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].tenant {:?} is not a valid tenant id (field `tenant`)",
                    table.tenant
                )));
            }
            if table.tenant == "default" {
                return Err(ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].tenant {:?} must live in the root users/acls fields (field `tenant`)",
                    table.tenant
                )));
            }
            if !seen_tenants.insert(table.tenant.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].tenant {:?} is duplicated (field `tenant`)",
                    table.tenant
                )));
            }
            Self::validate_users(&format!("mqtt_users.tenants[{index}].users"), &table.users)?;
            Self::validate_acls(&format!("mqtt_users.tenants[{index}].acls"), &table.acls)?;
            if table.acls.len() > MAX_MQTT_ACL_PER_TENANT {
                return Err(ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].acls holds {} entries, at most {MAX_MQTT_ACL_PER_TENANT} (field `acls`)",
                    table.acls.len(),
                )));
            }
            total_users += table.users.len();
        }
        if total_users > MAX_MQTT_USERS_TOTAL {
            return Err(ConfigError::Invalid(format!(
                "mqtt_users holds {total_users} users, at most {MAX_MQTT_USERS_TOTAL} (field `users`)"
            )));
        }
        Ok(())
    }

    /// Validates one user list (non-empty, unique, password present).
    fn validate_users(path: &str, users: &[MqttUser]) -> Result<(), ConfigError> {
        let mut seen = HashSet::with_capacity(users.len());
        for (index, user) in users.iter().enumerate() {
            if user.username.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "{path}[{index}].username must not be empty (field `username`)"
                )));
            }
            if user.password_hash.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "{path}[{index}].password_hash must not be empty (field `password_hash`)"
                )));
            }
            if !seen.insert(user.username.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "{path}[{index}].username {:?} is duplicated (field `username`)",
                    user.username
                )));
            }
        }
        Ok(())
    }

    /// Validates one ACL list (known action, non-empty fields).
    fn validate_acls(path: &str, acls: &[AclConf]) -> Result<(), ConfigError> {
        for (index, acl) in acls.iter().enumerate() {
            if acl.username.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "{path}[{index}].username must not be empty (field `username`)"
                )));
            }
            if acl.topic.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "{path}[{index}].topic must not be empty (field `topic`)"
                )));
            }
            if acl.action.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "{path}[{index}].action must not be empty (field `action`)"
                )));
            }
            if !is_known_acl_action(&acl.action) {
                return Err(ConfigError::Invalid(format!(
                    "{path}[{index}].action {:?} is unknown (field `action`)",
                    acl.action
                )));
            }
        }
        Ok(())
    }
}

/// One persisted rule action: the full lossless form of
/// `broker_rules::RuleAction` (every variant field round-trips; no
/// display-string coercion).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RuleActionEntry {
    /// Republish to `topic` with MQTT wire QoS 0/1/2.
    Republish {
        /// Destination topic; must be non-empty.
        topic: String,
        /// MQTT wire QoS (0, 1 or 2).
        qos: u8,
    },
    /// Emit a structured log line for the matched event.
    Log,
    /// Forward to a registered external connector by id.
    ForwardConnector {
        /// Connector id; must be non-empty.
        connector_id: String,
    },
    /// The built-in `republish` function of the reference API: topic,
    /// payload, QoS and retain flag are fixed values or templates.
    #[serde(rename = "republish_function")]
    RepublishFunction {
        /// The arguments as the API documents them, defaults filled in.
        args: RepublishArgs,
    },
    /// The built-in `console` function of the reference API: write the
    /// rule result to the log.
    Console,
}

/// Arguments of the built-in `republish` rule function, in the form the
/// reference API documents them. A value that holds `${...}` is a template
/// that the rule engine renders for each result row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepublishArgs {
    /// Target topic; a template.
    pub topic: String,
    /// Payload; a template. The default is `${payload}`.
    #[serde(default = "default_republish_payload")]
    pub payload: String,
    /// QoS: a number from 0 to 2 or a template. The default is `${qos}`.
    #[serde(default = "default_republish_qos")]
    pub qos: RepublishQos,
    /// Retain flag: a boolean or a template. The default is `${retain}`.
    #[serde(default = "default_republish_retain")]
    pub retain: RepublishFlag,
    /// MQTT properties to set. Keys are the property names of the
    /// reference API; values are templates.
    #[serde(default)]
    pub mqtt_properties: BTreeMap<String, String>,
    /// A single `${...}` reference to a map of user properties. An empty
    /// string means none. The default is `${user_properties}`.
    #[serde(default = "default_republish_user_properties")]
    pub user_properties: String,
    /// When true the message goes straight to the subscribers and is not
    /// retained. The default is false.
    #[serde(default = "default_republish_direct_dispatch")]
    pub direct_dispatch: RepublishFlag,
}

/// QoS of a republish: a fixed number or a template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RepublishQos {
    /// Fixed QoS, 0 to 2.
    Fixed(u8),
    /// Template that gives the QoS as text.
    Template(String),
}

/// A flag of a republish: a fixed boolean or a template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RepublishFlag {
    /// Fixed value.
    Fixed(bool),
    /// Template that gives `true` or `false` as text.
    Template(String),
}

fn default_republish_payload() -> String {
    "${payload}".to_string()
}

fn default_republish_qos() -> RepublishQos {
    RepublishQos::Template("${qos}".to_string())
}

fn default_republish_retain() -> RepublishFlag {
    RepublishFlag::Template("${retain}".to_string())
}

fn default_republish_user_properties() -> String {
    "${user_properties}".to_string()
}

fn default_republish_direct_dispatch() -> RepublishFlag {
    RepublishFlag::Fixed(false)
}

/// Serde default for a missing rule enable flag: historical entries
/// predate the flag and were always enabled.
fn default_rule_enabled() -> bool {
    true
}

/// Serde default for a missing rule tenant: historical entries predate
/// tenancy and ran in the default tenant.
fn default_rule_tenant() -> String {
    "default".to_string()
}

/// Longest accepted rule tenant id (128 chars). Reason: mirrors the
/// session tenant bound, so the id joins routing keys with bounded
/// key memory.
pub const MAX_RULE_TENANT_LEN: usize = 128;

/// True when a rule tenant id may own a rule: non-empty, at most
/// 128 chars, no control chars. Empty fails here; callers map empty
/// to the default tenant before this check.
pub fn is_valid_rule_tenant(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_RULE_TENANT_LEN && !id.chars().any(|c| c.is_control())
}

/// Map an absent or blank rule tenant to the default tenant.
pub fn canonical_rule_tenant(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        "default".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Upper bound for stored routing rules. Creates past this size are
/// rejected instead of growing the map without limit. Reason: the list
/// handler clones the whole snapshot once per request, so the catalogue
/// must stay small and bounded; 1024 covers the documented rule set with
/// headroom while keeping per-request clones cheap.
// The catalogue holds at most 1024 rules.
// Creates past the cap fail closed.
pub const MAX_RULES: usize = 1024;

/// Longest accepted rule `id` (256 bytes). Reason: ids are map keys
/// cloned into every list snapshot and persisted per entry, so an
/// unbounded id lets one record bloat each list read; 256 covers
/// operator naming with headroom while keeping per-request clones cheap.
pub const MAX_RULE_ID_LEN: usize = 256;

/// One routing rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RuleEntry {
    /// Rule id; must be non-empty and unique within the root.
    pub id: String,
    /// Rule name; free-form (pre-persistence files load as empty).
    #[serde(default)]
    pub name: String,
    /// Tenant that owns the rule. Empty loads as the default tenant so
    /// files written before tenancy keep their behaviour.
    #[serde(default = "default_rule_tenant")]
    pub tenant: String,
    /// MQTT topic filter; must be non-empty.
    pub topic_filter: String,
    /// Optional streaming-SQL program (`None` = match without SQL).
    /// Pre-persistence files always carry a string, which loads as
    /// `Some`; a present but empty string is rejected at validation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sql_query: Option<String>,
    /// Whether the rule evaluates at ingress.
    #[serde(default = "default_rule_enabled")]
    pub enabled: bool,
    /// Full action list in execution order.
    #[serde(default)]
    pub actions: Vec<RuleActionEntry>,
}

/// Root: routing rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RulesConf {
    #[serde(default)]
    pub rules: Vec<RuleEntry>,
}

impl RulesConf {
    /// Validates the registry length cap, rule ids (non-empty, bounded,
    /// unique), required fields and the full action list.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.rules.len() > MAX_RULES {
            return Err(ConfigError::Invalid(format!(
                "rules.rules holds {} entries, at most {} (field `rules`)",
                self.rules.len(),
                MAX_RULES
            )));
        }
        let mut seen = HashSet::with_capacity(self.rules.len());
        for (index, rule) in self.rules.iter().enumerate() {
            if rule.id.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "rules.rules[{index}].id must not be empty (field `id`)"
                )));
            }
            if rule.id.len() > MAX_RULE_ID_LEN {
                return Err(ConfigError::Invalid(format!(
                    "rules.rules[{index}].id {:?} is too long (field `id`)",
                    rule.id
                )));
            }
            if !is_valid_rule_tenant(&rule.tenant) {
                return Err(ConfigError::Invalid(format!(
                    "rules.rules[{index}].tenant {:?} is not valid (field `tenant`)",
                    rule.tenant
                )));
            }
            if rule.topic_filter.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "rules.rules[{index}].topic_filter must not be empty (field `topic_filter`)"
                )));
            }
            if let Some(sql) = rule.sql_query.as_ref() {
                if sql.trim().is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "rules.rules[{index}].sql_query must not be empty (field `sql_query`)"
                    )));
                }
            }
            for (action_index, action) in rule.actions.iter().enumerate() {
                match action {
                    RuleActionEntry::Republish { topic, qos } => {
                        if topic.trim().is_empty() {
                            return Err(ConfigError::Invalid(format!(
                                "rules.rules[{index}].actions[{action_index}].topic must not be empty (field `topic`)"
                            )));
                        }
                        if *qos > 2 {
                            return Err(ConfigError::Invalid(format!(
                                "rules.rules[{index}].actions[{action_index}].qos {qos} is out of range (field `qos`)"
                            )));
                        }
                    }
                    RuleActionEntry::Log | RuleActionEntry::Console => {}
                    RuleActionEntry::RepublishFunction { args } => {
                        if args.topic.trim().is_empty() {
                            return Err(ConfigError::Invalid(format!(
                                "rules.rules[{index}].actions[{action_index}].args.topic must not be empty (field `topic`)"
                            )));
                        }
                        if let RepublishQos::Fixed(qos) = &args.qos {
                            if *qos > 2 {
                                return Err(ConfigError::Invalid(format!(
                                    "rules.rules[{index}].actions[{action_index}].args.qos {qos} is out of range (field `qos`)"
                                )));
                            }
                        }
                    }
                    RuleActionEntry::ForwardConnector { connector_id } => {
                        if connector_id.trim().is_empty() {
                            return Err(ConfigError::Invalid(format!(
                                "rules.rules[{index}].actions[{action_index}].connector_id must not be empty (field `connector_id`)"
                            )));
                        }
                    }
                }
            }
            if !seen.insert(rule.id.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "rules.rules[{index}].id {:?} is duplicated (field `id`)",
                    rule.id
                )));
            }
        }
        Ok(())
    }
}

/// Serde default for a missing connector enable flag: historical
/// entries predate the flag and were always enabled.
fn default_connector_enabled() -> bool {
    true
}

/// One data connector.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectorEntry {
    /// Connector id; must be non-empty and unique within the root.
    pub id: String,
    /// Connector type; must name a known connector (field `connector_type`).
    pub connector_type: String,
    /// Whether the connector is enabled (missing entries from
    /// pre-persistence files load as `true`, the historical behaviour).
    #[serde(default = "default_connector_enabled")]
    pub enable: bool,
    /// Opaque connector configuration (URL, table, …); may be empty.
    /// The management API stores the full connector params here as a
    /// JSON document (everything the create call accepts except the
    /// derived `status`/`node_status` display values, which are
    /// recomputed at read time and never persisted).
    #[serde(default)]
    pub config: String,
}

/// Root: data connectors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ConnectorsConf {
    #[serde(default)]
    pub connectors: Vec<ConnectorEntry>,
}

impl ConnectorsConf {
    /// Validates ids (non-empty, unique) and connector types (known).
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut seen = HashSet::with_capacity(self.connectors.len());
        for (index, connector) in self.connectors.iter().enumerate() {
            if connector.id.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "connectors.connectors[{index}].id must not be empty (field `id`)"
                )));
            }
            if connector.connector_type.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "connectors.connectors[{index}].connector_type must not be empty (field `connector_type`)"
                )));
            }
            if !is_known_connector_type(&connector.connector_type) {
                return Err(ConfigError::Invalid(format!(
                    "connectors.connectors[{index}].connector_type {:?} is unknown (field `connector_type`)",
                    connector.connector_type
                )));
            }
            if !seen.insert(connector.id.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "connectors.connectors[{index}].id {:?} is duplicated (field `id`)",
                    connector.id
                )));
            }
        }
        Ok(())
    }
}

/// One authenticator-chain entry: the ordered chain slot the
/// management API lists and creates. `id` is the chain key (by
/// convention `{mechanism}:{backend}`, e.g.
/// `password_based:built_in_database`); `mechanism` names the credential
/// mechanism; `backend` names the credential store; `enable` selects
/// whether the slot participates; `config` carries the backend-specific
/// fields as a JSON document (may be empty for the built-in database).
/// Only `built_in_database` executes in the parity waves; every other
/// backend's config is stored and reported, never claimed as live.
// The entry carries id, mechanism, backend, enable and config.
// Fields the broker cannot supply stay omitted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuthnEntry {
    /// Chain key; must be non-empty and unique within the root.
    pub id: String,
    /// Credential mechanism; must name a known mechanism (field `mechanism`).
    pub mechanism: String,
    /// Credential store; must name a known backend (field `backend`).
    pub backend: String,
    /// Whether the slot participates (missing entries from
    /// pre-persistence files load as `true`, the historical behaviour).
    #[serde(default = "default_authn_enabled")]
    pub enable: bool,
    /// Opaque backend configuration as a JSON document; may be empty.
    #[serde(default)]
    pub config: String,
}

/// Serde default for a missing authenticator enable flag: historical
/// entries predate the flag and were always enabled.
fn default_authn_enabled() -> bool {
    true
}

/// Default node authentication cache entry cap (10 000 entries,
/// LRU-evicted). Reason: the cache is management-plane observability over
/// CONNECT decisions, so it must stay bounded; 10 000 distinct users cover
/// ordinary nodes with headroom while keeping the status snapshot cheap
/// (one short lock, at most the capped small entries).
pub const DEFAULT_AUTHN_NODE_CACHE_MAX: usize = 10_000;
/// Upper bound for the node authentication cache cap (1 000 000 entries).
/// Reason: caps above this would let one node hold unbounded auth state;
/// larger values are rejected instead of growing without limit.
pub const MAX_AUTHN_NODE_CACHE_MAX: usize = 1_000_000;

/// Node-level authentication cache configuration. Only the enabled flag
/// and the entry cap persist via the registry; cached entries themselves
/// are memory-only and never survive a restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthnNodeCacheConf {
    /// Whether successful CONNECT decisions are recorded (default true).
    #[serde(default = "default_authn_node_cache_enabled")]
    pub enabled: bool,
    /// Entry cap for the node cache (default 10 000, at most 1 000 000).
    #[serde(default = "default_authn_node_cache_max")]
    pub max_count: usize,
}

impl Default for AuthnNodeCacheConf {
    fn default() -> Self {
        Self {
            enabled: true,
            max_count: DEFAULT_AUTHN_NODE_CACHE_MAX,
        }
    }
}

fn default_authn_node_cache_enabled() -> bool {
    true
}

fn default_authn_node_cache_max() -> usize {
    DEFAULT_AUTHN_NODE_CACHE_MAX
}

impl AuthnNodeCacheConf {
    /// Validates the entry cap (finite and non-zero, at most
    /// [`MAX_AUTHN_NODE_CACHE_MAX`]).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_count == 0 || self.max_count > MAX_AUTHN_NODE_CACHE_MAX {
            return Err(ConfigError::Invalid(format!(
                "authn_node_cache.max_count holds {}, must be 1..={} (field `max_count`)",
                self.max_count, MAX_AUTHN_NODE_CACHE_MAX
            )));
        }
        Ok(())
    }
}

/// Default node-cache TTL inside authentication settings ("1m").
/// Reason: cached CONNECT decisions must expire quickly so a credential
/// change takes effect; one minute bounds staleness while keeping
/// successful reconnects cheap.
fn default_authn_settings_cache_ttl() -> String {
    "1m".to_string()
}

/// Default node-cache cleanup interval inside authentication settings
/// ("1m"). Reason: expiry sweeps run on the same cadence as the TTL so
/// expired entries do not linger past one TTL window.
fn default_authn_settings_cleanup_interval() -> String {
    "1m".to_string()
}

/// Default node-cache statistics update interval ("5s"). Reason: cache
/// counters feed management status reads, so a few seconds keep the
/// status fresh without waking the sweeper per request.
fn default_authn_settings_stat_update_interval() -> String {
    "5s".to_string()
}

/// Default node-cache entry cap inside authentication settings (10 000
/// entries). Reason: the settings snapshot is cloned once per CONNECT,
/// so the cap must stay small and bounded; 10 000 covers ordinary nodes
/// with headroom while keeping per-connect clones cheap.
fn default_authn_settings_max_count() -> usize {
    DEFAULT_AUTHN_NODE_CACHE_MAX
}

/// Default node-cache memory cap inside authentication settings
/// ("100MB"). Reason: entries are small per-username records, so 100MB
/// bounds the cache far above the entry-cap worst case while letting an
/// operator raise it explicitly with `unlimited`.
fn default_authn_settings_max_memory() -> String {
    "100MB".to_string()
}

/// Default built-in record-count refresh interval ("1h"). Reason: the
/// built-in database recounts its user records hourly so management
/// status stays fresh without scanning on every request.
fn default_authn_settings_refresh_interval() -> String {
    "1h".to_string()
}

/// Node-cache half of the global authentication settings: the enabled
/// flag, expiry cadences and bounds applied to the node auth cache.
/// Only the enabled flag and the entry cap drive broker behaviour (via
/// the settings subscriber); the cadences and memory cap are stored,
/// validated and reported.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthnSettingsNodeCacheConf {
    /// Whether successful CONNECT decisions are recorded (default true).
    /// Accepts `enabled` as an alias for older documents.
    #[serde(default = "default_authn_node_cache_enabled", alias = "enabled")]
    pub enable: bool,
    /// Cache entry TTL (default "1m").
    #[serde(default = "default_authn_settings_cache_ttl")]
    pub cache_ttl: String,
    /// Expired-entry sweep interval (default "1m").
    #[serde(default = "default_authn_settings_cleanup_interval")]
    pub cleanup_interval: String,
    /// Statistics refresh interval (default "5s").
    #[serde(default = "default_authn_settings_stat_update_interval")]
    pub stat_update_interval: String,
    /// Entry cap (default 10 000, at most 1 000 000).
    #[serde(default = "default_authn_settings_max_count")]
    pub max_count: usize,
    /// Memory cap (default "100MB", or explicit `unlimited` opt-in).
    #[serde(default = "default_authn_settings_max_memory")]
    pub max_memory: String,
}

impl Default for AuthnSettingsNodeCacheConf {
    fn default() -> Self {
        Self {
            enable: true,
            cache_ttl: default_authn_settings_cache_ttl(),
            cleanup_interval: default_authn_settings_cleanup_interval(),
            stat_update_interval: default_authn_settings_stat_update_interval(),
            max_count: default_authn_settings_max_count(),
            max_memory: default_authn_settings_max_memory(),
        }
    }
}

impl AuthnSettingsNodeCacheConf {
    /// Validates the entry cap and every duration/byte-size string.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_count == 0 || self.max_count > MAX_AUTHN_NODE_CACHE_MAX {
            return Err(ConfigError::Invalid(format!(
                "authn_settings.node_cache.max_count holds {}, must be 1..={} (field `max_count`)",
                self.max_count, MAX_AUTHN_NODE_CACHE_MAX
            )));
        }
        validate_settings_duration("authn_settings.node_cache.cache_ttl", &self.cache_ttl)?;
        validate_settings_duration(
            "authn_settings.node_cache.cleanup_interval",
            &self.cleanup_interval,
        )?;
        validate_settings_duration(
            "authn_settings.node_cache.stat_update_interval",
            &self.stat_update_interval,
        )?;
        validate_settings_bytesize("authn_settings.node_cache.max_memory", &self.max_memory)?;
        Ok(())
    }
}

/// Global authentication settings root: the backend-failure flag, the
/// node-cache half and the built-in record-count refresh interval.
/// Unknown fields are rejected (fail closed); missing fields load as
/// their documented defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthnSettingsConf {
    /// Whether a backend outage is ignored (default false, fail closed).
    #[serde(default)]
    pub ignore_backend_failures: bool,
    /// Node-cache half (defaults stated on the struct).
    #[serde(default)]
    pub node_cache: AuthnSettingsNodeCacheConf,
    /// Built-in record-count refresh interval (default "1h").
    #[serde(default = "default_authn_settings_refresh_interval")]
    pub builtin_record_count_refresh_interval: String,
}

impl Default for AuthnSettingsConf {
    fn default() -> Self {
        Self {
            ignore_backend_failures: false,
            node_cache: AuthnSettingsNodeCacheConf::default(),
            builtin_record_count_refresh_interval: default_authn_settings_refresh_interval(),
        }
    }
}

impl AuthnSettingsConf {
    /// Validates every field; the first bad field names itself.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.node_cache.validate()?;
        validate_settings_duration(
            "authn_settings.builtin_record_count_refresh_interval",
            &self.builtin_record_count_refresh_interval,
        )?;
        Ok(())
    }
}

/// Default authorization node-cache entry cap (50 000 entries,
/// LRU-evicted). Reason: the node cache holds one small verdict per
/// recently authorized (action, topic) pair, so it must stay bounded;
/// 50 000 entries cover a busy node's working set with headroom while
/// keeping status snapshots and resets cheap (one short lock, at most
/// the capped small entries).
pub const DEFAULT_AUTHZ_NODE_CACHE_MAX: usize = 50_000;
/// Upper bound for the authorization node-cache cap (1 000 000 entries).
/// Reason: caps above this would let one node hold unbounded
/// authorization state; larger values are rejected instead of growing
/// without limit.
pub const MAX_AUTHZ_NODE_CACHE_MAX: usize = 1_000_000;

/// Default authorization node-cache entry TTL ("60s"). Reason: cached
/// verdicts must expire quickly so an ACL change takes effect; one
/// minute bounds staleness while keeping repeated authorizations cheap
/// (mirrors the 60 s database verdict TTL).
fn default_authz_node_cache_ttl() -> String {
    "60s".to_string()
}

/// Default authorization node-cache cleanup interval ("60s"). Reason:
/// expiry sweeps run on the same cadence as the TTL so expired entries
/// do not linger past one TTL window.
fn default_authz_node_cache_cleanup_interval() -> String {
    "60s".to_string()
}

/// Default authorization node-cache statistics update interval ("5s").
/// Reason: cache counters feed management status reads, so a few
/// seconds keep the status fresh without waking the sweeper per request.
fn default_authz_node_cache_stat_update_interval() -> String {
    "5s".to_string()
}

/// Default authorization node-cache entry cap (50 000 entries). Reason:
/// stated on [`DEFAULT_AUTHZ_NODE_CACHE_MAX`].
fn default_authz_node_cache_max_count() -> usize {
    DEFAULT_AUTHZ_NODE_CACHE_MAX
}

/// Default authorization node-cache memory cap ("100MB"). Reason:
/// entries are small verdict records, so 100MB bounds the cache far
/// above the entry-cap worst case while letting an operator raise it
/// explicitly with `unlimited`.
fn default_authz_node_cache_max_memory() -> String {
    "100MB".to_string()
}

/// Node-level authorization decision-cache configuration. Only this
/// validated struct persists via the registry; cached verdicts
/// themselves are memory-only and never survive a restart.
// The config rejects an `unlimited` entry cap.
// A PUT replaces missing fields with the defaults below.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthzNodeCacheConf {
    /// Whether authorization verdicts are cached on this node
    /// (default true).
    #[serde(default = "default_authn_node_cache_enabled")]
    pub enable: bool,
    /// Cached verdict TTL (default "60s").
    #[serde(default = "default_authz_node_cache_ttl")]
    pub cache_ttl: String,
    /// Expired-entry sweep interval (default "60s").
    #[serde(default = "default_authz_node_cache_cleanup_interval")]
    pub cleanup_interval: String,
    /// Statistics refresh interval (default "5s").
    #[serde(default = "default_authz_node_cache_stat_update_interval")]
    pub stat_update_interval: String,
    /// Entry cap (default 50 000, at most 1 000 000).
    #[serde(default = "default_authz_node_cache_max_count")]
    pub max_count: usize,
    /// Memory cap (default "100MB", or explicit `unlimited` opt-in).
    #[serde(default = "default_authz_node_cache_max_memory")]
    pub max_memory: String,
}

impl Default for AuthzNodeCacheConf {
    fn default() -> Self {
        Self {
            enable: true,
            cache_ttl: default_authz_node_cache_ttl(),
            cleanup_interval: default_authz_node_cache_cleanup_interval(),
            stat_update_interval: default_authz_node_cache_stat_update_interval(),
            max_count: default_authz_node_cache_max_count(),
            max_memory: default_authz_node_cache_max_memory(),
        }
    }
}

impl AuthzNodeCacheConf {
    /// Validates the entry cap and every duration/byte-size string.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_count == 0 || self.max_count > MAX_AUTHZ_NODE_CACHE_MAX {
            return Err(ConfigError::Invalid(format!(
                "authz_node_cache.max_count holds {}, must be 1..={} (field `max_count`)",
                self.max_count, MAX_AUTHZ_NODE_CACHE_MAX
            )));
        }
        validate_settings_duration("authz_node_cache.cache_ttl", &self.cache_ttl)?;
        validate_settings_duration("authz_node_cache.cleanup_interval", &self.cleanup_interval)?;
        validate_settings_duration(
            "authz_node_cache.stat_update_interval",
            &self.stat_update_interval,
        )?;
        validate_settings_bytesize("authz_node_cache.max_memory", &self.max_memory)?;
        Ok(())
    }
}

/// Default authorization decision when no rule matches (`deny`).
/// Reason: the documented default; unmatched publishes and subscribes
/// stay refused until an explicit rule allows them (fail closed).
fn default_authz_settings_no_match() -> String {
    "deny".to_string()
}

/// Default action on an explicit deny (`ignore`). Reason: the documented
/// default; a denied publish is dropped while the connection stays up, so
/// one bad topic cannot tear down a fleet device.
fn default_authz_settings_deny_action() -> String {
    "ignore".to_string()
}

/// Default decision-cache enable flag (true). Reason: the documented
/// default; caching repeated verdicts keeps re-authorization cheap while
/// the TTL below bounds staleness.
fn default_authz_settings_cache_enabled() -> bool {
    true
}

/// Default decision-cache entry cap (32 entries). Reason: the documented
/// default; the cache holds one small verdict per recently authorized
/// (action, topic) pattern, so 32 covers a device's working set while
/// keeping the settings snapshot cheap (one short swap per request).
pub const DEFAULT_AUTHZ_SETTINGS_CACHE_MAX_SIZE: usize = 32;
/// Upper bound for the decision-cache entry cap (1 048 576 entries).
/// Reason: the documented range cap; larger values are rejected instead
/// of growing without limit.
pub const MAX_AUTHZ_SETTINGS_CACHE_MAX_SIZE: usize = 1_048_576;

/// Default decision-cache entry TTL (`1m`). Reason: cached verdicts must
/// expire quickly so a rule change takes effect; one minute bounds
/// staleness while keeping repeated authorizations cheap.
fn default_authz_settings_cache_ttl() -> String {
    "1m".to_string()
}

/// Bound on decision-cache topic-pattern exclusions (1024 entries).
/// Reason: each entry is one topic-pattern exclusion held in the snapshot
/// cloned per management read; 1024 covers per-pattern opt-outs with
/// headroom while bounding snapshot growth. Default is the empty list
/// (every verdict cacheable).
pub const MAX_AUTHZ_SETTINGS_EXCLUDES: usize = 1024;

/// Default mountpoint inclusion (false). Reason: the documented default;
/// the broker carries no mountpoints, so topics render bare unless the
/// operator opts in.
fn default_authz_settings_include_mountpoint() -> bool {
    false
}

/// Decision-cache half of the global authorization settings: the enabled
/// flag, entry cap, TTL and topic-pattern exclusions. Stored, validated
/// and reported; enforced on the publish, subscribe and will paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthzSettingsCacheConf {
    /// Whether authorization verdicts are cached (default true).
    #[serde(default = "default_authz_settings_cache_enabled")]
    pub enable: bool,
    /// Entry cap (default 32, at most 1 048 576).
    #[serde(default = "default_authz_settings_cache_max_size")]
    pub max_size: usize,
    /// Cached verdict TTL (default `1m`).
    #[serde(default = "default_authz_settings_cache_ttl")]
    pub ttl: String,
    /// Topic patterns excluded from caching (default empty).
    #[serde(default)]
    pub excludes: Vec<String>,
}

fn default_authz_settings_cache_max_size() -> usize {
    DEFAULT_AUTHZ_SETTINGS_CACHE_MAX_SIZE
}

impl Default for AuthzSettingsCacheConf {
    fn default() -> Self {
        Self {
            enable: default_authz_settings_cache_enabled(),
            max_size: default_authz_settings_cache_max_size(),
            ttl: default_authz_settings_cache_ttl(),
            excludes: Vec::new(),
        }
    }
}

impl AuthzSettingsCacheConf {
    /// Validates the entry cap, the TTL and every exclusion pattern.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_size == 0 || self.max_size > MAX_AUTHZ_SETTINGS_CACHE_MAX_SIZE {
            return Err(ConfigError::Invalid(format!(
                "authz_settings.cache.max_size holds {}, must be 1..={} (field `max_size`)",
                self.max_size, MAX_AUTHZ_SETTINGS_CACHE_MAX_SIZE
            )));
        }
        validate_settings_duration("authz_settings.cache.ttl", &self.ttl)?;
        if self.excludes.len() > MAX_AUTHZ_SETTINGS_EXCLUDES {
            return Err(ConfigError::Invalid(format!(
                "authz_settings.cache.excludes holds {} entries, at most {} (field `excludes`)",
                self.excludes.len(),
                MAX_AUTHZ_SETTINGS_EXCLUDES
            )));
        }
        for (index, pattern) in self.excludes.iter().enumerate() {
            if pattern.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "authz_settings.cache.excludes[{index}] must not be empty (field `excludes`)"
                )));
            }
        }
        Ok(())
    }
}

/// Global authorization settings root: the no-match decision, the
/// deny action, the decision-cache half, the mountpoint flag and the
/// backend-failure flag. Unknown fields are rejected (fail closed);
/// missing fields load as their documented defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthzSettingsConf {
    /// Decision when no rule matches: `allow` or `deny` (default `deny`).
    #[serde(default = "default_authz_settings_no_match")]
    pub no_match: String,
    /// Action on an explicit deny: `ignore` or `disconnect` (default
    /// `ignore`).
    #[serde(default = "default_authz_settings_deny_action")]
    pub deny_action: String,
    /// Decision-cache half (defaults stated on the struct).
    #[serde(default)]
    pub cache: AuthzSettingsCacheConf,
    /// Whether topics render with their mountpoint prefix (default false).
    #[serde(default = "default_authz_settings_include_mountpoint")]
    pub include_mountpoint: bool,
    /// Whether a backend outage is ignored (default false, fail closed).
    #[serde(default)]
    pub ignore_backend_failures: bool,
}

impl Default for AuthzSettingsConf {
    fn default() -> Self {
        Self {
            no_match: default_authz_settings_no_match(),
            deny_action: default_authz_settings_deny_action(),
            cache: AuthzSettingsCacheConf::default(),
            include_mountpoint: default_authz_settings_include_mountpoint(),
            ignore_backend_failures: false,
        }
    }
}

impl AuthzSettingsConf {
    /// Validates every field; the first bad field names itself.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.no_match != "allow" && self.no_match != "deny" {
            return Err(ConfigError::Invalid(format!(
                "authz_settings.no_match holds {:?}, must be `allow` or `deny` (field `no_match`)",
                self.no_match
            )));
        }
        if self.deny_action != "ignore" && self.deny_action != "disconnect" {
            return Err(ConfigError::Invalid(format!(
                "authz_settings.deny_action holds {:?}, must be `ignore` or `disconnect` (field `deny_action`)",
                self.deny_action
            )));
        }
        self.cache.validate()?;
        Ok(())
    }
}

/// Durations are `<n>ms|s|m|h|d` (integer or float count, no spaces).
fn validate_settings_duration(field: &str, text: &str) -> Result<(), ConfigError> {
    if parse_settings_duration_to_ms(text).is_none() {
        return Err(ConfigError::Invalid(format!(
            "{field} holds {text:?}, must be a duration like `1m`, `5s` or `1h` (field `{}`)",
            field.rsplit('.').next().unwrap_or(field)
        )));
    }
    Ok(())
}

/// Byte sizes are `<n>[B|KB|MB|GB]` (case-insensitive, optional space), a
/// plain positive integer string, or explicit `unlimited` opt-in.
fn validate_settings_bytesize(field: &str, text: &str) -> Result<(), ConfigError> {
    let lower = text.trim().to_ascii_lowercase();
    if lower == "unlimited" {
        return Ok(());
    }
    match parse_settings_bytesize_to_bytes(text) {
        Some(v) if v > 0 => Ok(()),
        _ => Err(ConfigError::Invalid(format!(
            "{field} holds {text:?}, must be a byte size like `100MB` or `unlimited` (field `{}`)",
            field.rsplit('.').next().unwrap_or(field)
        ))),
    }
}

/// Parse a settings duration (`1ms`, `5s`, `1m`, `1h`, `1d`) to millis.
/// Returns `None` for an invalid duration. Used by the authorize path
/// to enforce the cached verdict TTL without a new dependency.
pub fn parse_settings_duration_to_ms(text: &str) -> Option<u64> {
    let lower = text.trim().to_ascii_lowercase().replace(' ', "");
    if lower.is_empty() {
        return None;
    }
    let (num_part, mult): (&str, f64) = if lower.ends_with("ms") {
        (&lower[..lower.len() - 2], 1.0)
    } else if lower.ends_with('s') {
        (&lower[..lower.len() - 1], 1000.0)
    } else if lower.ends_with('m') {
        (&lower[..lower.len() - 1], 60_000.0)
    } else if lower.ends_with('h') {
        (&lower[..lower.len() - 1], 3_600_000.0)
    } else if lower.ends_with('d') {
        (&lower[..lower.len() - 1], 86_400_000.0)
    } else {
        return None;
    };
    if num_part.is_empty() {
        return None;
    }
    let num: f64 = num_part.parse().ok()?;
    if num < 0.0 || !num.is_finite() {
        return None;
    }
    Some((num * mult) as u64)
}

fn parse_settings_bytesize_to_bytes(text: &str) -> Option<u64> {
    let lower = text.trim().to_ascii_lowercase().replace(' ', "");
    if lower.is_empty() {
        return None;
    }
    if let Ok(plain) = lower.parse::<u64>() {
        return Some(plain);
    }
    let (num_part, mult) = lower
        .strip_suffix("kb")
        .map(|stripped| (stripped, 1024u64))
        .or_else(|| {
            lower
                .strip_suffix("mb")
                .map(|stripped| (stripped, 1024u64 * 1024))
        })
        .or_else(|| {
            lower
                .strip_suffix("gb")
                .map(|stripped| (stripped, 1024u64 * 1024 * 1024))
        })
        .or_else(|| lower.strip_suffix('b').map(|stripped| (stripped, 1u64)))?;
    let num: f64 = num_part.parse().ok()?;
    if num < 0.0 {
        return None;
    }
    Some((num * mult as f64) as u64)
}

/// Root: ordered authenticator chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuthnChainConf {
    #[serde(default)]
    pub authenticators: Vec<AuthnEntry>,
}

impl AuthnChainConf {
    /// Validates ids (non-empty, unique), mechanisms and backends
    /// (known), the chain length cap, and the id/mechanism consistency
    /// for colon-form ids.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.authenticators.len() > MAX_AUTHN_CHAIN_ENTRIES {
            return Err(ConfigError::Invalid(format!(
                "authn_chain.authenticators holds {} entries, at most {} (field `authenticators`)",
                self.authenticators.len(),
                MAX_AUTHN_CHAIN_ENTRIES
            )));
        }
        let mut seen = HashSet::with_capacity(self.authenticators.len());
        for (index, entry) in self.authenticators.iter().enumerate() {
            if entry.id.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "authn_chain.authenticators[{index}].id must not be empty (field `id`)"
                )));
            }
            if entry.mechanism.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "authn_chain.authenticators[{index}].mechanism must not be empty (field `mechanism`)"
                )));
            }
            if !is_known_authn_mechanism(&entry.mechanism) {
                return Err(ConfigError::Invalid(format!(
                    "authn_chain.authenticators[{index}].mechanism {:?} is unknown (field `mechanism`)",
                    entry.mechanism
                )));
            }
            if entry.backend.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "authn_chain.authenticators[{index}].backend must not be empty (field `backend`)"
                )));
            }
            if !is_known_authn_backend(&entry.backend) {
                return Err(ConfigError::Invalid(format!(
                    "authn_chain.authenticators[{index}].backend {:?} is unknown (field `backend`)",
                    entry.backend
                )));
            }
            if let Some((head, _)) = entry.id.split_once(':') {
                if head != entry.mechanism {
                    return Err(ConfigError::Invalid(format!(
                        "authn_chain.authenticators[{index}].id {:?} must start with its mechanism {:?} (field `id`)",
                        entry.id, entry.mechanism
                    )));
                }
            }
            if !seen.insert(entry.id.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "authn_chain.authenticators[{index}].id {:?} is duplicated (field `id`)",
                    entry.id
                )));
            }
        }
        Ok(())
    }
}

/// Upper bound for stored authorization sources. Writes past this size
/// are rejected instead of growing the list without limit. Reason: the
/// management plane snapshots the whole registry per request and the
/// decision path scans at most the capped entries, so the registry must
/// stay small and bounded; 8 covers the documented backend families with
/// headroom while keeping per-request clones cheap.
// TODO(parity): is 8 the documented source cap, or does the spec allow
// more entries? The rulebook does not decide the exact bound; 8 is the
// conservative choice until the conformance checker pins it.
pub const MAX_AUTHZ_SOURCES: usize = 8;

/// Authorization source types the kernel can store. Only
/// `built_in_database` executes in the parity waves; every other type's
/// config is stored and reported, never claimed as live. The set covers
/// the password, JWT, LDAP, HTTP, Redis, SQL, Mongo and file families
/// named for the authorization registries.
// TODO(parity): which source types does the spec document (file versus
// mongo versus mongodb spellings, jwt versus libs)? The rulebook does not
// decide the exact set; the current choice mirrors the authenticator
// backend families until the checker pins it.
pub const KNOWN_AUTHZ_SOURCE_TYPES: &[&str] = &[
    "built_in_database",
    "mysql",
    "postgresql",
    "mongodb",
    "redis",
    "http",
    "jwt",
    "scram",
    "ldap",
    "file",
];

/// Returns true for an authorization source type the kernel can store.
#[must_use]
pub fn is_known_authz_source_type(source_type: &str) -> bool {
    KNOWN_AUTHZ_SOURCE_TYPES.contains(&source_type)
}

/// One authorization source: the registry slot the management API lists
/// and creates. `source_type` is the registry key (for example
/// `built_in_database`); `enable` selects whether the slot participates;
/// `config` carries the backend-specific fields as a JSON document (may
/// be empty for the built-in database). Only `built_in_database`
/// executes in the parity waves; every other type's config is stored and
/// reported, never claimed as live.
// TODO(parity): which per-source fields does the spec require beyond
// type/enable (status, order, ...)? The rulebook does not decide the
// exact shape; fields the broker cannot supply are omitted rather than
// invented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AuthzSourceEntry {
    /// Registry key; must name a known source type (field `type`) and be
    /// unique within the root.
    #[serde(rename = "type")]
    pub source_type: String,
    /// Whether the slot participates (missing entries from
    /// pre-persistence files load as `true`, the historical behaviour).
    #[serde(default = "default_authz_source_enabled")]
    pub enable: bool,
    /// Opaque backend configuration as a JSON document; may be empty.
    #[serde(default)]
    pub config: String,
}

/// Serde default for a missing source enable flag: historical entries
/// predate the flag and were always enabled.
fn default_authz_source_enabled() -> bool {
    true
}

/// Root: ordered authorization source registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzSourcesConf {
    #[serde(default = "default_authz_sources")]
    pub sources: Vec<AuthzSourceEntry>,
}

/// Default registry: only the built-in database source. Reason: the
/// built-in database is the one backend that executes, so a fresh broker
/// authorizes against its local rule table without operator setup; every
/// other backend is opt-in stored configuration, never silently live.
fn default_authz_sources() -> Vec<AuthzSourceEntry> {
    vec![AuthzSourceEntry {
        source_type: "built_in_database".to_string(),
        enable: true,
        config: String::new(),
    }]
}

impl Default for AuthzSourcesConf {
    fn default() -> Self {
        Self {
            sources: default_authz_sources(),
        }
    }
}

impl AuthzSourcesConf {
    /// Validates source types (non-empty, known, unique) and the registry
    /// length cap.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.sources.len() > MAX_AUTHZ_SOURCES {
            return Err(ConfigError::Invalid(format!(
                "authz_sources.sources holds {} entries, at most {} (field `sources`)",
                self.sources.len(),
                MAX_AUTHZ_SOURCES
            )));
        }
        let mut seen = HashSet::with_capacity(self.sources.len());
        for (index, entry) in self.sources.iter().enumerate() {
            if entry.source_type.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "authz_sources.sources[{index}].type must not be empty (field `type`)"
                )));
            }
            if !is_known_authz_source_type(&entry.source_type) {
                return Err(ConfigError::Invalid(format!(
                    "authz_sources.sources[{index}].type {:?} is unknown (field `type`)",
                    entry.source_type
                )));
            }
            if !seen.insert(entry.source_type.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "authz_sources.sources[{index}].type {:?} is duplicated (field `type`)",
                    entry.source_type
                )));
            }
        }
        Ok(())
    }
}

/// Upper bound for stored management API keys. Creates past this size
/// are rejected instead of growing the map without limit. Reason: the
/// management plane snapshots the whole key list per list request and
/// each entry holds a hash plus metadata, so an unbounded map lets one
/// management batch exhaust the kernel; 1024 covers operator and
/// automation provisioning with headroom while keeping list reads paged
/// and per-request clones cheap.
// TODO(parity): is 1024 the documented key cap, or does the spec allow
// more entries? The rulebook does not decide the exact bound; 1024 is
// the conservative choice until the conformance checker pins it.
pub const MAX_API_KEYS: usize = 1024;

/// Longest accepted API key name (256 bytes, the limit of the reference
/// API). Reason: names are map keys cloned into every list snapshot and
/// persisted per entry, so an unbounded name lets one record bloat each
/// list read; 256 is the documented limit and keeps per-request clones
/// cheap.
pub const MAX_API_KEY_NAME_LEN: usize = 256;

/// Longest accepted per-scope token in an API key record (64 bytes).
/// Reason: scopes are cloned into every list snapshot and persisted per
/// entry, so an unbounded token lets one record bloat each list read;
/// 64 covers dotted scope names with headroom while keeping per-request
/// clones cheap.
pub const MAX_API_KEY_SCOPE_LEN: usize = 64;

/// Most scopes accepted on one API key record (32 entries). Reason: the
/// scope list is cloned per list row and scanned per management-plane
/// key check, so an unbounded list lets one record bloat reads and
/// checks; 32 covers multi-scope automation with headroom while keeping
/// rows small.
pub const MAX_API_KEY_SCOPES: usize = 32;

/// One stored management API key: the key name plus the secret hash.
///
/// `key_hash` holds the hex SHA-256 of the minted secret (never
/// plaintext); the registry treats it as an opaque non-empty string.
/// `key_id` is the public identifier of the key: the user name in Basic
/// authentication. An empty `key_id` (a record from an older release)
/// means that the name is the identifier.
/// `scopes` carries the granted scope set verbatim; it is the explicit
/// list only when `scopes_set` is true or the list is not empty. A record
/// with `scopes_set` false and an empty list has no scope policy: the
/// role decides. `description` is free-form operator text. `created_at`
/// renders the mint time in the same UTC `+00:00` shape the ban list
/// uses, or an empty string when the record predates the field.
/// `enable` false makes the broker refuse the key at authentication.
/// `expired_at` is the expiry time in epoch seconds; `None` never
/// expires. `role` is `administrator`, `viewer` or `publisher`; an empty
/// text means `administrator`.
///
/// Each field that an older release did not write has a serde default, so
/// an old file loads with the behaviour that the key had before.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiKeyEntry {
    /// Key name; must be non-empty and unique within the root.
    pub name: String,
    /// Hex SHA-256 of the minted secret; must be non-empty.
    pub key_hash: String,
    /// Public key identifier (the Basic user name); empty means the name.
    #[serde(default)]
    pub key_id: String,
    /// Granted scope set (may be empty).
    #[serde(default)]
    pub scopes: Vec<String>,
    /// True when `scopes` is an explicit list, even an empty one (an empty
    /// explicit list denies each scope).
    #[serde(default)]
    pub scopes_set: bool,
    /// Free-form description shown by the key list.
    #[serde(default)]
    pub description: String,
    /// Mint time (`YYYY-MM-DDTHH:MM:SS+00:00`) or empty when unknown.
    #[serde(default)]
    pub created_at: String,
    /// False when the key is disabled.
    #[serde(default = "default_api_key_enable")]
    pub enable: bool,
    /// Expiry time in epoch seconds; `None` never expires.
    #[serde(default)]
    pub expired_at: Option<u64>,
    /// `administrator`, `viewer` or `publisher`; empty means
    /// `administrator`.
    #[serde(default = "default_api_key_role")]
    pub role: String,
}

fn default_api_key_enable() -> bool {
    true
}

fn default_api_key_role() -> String {
    "administrator".to_string()
}

impl Default for ApiKeyEntry {
    fn default() -> Self {
        Self {
            name: String::new(),
            key_hash: String::new(),
            key_id: String::new(),
            scopes: Vec::new(),
            scopes_set: false,
            description: String::new(),
            created_at: String::new(),
            enable: default_api_key_enable(),
            expired_at: None,
            role: default_api_key_role(),
        }
    }
}

/// Root: management API key records.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ApiKeysConf {
    #[serde(default)]
    pub keys: Vec<ApiKeyEntry>,
}

impl ApiKeysConf {
    /// Validates names (non-empty, bounded, unique), hashes
    /// (non-empty), the scope caps and the root length cap.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.keys.len() > MAX_API_KEYS {
            return Err(ConfigError::Invalid(format!(
                "api_keys.keys holds {} entries, at most {} (field `keys`)",
                self.keys.len(),
                MAX_API_KEYS
            )));
        }
        let mut seen = HashSet::with_capacity(self.keys.len());
        for (index, entry) in self.keys.iter().enumerate() {
            if entry.name.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "api_keys.keys[{index}].name must not be empty (field `name`)"
                )));
            }
            if entry.name.len() > MAX_API_KEY_NAME_LEN {
                return Err(ConfigError::Invalid(format!(
                    "api_keys.keys[{index}].name {:?} is too long (field `name`)",
                    entry.name
                )));
            }
            if entry.key_hash.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "api_keys.keys[{index}].key_hash must not be empty (field `key_hash`)"
                )));
            }
            if entry.scopes.len() > MAX_API_KEY_SCOPES {
                return Err(ConfigError::Invalid(format!(
                    "api_keys.keys[{index}].scopes holds {} entries, at most {} (field `scopes`)",
                    entry.scopes.len(),
                    MAX_API_KEY_SCOPES
                )));
            }
            for scope in &entry.scopes {
                if scope.trim().is_empty() || scope.len() > MAX_API_KEY_SCOPE_LEN {
                    return Err(ConfigError::Invalid(format!(
                        "api_keys.keys[{index}].scopes holds an invalid scope (field `scopes`)"
                    )));
                }
            }
            if entry.key_id.len() > MAX_API_KEY_NAME_LEN {
                return Err(ConfigError::Invalid(format!(
                    "api_keys.keys[{index}].key_id is too long (field `key_id`)"
                )));
            }
            if !matches!(
                entry.role.as_str(),
                "" | "administrator" | "viewer" | "publisher"
            ) {
                return Err(ConfigError::Invalid(format!(
                    "api_keys.keys[{index}].role {:?} is not known (field `role`)",
                    entry.role
                )));
            }
            if !seen.insert(entry.name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "api_keys.keys[{index}].name {:?} is duplicated (field `name`)",
                    entry.name
                )));
            }
        }
        Ok(())
    }
}

/// Upper bound for configured single-sign-on backends. Writes past this
/// size are rejected instead of growing the list without limit. Reason:
/// the management plane snapshots the whole registry per list request
/// and each entry holds an opaque config document, so an unbounded list
/// lets one management batch exhaust the kernel; 16 covers the
/// dashboard backend families with headroom while keeping list reads
/// cheap (one short lock, at most the capped small entries).
// TODO(parity): is 16 the documented backend cap, or does the spec allow
// more entries? The rulebook does not decide the exact bound; 16 is the
// conservative choice until the conformance checker pins it.
pub const MAX_SSO_BACKENDS: usize = 16;

/// Longest accepted single-sign-on backend name (128 bytes). Reason:
/// names are map keys cloned into every list snapshot and persisted per
/// entry, so an unbounded name lets one record bloat each list read; 128
/// covers backend naming with headroom while keeping per-request clones
/// cheap.
pub const MAX_SSO_BACKEND_NAME_LEN: usize = 128;

/// One configured single-sign-on backend: the registry slot the
/// management API lists. `backend` is the registry key (for example
/// `oidc` or `saml`); `enable` selects whether the slot participates;
/// `config` carries the backend-specific fields as a JSON document (may
/// be empty until the per-backend detail task fills it). Only the
/// registry is real in this task; identity-provider clients are
/// out of scope and no endpoint claims a backend it cannot execute.
// TODO(parity): which per-backend fields does the spec document beyond
// backend/enable (display name, URLs, client ids, ...)? The rulebook does
// not decide the exact shape; fields the broker cannot supply are omitted
// rather than invented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SsoBackendEntry {
    /// Registry key; must be non-empty and unique within the root.
    pub backend: String,
    /// Whether the slot participates (missing entries from
    /// pre-persistence files load as `true`, the historical behaviour).
    #[serde(default = "default_sso_backend_enabled")]
    pub enable: bool,
    /// Opaque backend configuration as a JSON document; may be empty.
    #[serde(default)]
    pub config: String,
}

/// Serde default for a missing backend enable flag: historical entries
/// predate the flag and were always enabled.
fn default_sso_backend_enabled() -> bool {
    true
}

/// Root: configured single-sign-on backends.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SsoBackendsConf {
    #[serde(default)]
    pub backends: Vec<SsoBackendEntry>,
}

impl SsoBackendsConf {
    /// Validates backend names (non-empty, bounded, unique) and the
    /// registry length cap.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.backends.len() > MAX_SSO_BACKENDS {
            return Err(ConfigError::Invalid(format!(
                "sso_backends.backends holds {} entries, at most {} (field `backends`)",
                self.backends.len(),
                MAX_SSO_BACKENDS
            )));
        }
        let mut seen = HashSet::with_capacity(self.backends.len());
        for (index, entry) in self.backends.iter().enumerate() {
            if entry.backend.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "sso_backends.backends[{index}].backend must not be empty (field `backend`)"
                )));
            }
            if entry.backend.len() > MAX_SSO_BACKEND_NAME_LEN {
                return Err(ConfigError::Invalid(format!(
                    "sso_backends.backends[{index}].backend {:?} is too long (field `backend`)",
                    entry.backend
                )));
            }
            if entry.backend.contains('/') {
                return Err(ConfigError::Invalid(format!(
                    "sso_backends.backends[{index}].backend {:?} must not contain `/` (field `backend`)",
                    entry.backend
                )));
            }
            if !seen.insert(entry.backend.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "sso_backends.backends[{index}].backend {:?} is duplicated (field `backend`)",
                    entry.backend
                )));
            }
        }
        Ok(())
    }
}

/// Upper bound for stored rule-engine actions. Writes past this size
/// are rejected instead of growing the list without limit. Reason: the
/// management plane snapshots the whole registry per list request and
/// each entry holds an opaque config document, so an unbounded list
/// lets one management batch exhaust the kernel; 1024 covers the
/// documented action catalogue with headroom while keeping list reads
/// cheap (one short lock, at most the capped small entries).
// TODO(parity): is 1024 the documented action cap, or does the spec allow
// more entries? The rulebook does not decide the exact bound; 1024 is the
// conservative choice until the conformance checker pins it.
pub const MAX_ACTIONS: usize = 1024;

/// Longest accepted action id (256 bytes). Reason: ids are map keys
/// cloned into every list snapshot and persisted per entry, so an
/// unbounded id lets one record bloat each list read; 256 covers
/// operator naming with headroom while keeping per-request clones cheap.
pub const MAX_ACTION_ID_LEN: usize = 256;

/// One stored rule-engine action: the operator-chosen id, the action
/// kind it was validated against, whether it participates, and the
/// kind-specific fields as a JSON document (may be empty). Only the
/// registry is real in this task; per-action execution counters and
/// lifecycle operations land with their own tasks and never persist.
// TODO(parity): which per-action fields does the spec document beyond
// id/type/enable (connector reference, failure handling, ...)? The
// rulebook does not decide the exact shape; fields the broker cannot
// supply are omitted rather than invented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ActionEntry {
    /// Action id; must be non-empty and unique within the root.
    pub id: String,
    /// Action kind; must name a known action type (field `action_type`).
    pub action_type: String,
    /// Whether the action participates (missing entries from
    /// pre-persistence files load as `true`, the historical behaviour).
    #[serde(default = "default_action_enabled")]
    pub enable: bool,
    /// Opaque kind-specific configuration as a JSON document; may be empty.
    #[serde(default)]
    pub config: String,
}

/// Serde default for a missing action enable flag: historical entries
/// predate the flag and were always enabled.
fn default_action_enabled() -> bool {
    true
}

/// Root: stored rule-engine actions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ActionsConf {
    #[serde(default)]
    pub actions: Vec<ActionEntry>,
}

impl ActionsConf {
    /// Validates action ids (non-empty, bounded, unique), kinds (known)
    /// and the registry length cap.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.actions.len() > MAX_ACTIONS {
            return Err(ConfigError::Invalid(format!(
                "actions.actions holds {} entries, at most {} (field `actions`)",
                self.actions.len(),
                MAX_ACTIONS
            )));
        }
        let mut seen = HashSet::with_capacity(self.actions.len());
        for (index, entry) in self.actions.iter().enumerate() {
            if entry.id.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "actions.actions[{index}].id must not be empty (field `id`)"
                )));
            }
            if entry.id.len() > MAX_ACTION_ID_LEN {
                return Err(ConfigError::Invalid(format!(
                    "actions.actions[{index}].id {:?} is too long (field `id`)",
                    entry.id
                )));
            }
            if entry.action_type.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "actions.actions[{index}].action_type must not be empty (field `action_type`)"
                )));
            }
            if !is_known_action_type(&entry.action_type) {
                return Err(ConfigError::Invalid(format!(
                    "actions.actions[{index}].action_type {:?} is unknown (field `action_type`)",
                    entry.action_type
                )));
            }
            if !seen.insert(entry.id.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "actions.actions[{index}].id {:?} is duplicated (field `id`)",
                    entry.id
                )));
            }
        }
        Ok(())
    }
}

/// Upper bound for stored ingress sources. Writes past this size are
/// rejected instead of growing the list without limit. Reason: the
/// management plane snapshots the whole directory per list request, so
/// an unbounded list lets one management batch exhaust the kernel; 512
/// covers the documented ingress catalogue with headroom while keeping
/// list reads cheap (one short lock, at most the capped small entries).
// TODO(parity): is 512 the documented source cap, or does the spec allow
// more entries? The rulebook does not decide the exact bound; 512 is the
// conservative choice until the conformance checker pins it.
pub const MAX_SOURCES: usize = 512;

/// Longest accepted source id (256 bytes). Reason: ids are map keys
/// cloned into every list snapshot and persisted per entry, so an
/// unbounded id lets one record bloat each list read; 256 covers
/// operator naming with headroom while keeping per-request clones cheap.
pub const MAX_SOURCE_ID_LEN: usize = 256;

/// Serde default for a missing source enable flag: historical entries
/// predate the flag and were always enabled.
fn default_source_enabled() -> bool {
    true
}

/// One stored ingress source: the operator-chosen id, the source kind
/// it was validated against, and whether it participates. Only the
/// registry is real in this task; ingress execution consults a snapshot
/// off the message path and lands with the enable/lifecycle tasks.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SourceEntry {
    /// Source id; must be non-empty and unique within the root.
    pub id: String,
    /// Source kind; must name a known source type (field `source_type`).
    pub source_type: String,
    /// Whether the source participates (missing entries from
    /// pre-persistence files load as `true`, the historical behaviour).
    #[serde(default = "default_source_enabled")]
    pub enable: bool,
    /// Opaque kind-specific configuration as a JSON document; may be empty.
    #[serde(default)]
    pub config: String,
}

/// Root: stored ingress sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SourcesConf {
    #[serde(default)]
    pub sources: Vec<SourceEntry>,
}

impl SourcesConf {
    /// Validates source ids (non-empty, bounded, unique), kinds (known)
    /// and the registry length cap.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.sources.len() > MAX_SOURCES {
            return Err(ConfigError::Invalid(format!(
                "sources.sources holds {} entries, at most {} (field `sources`)",
                self.sources.len(),
                MAX_SOURCES
            )));
        }
        let mut seen = HashSet::with_capacity(self.sources.len());
        for (index, entry) in self.sources.iter().enumerate() {
            if entry.id.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "sources.sources[{index}].id must not be empty (field `id`)"
                )));
            }
            if entry.id.len() > MAX_SOURCE_ID_LEN {
                return Err(ConfigError::Invalid(format!(
                    "sources.sources[{index}].id {:?} is too long (field `id`)",
                    entry.id
                )));
            }
            if entry.source_type.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "sources.sources[{index}].source_type must not be empty (field `source_type`)"
                )));
            }
            if !is_known_source_type(&entry.source_type) {
                return Err(ConfigError::Invalid(format!(
                    "sources.sources[{index}].source_type {:?} is unknown (field `source_type`)",
                    entry.source_type
                )));
            }
            if !seen.insert(entry.id.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "sources.sources[{index}].id {:?} is duplicated (field `id`)",
                    entry.id
                )));
            }
        }
        Ok(())
    }
}

/// Upper bound for stored message transformations. Writes past this
/// size are rejected instead of growing the list without limit. Reason:
/// the management plane snapshots the whole chain per list request and
/// the publish path clones one snapshot per ingress, so the chain must
/// stay small and bounded; 64 covers the documented chain with headroom
/// while keeping per-request clones cheap.
// TODO(parity): is 64 the documented chain cap? The rulebook does not
// decide the exact bound; 64 is the conservative choice until the
// conformance checker pins it.
pub const MAX_TRANSFORMATIONS: usize = 64;

/// Longest accepted transformation name (256 bytes). Reason: names are
/// map keys cloned into every list snapshot and persisted per entry, so
/// an unbounded name lets one record bloat each list read; 256 covers
/// operator naming with headroom while keeping per-request clones cheap.
pub const MAX_TRANSFORMATION_NAME_LEN: usize = 256;

/// Most topic filters accepted in one transformation (16). Reason: the
/// publish path tests each filter per ingress, so the list stays small
/// and bounded; 16 covers operator routing with headroom while keeping
/// per-ingress matching cheap.
pub const MAX_TRANSFORMATION_TOPICS: usize = 16;

/// Longest accepted transformation topic filter (256 bytes). Reason:
/// filters clone into every list snapshot and persist per entry, so an
/// unbounded filter lets one record bloat each list read; 256 covers
/// topic filters with headroom while keeping snapshots small.
pub const MAX_TRANSFORMATION_TOPIC_LEN: usize = 256;

/// Most operations accepted in one transformation (16). Reason: the
/// publish path renders each operation per matching ingress, so the
/// list stays small and bounded; 16 covers chained rewrites with
/// headroom while keeping per-ingress work cheap.
pub const MAX_TRANSFORMATION_OPERATIONS: usize = 16;

/// Longest accepted operation template (1024 bytes). Reason: templates
/// render per matching ingress and clone into every snapshot, so an
/// unbounded template lets one record bloat reads and the fire path;
/// 1024 covers rewrite templates with headroom while keeping rows small.
pub const MAX_TRANSFORMATION_VALUE_LEN: usize = 1024;

/// Longest accepted opaque transformation config document (4096 bytes).
/// Reason: the document clones into every list snapshot and persists
/// per entry, so an unbounded document lets one record bloat each
/// read; 4096 covers decoder settings with headroom while keeping
/// snapshots small.
pub const MAX_TRANSFORMATION_CONFIG_LEN: usize = 4096;

/// Serde default for a missing transformation enable flag: historical
/// entries predate the flag and were always enabled.
fn default_transformation_enabled() -> bool {
    true
}

/// One operation inside a stored transformation: set `key` to `value`.
/// `key` names the message field (`payload` or `topic`); `value` is a
/// template where `${payload}` expands to the current payload text and
/// `${topic}` expands to the current topic name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransformationOperation {
    /// Message field to set; must be non-empty (field `key`).
    pub key: String,
    /// Template for the new value (field `value`).
    pub value: String,
}

/// One stored message transformation: the operator-chosen name, the
/// topic filters it applies to, the failure behaviour, whether it
/// participates, the ordered operations, and the remaining fields as an
/// opaque JSON document (may be empty).
// TODO(parity): which per-entry fields does the documented shape require
// beyond name/topics/failure_action/enable/operations (decoders,
// encoders, description, tags, log settings)? The rulebook does not
// decide the exact shape; fields the broker cannot execute are stored
// opaquely and reported back rather than invented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransformationEntry {
    /// Transformation name; must be non-empty and unique within the root.
    pub name: String,
    /// Topic filters the transformation applies to; at least one entry.
    #[serde(default)]
    pub topics: Vec<String>,
    /// Failure behaviour: `drop`, `disconnect` or `ignore`.
    #[serde(default = "default_transformation_failure_action")]
    pub failure_action: String,
    /// Whether the entry participates (missing entries from
    /// pre-persistence files load as `true`, the historical behaviour).
    #[serde(default = "default_transformation_enabled")]
    pub enable: bool,
    /// Ordered operations applied in order on matching messages.
    #[serde(default)]
    pub operations: Vec<TransformationOperation>,
    /// Opaque remaining configuration as a JSON document; may be empty.
    #[serde(default)]
    pub config: String,
}

/// Serde default for a missing failure behaviour: drop the message
/// rather than deliver it untransformed (fail closed).
fn default_transformation_failure_action() -> String {
    "drop".to_string()
}

/// Returns true for a transformation failure behaviour the kernel stores.
#[must_use]
pub fn is_known_transformation_failure_action(action: &str) -> bool {
    matches!(action, "drop" | "disconnect" | "ignore")
}

/// Root: stored message transformations in chain order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TransformationsConf {
    #[serde(default)]
    pub transformations: Vec<TransformationEntry>,
}

impl TransformationsConf {
    /// Validates names (non-empty, bounded, unique), topics (at least one
    /// valid filter each), failure behaviours (known), operations (known
    /// keys, string values) and the chain length cap.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.transformations.len() > MAX_TRANSFORMATIONS {
            return Err(ConfigError::Invalid(format!(
                "transformations.transformations holds {} entries, at most {} (field `transformations`)",
                self.transformations.len(),
                MAX_TRANSFORMATIONS
            )));
        }
        let mut seen = HashSet::with_capacity(self.transformations.len());
        for (index, entry) in self.transformations.iter().enumerate() {
            if entry.name.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].name must not be empty (field `name`)"
                )));
            }
            if entry.name.len() > MAX_TRANSFORMATION_NAME_LEN {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].name {:?} is too long (field `name`)",
                    entry.name
                )));
            }
            if entry.topics.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].topics must not be empty (field `topics`)"
                )));
            }
            if entry.topics.len() > MAX_TRANSFORMATION_TOPICS {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].topics holds {} entries, at most {} (field `topics`)",
                    entry.topics.len(),
                    MAX_TRANSFORMATION_TOPICS
                )));
            }
            for topic in &entry.topics {
                if topic.trim().is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "transformations.transformations[{index}].topics entry must not be empty (field `topics`)"
                    )));
                }
                if topic.len() > MAX_TRANSFORMATION_TOPIC_LEN {
                    return Err(ConfigError::Invalid(format!(
                        "transformations.transformations[{index}].topics entry holds {} bytes, at most {} (field `topics`)",
                        topic.len(),
                        MAX_TRANSFORMATION_TOPIC_LEN
                    )));
                }
            }
            if !is_known_transformation_failure_action(&entry.failure_action) {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].failure_action {:?} is unknown (field `failure_action`)",
                    entry.failure_action
                )));
            }
            if entry.operations.len() > MAX_TRANSFORMATION_OPERATIONS {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].operations holds {} entries, at most {} (field `operations`)",
                    entry.operations.len(),
                    MAX_TRANSFORMATION_OPERATIONS
                )));
            }
            for (op_index, op) in entry.operations.iter().enumerate() {
                if op.key.trim().is_empty() {
                    return Err(ConfigError::Invalid(format!(
                        "transformations.transformations[{index}].operations[{op_index}].key must not be empty (field `key`)"
                    )));
                }
                if !matches!(op.key.as_str(), "payload" | "topic") {
                    return Err(ConfigError::Invalid(format!(
                        "transformations.transformations[{index}].operations[{op_index}].key {:?} is unknown (field `key`)",
                        op.key
                    )));
                }
                if op.value.len() > MAX_TRANSFORMATION_VALUE_LEN {
                    return Err(ConfigError::Invalid(format!(
                        "transformations.transformations[{index}].operations[{op_index}].value holds {} bytes, at most {} (field `value`)",
                        op.value.len(),
                        MAX_TRANSFORMATION_VALUE_LEN
                    )));
                }
            }
            if entry.config.len() > MAX_TRANSFORMATION_CONFIG_LEN {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].config holds {} bytes, at most {} (field `config`)",
                    entry.config.len(),
                    MAX_TRANSFORMATION_CONFIG_LEN
                )));
            }
            if !seen.insert(entry.name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "transformations.transformations[{index}].name {:?} is duplicated (field `name`)",
                    entry.name
                )));
            }
        }
        Ok(())
    }
}

/// Most schemas kept in the registry (256). Reason: the list clones the full
/// set per read, so the set stays small; 256 covers operator use with room.
pub const MAX_SCHEMAS: usize = 256;

/// Longest accepted schema name (256 bytes). Reason: names are map keys and
/// clone per read, so one long name cannot bloat reads.
pub const MAX_SCHEMA_NAME_LEN: usize = 256;

/// Most bytes accepted in one schema document (1 MiB). Reason: documents
/// clone per read and persist per entry, so one large document cannot bloat
/// reads or the state file; 1 MiB holds large schemas with room.
pub const MAX_SCHEMA_BYTES: usize = 1_048_576;

/// Longest accepted schema description (1024 bytes). Reason: descriptions
/// clone per read, so one long text cannot bloat reads.
pub const MAX_SCHEMA_DESC_LEN: usize = 1024;

/// Schema kinds the registry stores.
pub const KNOWN_SCHEMA_TYPES: &[&str] = &["json", "avro", "protobuf", "external_http"];

/// True for a schema kind the registry stores.
#[must_use]
pub fn is_known_schema_type(kind: &str) -> bool {
    KNOWN_SCHEMA_TYPES.contains(&kind)
}

/// True for a JSON Schema type name.
#[must_use]
pub fn is_known_json_schema_type(kind: &str) -> bool {
    matches!(
        kind,
        "object" | "array" | "string" | "number" | "integer" | "boolean" | "null"
    )
}

/// Check one Avro record document. The document must be an object with
/// type record, a non-empty name and a non-empty fields array. Each field
/// must carry a name and a type. Return the parsed value on success.
pub fn validate_avro_document(value: &serde_json::Value) -> Result<(), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "schema must be a JSON object".to_string())?;
    match obj.get("type").and_then(|v| v.as_str()) {
        Some("record") => {}
        _ => return Err("avro schema needs `\"type\": \"record\"`".to_string()),
    }
    match obj.get("name").and_then(|v| v.as_str()) {
        Some(name) if !name.trim().is_empty() => {}
        _ => return Err("avro schema needs a non-empty `name`".to_string()),
    }
    let fields = obj
        .get("fields")
        .and_then(|v| v.as_array())
        .ok_or_else(|| "avro schema needs a `fields` array".to_string())?;
    if fields.is_empty() {
        return Err("avro schema `fields` must not be empty".to_string());
    }
    for (index, field) in fields.iter().enumerate() {
        let entry = field
            .as_object()
            .ok_or_else(|| format!("avro `fields[{index}]` must be an object"))?;
        match entry.get("name").and_then(|v| v.as_str()) {
            Some(name) if !name.trim().is_empty() => {}
            _ => return Err(format!("avro `fields[{index}]` needs a `name`")),
        }
        if !entry.contains_key("type") {
            return Err(format!("avro `fields[{index}]` needs a `type`"));
        }
    }
    Ok(())
}

/// Check one JSON Schema document. The document must be a non-empty object.
/// It must carry at least one schema keyword. A present `type` must name a
/// known JSON type. A present `properties` must be an object.
pub fn validate_json_schema_document(value: &serde_json::Value) -> Result<(), String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "schema must be a JSON object".to_string())?;
    if obj.is_empty() {
        return Err("json schema must not be empty".to_string());
    }
    let has_keyword = [
        "type",
        "properties",
        "$schema",
        "allOf",
        "anyOf",
        "oneOf",
        "enum",
        "$ref",
        "items",
        "required",
    ]
    .iter()
    .any(|key| obj.contains_key(*key));
    if !has_keyword {
        return Err("json schema needs a type keyword".to_string());
    }
    if let Some(type_value) = obj.get("type") {
        if let Some(name) = type_value.as_str() {
            if !is_known_json_schema_type(name) {
                return Err(format!("json schema `type` {name:?} is unknown"));
            }
        } else if let Some(names) = type_value.as_array() {
            if names.is_empty() {
                return Err("json schema `type` must not be empty".to_string());
            }
            for name in names {
                match name.as_str() {
                    Some(text) if is_known_json_schema_type(text) => {}
                    _ => return Err("json schema `type` holds unknown entry".to_string()),
                }
            }
        } else {
            return Err("json schema `type` must be a string".to_string());
        }
    }
    if let Some(props) = obj.get("properties") {
        if !props.is_object() {
            return Err("json schema `properties` must be an object".to_string());
        }
    }
    Ok(())
}

/// Check one schema document for the named kind. Parse already happened;
/// this only checks the shape. Return an error string on failure.
pub fn validate_schema_document(kind: &str, value: &serde_json::Value) -> Result<(), String> {
    match kind {
        "avro" => validate_avro_document(value),
        "json" => validate_json_schema_document(value),
        // No compiler for this kind runs in the broker. The check keeps
        // only the non-empty rule. The broker stores the text.
        // TODO(parity): which shape checks does protobuf need here?
        "protobuf" => match value.as_str() {
            Some(text) if !text.trim().is_empty() => Ok(()),
            _ => Err("protobuf `source` must be non-empty text".to_string()),
        },
        // The broker stores the parameters. It never calls the server.
        // TODO(parity): which shape checks does external_http need here?
        "external_http" => match value.as_object() {
            Some(obj) if !obj.is_empty() => Ok(()),
            _ => Err("external_http `parameters` must be a non-empty object".to_string()),
        },
        _ => Err(format!("unknown schema type: {kind}")),
    }
}

/// One stored schema: the operator-chosen name, the kind it was checked
/// against, an optional description, and the schema document as JSON text.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemaEntry {
    /// Schema name; must be non-empty and unique within the root.
    pub name: String,
    /// Schema kind; must name a known kind (field `type`).
    pub schema_type: String,
    /// Optional description; at most [`MAX_SCHEMA_DESC_LEN`] bytes.
    #[serde(default)]
    pub description: String,
    /// Schema document as JSON text; at most [`MAX_SCHEMA_BYTES`] bytes.
    #[serde(default)]
    pub schema: String,
}

/// Root: stored schemas.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct SchemasConf {
    #[serde(default)]
    pub schemas: Vec<SchemaEntry>,
}

impl SchemasConf {
    /// Check names (non-empty, bounded, unique), kinds (known), sizes
    /// (bounded) and documents (parsed per kind).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.schemas.len() > MAX_SCHEMAS {
            return Err(ConfigError::Invalid(format!(
                "schemas.schemas holds {} entries, at most {} (field `schemas`)",
                self.schemas.len(),
                MAX_SCHEMAS
            )));
        }
        let mut seen = HashSet::with_capacity(self.schemas.len());
        for (index, entry) in self.schemas.iter().enumerate() {
            if entry.name.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].name must not be empty (field `name`)"
                )));
            }
            if entry.name.len() > MAX_SCHEMA_NAME_LEN {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].name {:?} is too long (field `name`)",
                    entry.name
                )));
            }
            if entry.schema_type.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].schema_type must not be empty (field `type`)"
                )));
            }
            if !is_known_schema_type(entry.schema_type.trim()) {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].schema_type {:?} is unknown (field `type`)",
                    entry.schema_type
                )));
            }
            if entry.description.len() > MAX_SCHEMA_DESC_LEN {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].description holds {} bytes, at most {} (field `description`)",
                    entry.description.len(),
                    MAX_SCHEMA_DESC_LEN
                )));
            }
            if entry.schema.trim().is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].schema must not be empty (field `schema`)"
                )));
            }
            if entry.schema.len() > MAX_SCHEMA_BYTES {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].schema holds {} bytes, at most {} (field `schema`)",
                    entry.schema.len(),
                    MAX_SCHEMA_BYTES
                )));
            }
            // Protobuf stores raw text. It is not JSON. The check runs
            // on the text itself.
            let kind = entry.schema_type.trim();
            if kind == "protobuf" {
                validate_schema_document(kind, &serde_json::Value::String(entry.schema.clone()))
                    .map_err(|reason| {
                        ConfigError::Invalid(format!(
                            "schemas.schemas[{index}].schema is invalid: {reason} (field `schema`)"
                        ))
                    })?;
            } else {
                let parsed: serde_json::Value =
                    serde_json::from_str(&entry.schema).map_err(|err| {
                        ConfigError::Invalid(format!(
                            "schemas.schemas[{index}].schema is not valid JSON: {err} (field `schema`)"
                        ))
                    })?;
                validate_schema_document(kind, &parsed).map_err(|reason| {
                    ConfigError::Invalid(format!(
                        "schemas.schemas[{index}].schema is invalid: {reason} (field `schema`)"
                    ))
                })?;
            }
            if !seen.insert(entry.name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "schemas.schemas[{index}].name {:?} is duplicated (field `name`)",
                    entry.name
                )));
            }
        }
        Ok(())
    }
}

/// Most entries accepted in one request-filter list inside the
/// rule-engine settings (allow/deny CIDRs, deny hosts). Reason: the lists
/// are cloned into every management snapshot and consulted per connector
/// send, so an unbounded list lets one write bloat reads and the send
/// path; 1024 covers operator allow/deny provisioning with headroom while
/// keeping snapshots small.
pub const MAX_SSRF_LIST_ENTRIES: usize = 1024;

/// Longest accepted single request-filter entry (one CIDR or hostname, 256
/// bytes). Reason: entries are cloned per snapshot and persisted per
/// record, so an unbounded entry lets one record bloat each read; 256
/// covers IPv6 CIDRs and long hostnames with headroom while keeping rows
/// small.
pub const MAX_SSRF_ENTRY_LEN: usize = 256;

/// Longest accepted jq-function timeout setting (`10s` shape, at most 32
/// bytes). Reason: the setting rides the per-request snapshot clone, so
/// one write cannot balloon memory; duration strings longer than this
/// cannot be valid timeouts and are rejected instead of growing the
/// snapshot.
pub const MAX_JQ_TIMEOUT_LEN: usize = 32;

/// Upper bound for the jq-function default timeout (1 hour in
/// milliseconds). Reason: jq runs per rule evaluation, so the timeout must
/// stay bounded; 1 hour is orders above the documented default with
/// headroom while refusing absurd values instead of honouring them.
// TODO(parity): is the documented timeout range capped, and does the
// documented PUT merge missing fields or reset them to defaults? The
// rulebook does not decide the range cap or the merge semantics; the
// current choice bounds the timeout conservatively and replaces missing
// fields with the defaults below (full replace) until the checker pins it.
pub const MAX_JQ_TIMEOUT_MS: u64 = 3_600_000;

/// Default request-filter allow list (empty: nothing explicitly allowed).
/// Reason: the documented default; filtering denies by default and the
/// operator opts ranges in explicitly (fail closed).
fn default_ssrf_allow_cidrs() -> Vec<String> {
    Vec::new()
}

/// Default request-filter deny CIDRs. Reason: the documented defaults;
/// loopback, link-local, private ranges, the unspecified and multicast
/// blocks and the well-known cloud metadata endpoints stay unreachable
/// from rule actions until the operator narrows the list explicitly.
fn default_ssrf_deny_cidrs() -> Vec<String> {
    [
        "127.0.0.0/8",
        "::1/128",
        "fe80::/10",
        "10.0.0.0/8",
        "172.16.0.0/12",
        "192.168.0.0/16",
        "fc00::/7",
        "0.0.0.0/32",
        "224.0.0.0/4",
        "ff00::/8",
        "100.100.100.200/32",
        "169.254.0.0/16",
    ]
    .iter()
    .map(|entry| entry.to_string())
    .collect()
}

/// Default request-filter deny hosts. Reason: the documented defaults;
/// the well-known cloud metadata hostnames stay unreachable from rule
/// actions until the operator narrows the list explicitly.
fn default_ssrf_deny_hosts() -> Vec<String> {
    [
        "metadata.tencentyun.com",
        "metadata.google.internal",
        "metadata.azure.internal",
    ]
    .iter()
    .map(|entry| entry.to_string())
    .collect()
}

/// Default jq-function default timeout (`10s`). Reason: the documented
/// default; jq evaluations that run past it fail the evaluation instead
/// of stalling the rule.
fn default_jq_function_default_timeout() -> String {
    "10s".to_string()
}

/// Default jq implementation backend. Reason: the documented default; the
/// native backend serves evaluations unless the operator opts into the
/// port backend explicitly.
fn default_jq_implementation_module() -> String {
    "jq_nif".to_string()
}

/// Request-filtering half of the rule-engine settings: whether outbound
/// rule-action requests are filtered, which CIDRs are allowed or denied
/// and which hostnames are denied. Stored, validated and reported;
/// enforcement on the connector send path is a sibling concern, never
/// this task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SsrfConf {
    /// Whether outbound request filtering applies (default false).
    #[serde(default)]
    pub enable: bool,
    /// Explicitly allowed CIDRs (default empty).
    #[serde(default = "default_ssrf_allow_cidrs")]
    pub allow_cidrs: Vec<String>,
    /// Denied CIDRs (defaults stated above).
    #[serde(default = "default_ssrf_deny_cidrs")]
    pub deny_cidrs: Vec<String>,
    /// Denied hostnames (defaults stated above).
    #[serde(default = "default_ssrf_deny_hosts")]
    pub deny_hosts: Vec<String>,
}

impl Default for SsrfConf {
    fn default() -> Self {
        Self {
            enable: false,
            allow_cidrs: default_ssrf_allow_cidrs(),
            deny_cidrs: default_ssrf_deny_cidrs(),
            deny_hosts: default_ssrf_deny_hosts(),
        }
    }
}

impl SsrfConf {
    /// Validates the list caps and every entry (CIDR shape for the CIDR
    /// lists, non-empty bounded hostnames for the host list).
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, list, is_cidr) in [
            ("ssrf.allow_cidrs", &self.allow_cidrs, true),
            ("ssrf.deny_cidrs", &self.deny_cidrs, true),
            ("ssrf.deny_hosts", &self.deny_hosts, false),
        ] {
            if list.len() > MAX_SSRF_LIST_ENTRIES {
                return Err(ConfigError::Invalid(format!(
                    "rule_engine.{field} holds {} entries, at most {} (field `{}`)",
                    list.len(),
                    MAX_SSRF_LIST_ENTRIES,
                    field.rsplit('.').next().unwrap_or(field)
                )));
            }
            for (index, entry) in list.iter().enumerate() {
                if entry.trim().is_empty() || entry.len() > MAX_SSRF_ENTRY_LEN {
                    return Err(ConfigError::Invalid(format!(
                        "rule_engine.{field}[{index}] must be 1..={} bytes (field `{}`)",
                        MAX_SSRF_ENTRY_LEN,
                        field.rsplit('.').next().unwrap_or(field)
                    )));
                }
                if is_cidr && !is_valid_cidr(entry) {
                    return Err(ConfigError::Invalid(format!(
                        "rule_engine.{field}[{index}] holds {entry:?}, must be a CIDR like `192.168.0.0/16` (field `{}`)",
                        field.rsplit('.').next().unwrap_or(field)
                    )));
                }
            }
        }
        Ok(())
    }
}

/// True for `address/prefix` strings with a parseable IP literal and an
/// in-range prefix length (32-bit for IPv4, 128-bit for IPv6).
fn is_valid_cidr(entry: &str) -> bool {
    let Some((addr, prefix)) = entry.trim().split_once('/') else {
        return false;
    };
    let addr: Result<std::net::IpAddr, _> = addr.trim().parse();
    let Ok(addr) = addr else {
        return false;
    };
    let Ok(bits): Result<u8, _> = prefix.trim().parse() else {
        return false;
    };
    match addr {
        std::net::IpAddr::V4(_) => bits <= 32,
        std::net::IpAddr::V6(_) => bits <= 128,
    }
}

/// Rule-engine settings root: the live engine knobs served by the
/// management plane. Fixed struct (stated fields and defaults); no
/// unbounded values: every list is capped and every duration is bounded.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleEngineConf {
    /// Whether system-message topics are skipped by rule matching
    /// (default true).
    #[serde(default = "default_rule_engine_ignore_sys_message")]
    pub ignore_sys_message: bool,
    /// Default jq-function timeout (default `10s`).
    #[serde(default = "default_jq_function_default_timeout")]
    pub jq_function_default_timeout: String,
    /// jq implementation backend: `jq_nif` or `jq_port` (default
    /// `jq_nif`).
    #[serde(default = "default_jq_implementation_module")]
    pub jq_implementation_module: String,
    /// Request-filtering half (defaults stated on the struct).
    #[serde(default)]
    pub ssrf: SsrfConf,
    /// Whether rule `SELECT` fields stay scoped to their namespace
    /// (default true).
    #[serde(default = "default_rule_engine_limit_selects_in_namespace")]
    pub limit_selects_in_namespace: bool,
}

/// Default system-message skip (true). Reason: the documented default;
/// system topics stay out of user rules unless the operator opts in.
fn default_rule_engine_ignore_sys_message() -> bool {
    true
}

/// Default namespace scoping for rule selects (true). Reason: the
/// documented default; rule projections stay inside their namespace
/// unless the operator opts out.
fn default_rule_engine_limit_selects_in_namespace() -> bool {
    true
}

impl Default for RuleEngineConf {
    fn default() -> Self {
        Self {
            ignore_sys_message: default_rule_engine_ignore_sys_message(),
            jq_function_default_timeout: default_jq_function_default_timeout(),
            jq_implementation_module: default_jq_implementation_module(),
            ssrf: SsrfConf::default(),
            limit_selects_in_namespace: default_rule_engine_limit_selects_in_namespace(),
        }
    }
}

impl RuleEngineConf {
    /// Validates every field; the first bad field names itself.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.jq_function_default_timeout.len() > MAX_JQ_TIMEOUT_LEN {
            return Err(ConfigError::Invalid(format!(
                "rule_engine.jq_function_default_timeout holds {:?}, must be a duration like `10s` (field `jq_function_default_timeout`)",
                self.jq_function_default_timeout
            )));
        }
        match parse_settings_duration_to_ms(&self.jq_function_default_timeout) {
            Some(ms) if (1..=MAX_JQ_TIMEOUT_MS).contains(&ms) => {}
            _ => {
                return Err(ConfigError::Invalid(format!(
                    "rule_engine.jq_function_default_timeout holds {:?}, must be a duration like `10s` (field `jq_function_default_timeout`)",
                    self.jq_function_default_timeout
                )));
            }
        }
        if self.jq_implementation_module != "jq_nif" && self.jq_implementation_module != "jq_port" {
            return Err(ConfigError::Invalid(format!(
                "rule_engine.jq_implementation_module holds {:?}, must be `jq_nif` or `jq_port` (field `jq_implementation_module`)",
                self.jq_implementation_module
            )));
        }
        self.ssrf.validate()?;
        Ok(())
    }

    /// Default timeout in milliseconds (the validated
    /// `jq_function_default_timeout` parsed; falls back to 10 000 ms only
    /// when validation was bypassed, never on the validated path).
    #[must_use]
    pub fn jq_timeout_ms(&self) -> u64 {
        parse_settings_duration_to_ms(&self.jq_function_default_timeout).unwrap_or(10_000)
    }
}

/// Longest stored cluster description (1024 chars). Reason: the console
/// renders it in the cluster page header, and handlers clone it once per
/// request; 1024 chars cover operator notes with headroom while keeping
/// each read cheap.
pub const MAX_CLUSTER_DESCRIPTION_LEN: usize = 1024;

/// Cluster display root: operator text for the cluster page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ClusterMetaConf {
    /// Free-form cluster description shown by `GET /cluster`.
    #[serde(default)]
    pub description: String,
}

impl ClusterMetaConf {
    /// Validates the description length.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.description.len() > MAX_CLUSTER_DESCRIPTION_LEN {
            return Err(ConfigError::Invalid(format!(
                "cluster_meta.description holds {} bytes, at most {} (field `description`)",
                self.description.len(),
                MAX_CLUSTER_DESCRIPTION_LEN
            )));
        }
        Ok(())
    }
}

/// Default QoS 1 window applied to new sessions (100 unacked downlinks).
/// Reason: matches the session default, so a fresh registry reads the
/// same value the broker enforces.
fn default_global_zone_max_inflight() -> u32 {
    100
}

/// Default offline queue cap applied to new sessions (10 000 messages).
/// Reason: matches the session default, so a fresh registry reads the
/// same value the broker enforces.
fn default_global_zone_max_mqueue_len() -> Option<u32> {
    Some(10_000)
}

/// Default inbound topic-alias maximum (10 aliases). Reason: matches the
/// session default, so a fresh registry reads the same value the broker
/// advertises in CONNACK.
fn default_global_zone_max_topic_alias() -> u16 {
    10
}

/// Upper bound for the offline queue cap (10 000 000 messages). Reason:
/// one detached session past this holds gigabytes of small frames, so
/// larger values are rejected instead of growing without limit. Explicit
/// `None` (infinity) stays an operator opt-in.
pub const MAX_GLOBAL_ZONE_MQUEUE_LEN: u32 = 10_000_000;

/// Write one offline cap. `None` writes as `infinity`.
fn serialize_mqueue_len<S>(value: &Option<u32>, ser: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match value {
        Some(limit) => ser.serialize_u32(*limit),
        None => ser.serialize_str("infinity"),
    }
}

/// Read one offline cap. Accepts an integer or `infinity`.
fn deserialize_mqueue_len<'de, D>(de: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Error, Unexpected, Visitor};
    struct Cap;
    impl<'de> Visitor<'de> for Cap {
        type Value = Option<u32>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an integer or `infinity`")
        }
        fn visit_none<E: Error>(self) -> Result<Option<u32>, E> {
            Ok(None)
        }
        fn visit_unit<E: Error>(self) -> Result<Option<u32>, E> {
            Ok(None)
        }
        fn visit_u64<E: Error>(self, value: u64) -> Result<Option<u32>, E> {
            u32::try_from(value)
                .map(Some)
                .map_err(|_| Error::invalid_value(Unexpected::Unsigned(value), &self))
        }
        fn visit_i64<E: Error>(self, value: i64) -> Result<Option<u32>, E> {
            u32::try_from(value)
                .map(Some)
                .map_err(|_| Error::invalid_value(Unexpected::Signed(value), &self))
        }
        fn visit_str<E: Error>(self, value: &str) -> Result<Option<u32>, E> {
            if value == "infinity" {
                return Ok(None);
            }
            Err(Error::invalid_value(Unexpected::Str(value), &self))
        }
        fn visit_string<E: Error>(self, value: String) -> Result<Option<u32>, E> {
            self.visit_str(&value)
        }
    }
    de.deserialize_any(Cap)
}

/// Default-zone MQTT knobs the broker enforces. Each field maps to a
/// session limit applied at session creation; every other documented
/// `global_zone` field has no broker setting and is omitted by the API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalZoneConf {
    /// QoS 1 window for new sessions (1..=65535).
    #[serde(default = "default_global_zone_max_inflight")]
    pub max_inflight: u32,
    /// Offline queue cap for new sessions (`None` = infinity).
    #[serde(
        default = "default_global_zone_max_mqueue_len",
        serialize_with = "serialize_mqueue_len",
        deserialize_with = "deserialize_mqueue_len"
    )]
    pub max_mqueue_len: Option<u32>,
    /// Inbound topic-alias maximum for new sessions (0..=65535).
    #[serde(default = "default_global_zone_max_topic_alias")]
    pub max_topic_alias: u16,
}

impl Default for GlobalZoneConf {
    fn default() -> Self {
        Self {
            max_inflight: default_global_zone_max_inflight(),
            max_mqueue_len: default_global_zone_max_mqueue_len(),
            max_topic_alias: default_global_zone_max_topic_alias(),
        }
    }
}

impl GlobalZoneConf {
    /// Validates every bound; the first bad field names itself.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.max_inflight < 1 || self.max_inflight > 65_535 {
            return Err(ConfigError::Invalid(format!(
                "global_zone.max_inflight holds {}, must be 1..=65535 (field `max_inflight`)",
                self.max_inflight
            )));
        }
        if let Some(len) = self.max_mqueue_len {
            if !(1..=MAX_GLOBAL_ZONE_MQUEUE_LEN).contains(&len) {
                return Err(ConfigError::Invalid(format!(
                    "global_zone.max_mqueue_len holds {len}, must be 1..={} or infinity (field `max_mqueue_len`)",
                    MAX_GLOBAL_ZONE_MQUEUE_LEN
                )));
            }
        }
        Ok(())
    }
}

/// One exporter collector switch. Only the documented values exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PrometheusCollector {
    /// Collector is off. This is the default for every collector.
    #[default]
    Disabled,
    /// Collector is on.
    Enabled,
}

/// Internal exporter collectors. Every switch defaults to off: the
/// switches add CPU load and are only enabled for load analysis.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PrometheusCollectorsConf {
    /// Store collector switch.
    #[serde(default)]
    pub mnesia: PrometheusCollector,
    /// Distribution collector switch.
    #[serde(default)]
    pub vm_dist: PrometheusCollector,
    /// Memory collector switch.
    #[serde(default)]
    pub vm_memory: PrometheusCollector,
    /// Microstate accounting collector switch.
    #[serde(default)]
    pub vm_msacc: PrometheusCollector,
    /// Statistics collector switch.
    #[serde(default)]
    pub vm_statistics: PrometheusCollector,
    /// System information collector switch.
    #[serde(default)]
    pub vm_system_info: PrometheusCollector,
}

/// Default push interval. Reason: the documented default is 15 seconds.
fn default_prometheus_push_interval() -> String {
    "15s".to_string()
}

/// Default push job name. Reason: the documented default names the node
/// and host through the node and host variables.
fn default_prometheus_push_job() -> String {
    "${name}/instance/${name}~${host}".to_string()
}

/// Default push method. Reason: the documented default replaces the
/// grouped metrics on each push.
fn default_prometheus_push_method() -> String {
    "put".to_string()
}

/// Default push target. Reason: the documented default is the local
/// push target on its default port.
fn default_prometheus_push_url() -> String {
    "http://127.0.0.1:9091".to_string()
}

/// Push target for the text exposition. Disabled by default: no push
/// target is configured unless the operator sets one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrometheusPushGatewayConf {
    /// Push switch. Off by default.
    #[serde(default)]
    pub enable: bool,
    /// Headers sent with each push. Empty by default.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Push interval text (for example `15s`).
    #[serde(default = "default_prometheus_push_interval")]
    pub interval: String,
    /// Job name template for each push.
    #[serde(default = "default_prometheus_push_job")]
    pub job_name: String,
    /// Push method: `put` or `post`.
    #[serde(default = "default_prometheus_push_method")]
    pub method: String,
    /// Push target URL.
    #[serde(default = "default_prometheus_push_url")]
    pub url: String,
}

/// Defaults match the documented push target.
impl Default for PrometheusPushGatewayConf {
    fn default() -> Self {
        Self {
            enable: false,
            headers: BTreeMap::new(),
            interval: default_prometheus_push_interval(),
            job_name: default_prometheus_push_job(),
            method: default_prometheus_push_method(),
            url: default_prometheus_push_url(),
        }
    }
}

/// Rate limit for the namespaced exposition. Only the rate text is
/// stored; enforcement lands with namespaced metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PrometheusRateLimiterConf {
    /// Rate text (for example `1000/s`). Absent means no limit set.
    #[serde(default)]
    pub rate: Option<String>,
}

/// Default scrape authentication flag. Reason: the documented default
/// requires credentials on the scrape endpoints.
fn default_prometheus_basic_auth() -> bool {
    true
}

/// Default latency histogram buckets. Reason: the documented default
/// covers 10 ms to 30 s.
fn default_prometheus_latency_buckets() -> String {
    "10ms, 100ms, 1s, 5s, 30s".to_string()
}

/// Longest accepted free-form exporter text (512 bytes). Reason: the
/// fields hold URLs, job templates and bucket lists; longer values are
/// almost certainly a typo, and the write fails closed.
pub const MAX_PROMETHEUS_TEXT_LEN: usize = 512;

/// Most headers accepted on the push target (16). Reason: a push needs
/// at most an authorization header plus a few extras; more entries only
/// grow the stored root.
pub const MAX_PROMETHEUS_HEADERS: usize = 16;

/// Exporter settings behind `GET`/`PUT /prometheus`. The broker only
/// stores the settings and serves the text exposition from live
/// counters; it runs no push client and no collector beyond the
/// counters the kernel already keeps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PrometheusConf {
    /// Internal collector switches. All off by default.
    #[serde(default)]
    pub collectors: PrometheusCollectorsConf,
    /// Require credentials on the scrape endpoints. On by default.
    #[serde(default = "default_prometheus_basic_auth")]
    pub enable_basic_auth: bool,
    /// Latency histogram bucket list text.
    #[serde(default = "default_prometheus_latency_buckets")]
    pub latency_buckets: String,
    /// Push target. Disabled by default.
    #[serde(default)]
    pub push_gateway: PrometheusPushGatewayConf,
    /// Namespaced exposition rate limit. Absent by default.
    #[serde(default)]
    pub namespaced_metrics_limiter: Option<PrometheusRateLimiterConf>,
}

impl Default for PrometheusConf {
    fn default() -> Self {
        Self {
            collectors: PrometheusCollectorsConf::default(),
            enable_basic_auth: default_prometheus_basic_auth(),
            latency_buckets: default_prometheus_latency_buckets(),
            push_gateway: PrometheusPushGatewayConf::default(),
            namespaced_metrics_limiter: None,
        }
    }
}

impl PrometheusConf {
    /// Validates the method, the text lengths and the header count; the
    /// first bad field names itself.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.latency_buckets.len() > MAX_PROMETHEUS_TEXT_LEN {
            return Err(ConfigError::Invalid(format!(
                "prometheus.latency_buckets holds {} bytes, at most {} (field `latency_buckets`)",
                self.latency_buckets.len(),
                MAX_PROMETHEUS_TEXT_LEN
            )));
        }
        let gateway = &self.push_gateway;
        if gateway.method != "put" && gateway.method != "post" {
            return Err(ConfigError::Invalid(format!(
                "prometheus.push_gateway.method holds {:?}, must be `put` or `post` (field `method`)",
                gateway.method
            )));
        }
        for (field, value) in [
            ("interval", gateway.interval.as_str()),
            ("job_name", gateway.job_name.as_str()),
            ("url", gateway.url.as_str()),
        ] {
            if value.len() > MAX_PROMETHEUS_TEXT_LEN {
                return Err(ConfigError::Invalid(format!(
                    "prometheus.push_gateway.{field} holds {} bytes, at most {} (field `{field}`)",
                    value.len(),
                    MAX_PROMETHEUS_TEXT_LEN
                )));
            }
        }
        if gateway.headers.len() > MAX_PROMETHEUS_HEADERS {
            return Err(ConfigError::Invalid(format!(
                "prometheus.push_gateway.headers holds {} entries, at most {} (field `headers`)",
                gateway.headers.len(),
                MAX_PROMETHEUS_HEADERS
            )));
        }
        for (key, value) in &gateway.headers {
            if key.len() > MAX_PROMETHEUS_TEXT_LEN || value.len() > MAX_PROMETHEUS_TEXT_LEN {
                return Err(ConfigError::Invalid(format!(
                    "prometheus.push_gateway.headers holds an entry past {MAX_PROMETHEUS_TEXT_LEN} bytes (field `headers`)"
                )));
            }
        }
        if let Some(limiter) = &self.namespaced_metrics_limiter {
            if let Some(rate) = &limiter.rate {
                if rate.len() > MAX_PROMETHEUS_TEXT_LEN {
                    return Err(ConfigError::Invalid(format!(
                        "prometheus.namespaced_metrics_limiter.rate holds {} bytes, at most {} (field `rate`)",
                        rate.len(),
                        MAX_PROMETHEUS_TEXT_LEN
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Upper bound for managed tenant namespaces. Writes past this size are
/// rejected. Reason: one entry holds short strings,
/// so 1024 entries bound the root. Larger tables are rejected.
pub const MAX_NAMESPACES: usize = 1024;

/// Longest namespace name. Reason: the name keys session keys
/// and log lines. 128 bounds key memory and matches the session
/// tenant bound. Longer names are rejected.
pub const MAX_NAMESPACE_NAME_LEN: usize = 128;

/// Longest stored namespace configuration text in bytes.
/// Reason: the configuration holds limit numbers as JSON.
/// 16 KiB bounds the root. Larger values are rejected.
pub const MAX_NAMESPACE_CONFIG_LEN: usize = 16 * 1024;

/// One managed tenant namespace record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NamespaceEntry {
    /// Namespace name (the tenant id).
    #[serde(default)]
    pub name: String,
    /// Creation time as unix seconds.
    #[serde(default)]
    pub created_at: u64,
    /// The namespace configuration as JSON text (an object).
    #[serde(default)]
    pub config_json: String,
}

/// Root: managed tenant namespaces and their stored configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct NamespacesConf {
    #[serde(default)]
    pub namespaces: Vec<NamespaceEntry>,
}

impl NamespacesConf {
    /// Validates the entry count, the names (non-empty, bounded, no
    /// control characters, unique) and the configuration text (a JSON
    /// object of bounded size).
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.namespaces.len() > MAX_NAMESPACES {
            return Err(ConfigError::Invalid(format!(
                "namespaces.namespaces holds {} entries, at most {} (field `namespaces`)",
                self.namespaces.len(),
                MAX_NAMESPACES
            )));
        }
        let mut seen = HashSet::with_capacity(self.namespaces.len());
        for (index, entry) in self.namespaces.iter().enumerate() {
            if entry.name.trim().is_empty()
                || entry.name.len() > MAX_NAMESPACE_NAME_LEN
                || entry.name.chars().any(|c| c.is_control())
            {
                return Err(ConfigError::Invalid(format!(
                    "namespaces.namespaces[{index}].name is not valid (field `name`)"
                )));
            }
            if entry.config_json.len() > MAX_NAMESPACE_CONFIG_LEN {
                return Err(ConfigError::Invalid(format!(
                    "namespaces.namespaces[{index}].config_json holds {} bytes, at most {} (field `config_json`)",
                    entry.config_json.len(),
                    MAX_NAMESPACE_CONFIG_LEN
                )));
            }
            if !entry.config_json.is_empty()
                && !serde_json::from_str::<serde_json::Value>(&entry.config_json)
                    .is_ok_and(|value| value.is_object())
            {
                return Err(ConfigError::Invalid(format!(
                    "namespaces.namespaces[{index}].config_json is not a JSON object (field `config_json`)"
                )));
            }
            if !seen.insert(entry.name.clone()) {
                return Err(ConfigError::Invalid(format!(
                    "namespaces.namespaces[{index}].name {:?} is duplicated (field `name`)",
                    entry.name
                )));
            }
        }
        Ok(())
    }
}

/// The full validated configuration: all roots in one snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct FullSnapshot {
    #[serde(default)]
    pub admin_users: AdminUsersConf,
    #[serde(default)]
    pub mqtt_users: MqttUsersConf,
    #[serde(default)]
    pub rules: RulesConf,
    #[serde(default)]
    pub connectors: ConnectorsConf,
    #[serde(default)]
    pub authn_chain: AuthnChainConf,
    #[serde(default)]
    pub authn_node_cache: AuthnNodeCacheConf,
    #[serde(default)]
    pub authn_settings: AuthnSettingsConf,
    #[serde(default)]
    pub authz_node_cache: AuthzNodeCacheConf,
    #[serde(default)]
    pub authz_settings: AuthzSettingsConf,
    #[serde(default)]
    pub authz_sources: AuthzSourcesConf,
    #[serde(default)]
    pub api_keys: ApiKeysConf,
    #[serde(default)]
    pub sso_backends: SsoBackendsConf,
    #[serde(default)]
    pub actions: ActionsConf,
    #[serde(default)]
    pub sources: SourcesConf,
    #[serde(default)]
    pub rule_engine: RuleEngineConf,
    #[serde(default)]
    pub transformations: TransformationsConf,
    #[serde(default)]
    pub schemas: SchemasConf,
    /// Cluster display text (AP-03).
    #[serde(default)]
    pub cluster_meta: ClusterMetaConf,
    /// Default-zone MQTT knobs (AP-03).
    #[serde(default)]
    pub global_zone: GlobalZoneConf,
    /// Exporter settings (AP-09).
    #[serde(default)]
    pub prometheus: PrometheusConf,
    /// Managed tenant namespaces (TM-02).
    #[serde(default)]
    pub namespaces: NamespacesConf,
}

impl FullSnapshot {
    /// Validates every root. Empty defaults are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.admin_users.validate()?;
        self.mqtt_users.validate()?;
        self.rules.validate()?;
        self.connectors.validate()?;
        self.authn_chain.validate()?;
        self.authn_node_cache.validate()?;
        self.authn_settings.validate()?;
        self.authz_node_cache.validate()?;
        self.authz_settings.validate()?;
        self.authz_sources.validate()?;
        self.api_keys.validate()?;
        self.sso_backends.validate()?;
        self.actions.validate()?;
        self.sources.validate()?;
        self.rule_engine.validate()?;
        self.transformations.validate()?;
        self.schemas.validate()?;
        self.cluster_meta.validate()?;
        self.global_zone.validate()?;
        self.prometheus.validate()?;
        self.namespaces.validate()?;
        Ok(())
    }

    /// Validates every root plus cross-root references (M1-05 whole-config
    /// validation). Reports every offending setting in one error.
    pub fn validate_whole(&self) -> Result<(), ConfigError> {
        runtime::validate_whole(self)
    }
}

/// One validated root ready to be committed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigRoot {
    /// Validated admin-users root.
    AdminUsers(AdminUsersConf),
    /// Validated MQTT-users root.
    MqttUsers(MqttUsersConf),
    /// Validated rules root.
    Rules(RulesConf),
    /// Validated connectors root.
    Connectors(ConnectorsConf),
    /// Validated authenticator-chain root.
    AuthnChain(AuthnChainConf),
    /// Validated node authentication cache config root.
    AuthnNodeCache(AuthnNodeCacheConf),
    /// Validated global authentication settings root.
    AuthnSettings(AuthnSettingsConf),
    /// Validated node authorization cache config root.
    AuthzNodeCache(AuthzNodeCacheConf),
    /// Validated global authorization settings root.
    AuthzSettings(AuthzSettingsConf),
    /// Validated authorization source registry root.
    AuthzSources(AuthzSourcesConf),
    /// Validated management API key records root.
    ApiKeys(ApiKeysConf),
    /// Validated single-sign-on backend registry root.
    SsoBackends(SsoBackendsConf),
    /// Validated rule-engine action registry root.
    Actions(ActionsConf),
    /// Validated ingress source registry root.
    Sources(SourcesConf),
    /// Validated rule-engine settings root.
    RuleEngine(RuleEngineConf),
    /// Validated message transformation chain root.
    Transformations(TransformationsConf),
    /// Validated schema registry root.
    Schemas(SchemasConf),
    /// Validated cluster display root.
    ClusterMeta(ClusterMetaConf),
    /// Validated default-zone MQTT root.
    GlobalZone(GlobalZoneConf),
    /// Validated exporter settings root.
    Prometheus(PrometheusConf),
    /// Validated managed tenant namespaces root.
    Namespaces(NamespacesConf),
}

static SAVE_TMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn state_path(dir: &Path) -> PathBuf {
    dir.join(STATE_FILE_NAME)
}

/// Kernel-owned configuration registry.
///
/// Readers call [`ConfigRegistry::snapshot`], which loads the current
/// [`FullSnapshot`] through `ArcSwap` without taking any lock.
/// Writers call [`ConfigRegistry::commit`] (or one of the per-root
/// helpers), which validates the incoming root before swapping it into
/// view; invalid state can never become visible. [`ConfigRegistry::save`]
/// persists the current snapshot atomically.
///
/// Runtime changes (M1-05) persist into the kernel data directory
/// (`state.toml` plus `config-history.json` beside it). The broker never
/// writes into the operator config directory (`indra.toml`, `conf.d/`);
/// reloads read those files but only the data directory is ever written.
#[derive(Debug)]
pub struct ConfigRegistry {
    dir: PathBuf,
    current: ArcSwap<FullSnapshot>,
    history: ConfigHistory,
    /// True when the data directory existed at load. Boot creates the
    /// directory before load, so production always sees true and saves
    /// fail closed when the directory goes missing later. Tests that
    /// load a fresh scratch path skip boot creation; the first save
    /// completes it and then sets this flag.
    dir_present_at_load: AtomicBool,
}

impl ConfigRegistry {
    /// Loads the registry from `<dir>/state.toml`.
    ///
    /// A missing file yields validated defaults (all roots empty). A
    /// corrupt file or a validation failure is an error naming the file
    /// and the field; defaults are never silently substituted. The version
    /// history loads from `<dir>/config-history.json` when present and is
    /// seeded with the boot snapshot otherwise.
    pub fn load(dir: &Path) -> Result<Self, ConfigError> {
        let file = state_path(dir);
        let snapshot = if !file.exists() {
            let defaults = FullSnapshot::default();
            defaults.validate().map_err(|err| ConfigError::Load {
                file: file.display().to_string(),
                reason: err.to_string(),
            })?;
            defaults
        } else {
            let text = std::fs::read_to_string(&file).map_err(|err| ConfigError::Load {
                file: file.display().to_string(),
                reason: format!("cannot read file: {err}"),
            })?;
            let parsed: FullSnapshot = toml::from_str(&text).map_err(|err| ConfigError::Load {
                file: file.display().to_string(),
                reason: format!("invalid TOML: {err}"),
            })?;
            parsed.validate().map_err(|err| ConfigError::Load {
                file: file.display().to_string(),
                reason: err.to_string(),
            })?;
            parsed
        };
        let dir_present = dir.is_dir();
        let registry = Self {
            dir: dir.to_path_buf(),
            current: ArcSwap::from(Arc::new(snapshot.clone())),
            history: ConfigHistory::new(),
            dir_present_at_load: AtomicBool::new(dir_present),
        };
        registry.load_history_file();
        if registry.history.is_empty() {
            registry.seed_boot_version();
        }
        Ok(registry)
    }

    /// Returns the directory this registry persists into.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Makes the data directory ready for a save. Boot creates the
    /// directory before load, so a missing directory then is a failure
    /// (fail closed, deny the write and report it). A fresh scratch
    /// path that missed boot creation gets one creation here; later
    /// deletions still fail because the flag is set after creation.
    fn ensure_data_dir(&self, file: &Path) -> Result<(), ConfigError> {
        if self.dir.is_dir() {
            return Ok(());
        }
        if self.dir_present_at_load.load(Ordering::Relaxed) {
            return Err(ConfigError::Save {
                file: file.display().to_string(),
                reason: format!("data directory is missing: {}", self.dir.display()),
            });
        }
        std::fs::create_dir_all(&self.dir).map_err(|err| ConfigError::Save {
            file: file.display().to_string(),
            reason: format!("cannot create data directory: {err}"),
        })?;
        self.dir_present_at_load.store(true, Ordering::Relaxed);
        Ok(())
    }

    /// Takes a lock-free snapshot of the current configuration.
    #[must_use]
    pub fn snapshot(&self) -> Arc<FullSnapshot> {
        self.current.load_full()
    }

    /// Commits one validated root, swapping it into view atomically.
    ///
    /// The root is validated before the swap; on failure the visible
    /// snapshot is unchanged.
    pub fn commit(&self, root: ConfigRoot) -> Result<(), ConfigError> {
        match root {
            ConfigRoot::AdminUsers(conf) => self.commit_admin_users(conf),
            ConfigRoot::MqttUsers(conf) => self.commit_mqtt_users(conf),
            ConfigRoot::Rules(conf) => self.commit_rules(conf),
            ConfigRoot::Connectors(conf) => self.commit_connectors(conf),
            ConfigRoot::AuthnChain(conf) => self.commit_authn_chain(conf),
            ConfigRoot::AuthnNodeCache(conf) => self.commit_authn_node_cache(conf),
            ConfigRoot::AuthnSettings(conf) => self.commit_authn_settings(conf),
            ConfigRoot::AuthzNodeCache(conf) => self.commit_authz_node_cache(conf),
            ConfigRoot::AuthzSettings(conf) => self.commit_authz_settings(conf),
            ConfigRoot::AuthzSources(conf) => self.commit_authz_sources(conf),
            ConfigRoot::ApiKeys(conf) => self.commit_api_keys(conf),
            ConfigRoot::SsoBackends(conf) => self.commit_sso_backends(conf),
            ConfigRoot::Actions(conf) => self.commit_actions(conf),
            ConfigRoot::Sources(conf) => self.commit_sources(conf),
            ConfigRoot::RuleEngine(conf) => self.commit_rule_engine(conf),
            ConfigRoot::Transformations(conf) => self.commit_transformations(conf),
            ConfigRoot::Schemas(conf) => self.commit_schemas(conf),
            ConfigRoot::ClusterMeta(conf) => self.commit_cluster_meta(conf),
            ConfigRoot::GlobalZone(conf) => self.commit_global_zone(conf),
            ConfigRoot::Prometheus(conf) => self.commit_prometheus(conf),
            ConfigRoot::Namespaces(conf) => self.commit_namespaces(conf),
        }
    }

    /// Commits a validated admin-users root.
    ///
    /// Records a version in the history (actor `api`, naming the root) so
    /// every applied change is versioned, and persists the history file
    /// alongside `state.toml`.
    pub fn commit_admin_users(&self, conf: AdminUsersConf) -> Result<(), ConfigError> {
        conf.validate()?;
        let old = self.snapshot();
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.admin_users = conf.clone();
            next
        });
        let next = self.snapshot();
        self.history
            .record("api", "commit admin_users", &old, &next);
        self.save_history_file()?;
        Ok(())
    }

    /// Commits a validated MQTT-users root.
    ///
    /// Records a version in the history (actor `api`, naming the root) so
    /// every applied change is versioned, and persists the history file
    /// alongside `state.toml`.
    pub fn commit_mqtt_users(&self, conf: MqttUsersConf) -> Result<(), ConfigError> {
        conf.validate()?;
        let old = self.snapshot();
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.mqtt_users = conf.clone();
            next
        });
        let next = self.snapshot();
        self.history.record("api", "commit mqtt_users", &old, &next);
        self.save_history_file()?;
        Ok(())
    }

    /// Commits a validated rules root.
    ///
    /// Records a version in the history (actor `api`, naming the root) so
    /// every applied change is versioned, and persists the history file
    /// alongside `state.toml`.
    pub fn commit_rules(&self, conf: RulesConf) -> Result<(), ConfigError> {
        conf.validate()?;
        let old = self.snapshot();
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.rules = conf.clone();
            next
        });
        let next = self.snapshot();
        self.history.record("api", "commit rules", &old, &next);
        self.save_history_file()?;
        Ok(())
    }

    /// Commits a validated connectors root.
    ///
    /// Records a version in the history (actor `api`, naming the root) so
    /// every applied change is versioned, and persists the history file
    /// alongside `state.toml`.
    pub fn commit_connectors(&self, conf: ConnectorsConf) -> Result<(), ConfigError> {
        conf.validate()?;
        let old = self.snapshot();
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.connectors = conf.clone();
            next
        });
        let next = self.snapshot();
        self.history.record("api", "commit connectors", &old, &next);
        self.save_history_file()?;
        Ok(())
    }

    /// Validates a whole snapshot across roots without changing visible state.
    ///
    /// Reports every offending setting in one error; the running snapshot
    /// is untouched on failure.
    pub fn validate_whole_snapshot(&self, next: &FullSnapshot) -> Result<(), ConfigError> {
        next.validate_whole()
    }

    /// Diffs two snapshots into per-setting rows (setting, old, new).
    #[must_use]
    pub fn diff(&self, old: &FullSnapshot, new: &FullSnapshot) -> Vec<ConfigDiff> {
        runtime::diff_snapshots(old, new)
    }

    /// Commits a whole snapshot atomically after whole-config validation.
    ///
    /// Either the entire change set becomes visible or nothing does; a
    /// failure names every offending setting and leaves the running
    /// snapshot untouched. Records a version in the history and persists
    /// both files on success.
    pub fn commit_whole(
        &self,
        next: FullSnapshot,
        actor: &str,
        summary: &str,
    ) -> Result<u64, ConfigError> {
        self.validate_whole_snapshot(&next)?;
        let old = self.snapshot();

        // Prepare the new history entry without mutating shared state yet.
        let changes = runtime::diff_snapshots(&old, &next);
        let id = self.history.next_id();
        let new_version = runtime::ConfigVersion {
            id,
            timestamp_secs: runtime::now_secs(),
            actor: actor.to_string(),
            summary: summary.to_string(),
            changes,
            snapshot: next.clone(),
        };

        // Serialise both artefacts to bytes first.
        let snapshot_bytes = toml::to_string(&next).map_err(|err| ConfigError::Save {
            file: state_path(&self.dir).display().to_string(),
            reason: format!("cannot serialise snapshot: {err}"),
        })?;
        let mut history_versions = self.history.list();
        history_versions.push(new_version.clone());
        while history_versions.len() > runtime::MAX_HISTORY_ENTRIES {
            history_versions.remove(0);
        }
        let history_bytes =
            serde_json::to_string_pretty(&history_versions).map_err(|err| ConfigError::Save {
                file: self.history_path().display().to_string(),
                reason: format!("cannot serialise history: {err}"),
            })?;

        // Persist both files atomically (temp-then-rename, fsynced).
        self.write_atomic(&state_path(&self.dir), snapshot_bytes.as_bytes())?;
        self.write_atomic(&self.history_path(), history_bytes.as_bytes())?;

        // Only now mutate in-memory state.
        self.current.store(Arc::new(next));
        self.history.commit_record(new_version);
        Ok(id)
    }

    /// Lists versions newest-first.
    #[must_use]
    pub fn history_list(&self) -> Vec<ConfigVersion> {
        self.history.list()
    }

    /// Fetches one version by id.
    #[must_use]
    pub fn history_get(&self, id: u64) -> Option<ConfigVersion> {
        self.history.get(id)
    }

    /// Restores a recorded version through the same validate-whole path.
    pub fn history_restore(&self, id: u64, actor: &str) -> Result<u64, ConfigError> {
        let version = self.history_get(id).ok_or_else(|| {
            ConfigError::Invalid(format!(
                "config version {id} does not exist (field `version`)"
            ))
        })?;
        self.commit_whole(version.snapshot, actor, &format!("restore version {id}"))
    }

    /// Truncate history to at most `max_len` versions (newest removed first).
    /// Used to roll back history when a reload fails after a version was recorded.
    pub fn history_truncate_to(&self, max_len: usize) {
        self.history.truncate_to(max_len);
    }

    /// Truncate history and persist the result to disk. Used when a reload
    /// fails after live owners were applied but before the whole commit.
    pub fn truncate_history_and_save(&self, max_len: usize) -> Result<(), ConfigError> {
        self.history.truncate_to(max_len);
        self.save_history_file()
    }

    /// Seeds the boot version when history is empty (load path).
    pub fn seed_boot_version(&self) {
        self.history.seed_boot(&self.snapshot());
    }

    fn history_path(&self) -> PathBuf {
        self.dir.join(runtime::HISTORY_FILE_NAME)
    }

    fn load_history_file(&self) {
        let path = self.history_path();
        let Ok(text) = std::fs::read_to_string(&path) else {
            return;
        };
        if let Ok(versions) = serde_json::from_str::<Vec<ConfigVersion>>(&text) {
            self.history.replace(versions);
        }
    }

    pub fn save_history_file(&self) -> Result<(), ConfigError> {
        let path = self.history_path();
        let versions = self.history.list();
        let text = serde_json::to_string_pretty(&versions).map_err(|err| ConfigError::Save {
            file: path.display().to_string(),
            reason: format!("cannot serialise history: {err}"),
        })?;
        // Atomic write beside state.toml (same temp-then-rename discipline,
        // fsynced before the rename); never touches the config directory.
        self.ensure_data_dir(&path)?;
        let tmp = self.dir.join(format!(
            ".{}.{}.tmp",
            runtime::HISTORY_FILE_NAME,
            std::process::id()
        ));
        let write_result = (|| -> std::io::Result<()> {
            use std::io::Write as _;
            let mut handle = std::fs::File::create(&tmp)?;
            handle.write_all(text.as_bytes())?;
            handle.sync_all()?;
            drop(handle);
            std::fs::rename(&tmp, &path)?;
            Ok(())
        })();
        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&tmp);
            return Err(ConfigError::Save {
                file: path.display().to_string(),
                reason: format!("cannot write file: {err}"),
            });
        }
        Ok(())
    }

    /// Commits a validated authenticator-chain root.
    pub fn commit_authn_chain(&self, conf: AuthnChainConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.authn_chain = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated node authentication cache config root.
    pub fn commit_authn_node_cache(&self, conf: AuthnNodeCacheConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.authn_node_cache = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated global authentication settings.
    pub fn commit_authn_settings(&self, conf: AuthnSettingsConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.authn_settings = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated node authorization cache config root.
    pub fn commit_authz_node_cache(&self, conf: AuthzNodeCacheConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.authz_node_cache = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated global authorization settings.
    pub fn commit_authz_settings(&self, conf: AuthzSettingsConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.authz_settings = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated authorization source registry.
    pub fn commit_authz_sources(&self, conf: AuthzSourcesConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.authz_sources = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated management API key records.
    pub fn commit_api_keys(&self, conf: ApiKeysConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.api_keys = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated single-sign-on backend registry.
    pub fn commit_sso_backends(&self, conf: SsoBackendsConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.sso_backends = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated rule-engine action registry.
    pub fn commit_actions(&self, conf: ActionsConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.actions = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated ingress source registry.
    pub fn commit_sources(&self, conf: SourcesConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.sources = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated rule-engine settings.
    pub fn commit_rule_engine(&self, conf: RuleEngineConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.rule_engine = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated message transformation chain.
    pub fn commit_transformations(&self, conf: TransformationsConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.transformations = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits a validated schema registry.
    pub fn commit_schemas(&self, conf: SchemasConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.schemas = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated cluster display text.
    pub fn commit_cluster_meta(&self, conf: ClusterMetaConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.cluster_meta = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated default-zone MQTT knobs.
    pub fn commit_global_zone(&self, conf: GlobalZoneConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.global_zone = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated managed tenant namespaces.
    pub fn commit_namespaces(&self, conf: NamespacesConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.namespaces = conf.clone();
            next
        });
        Ok(())
    }

    /// Commits validated exporter settings without touching other roots.
    pub fn commit_prometheus(&self, conf: PrometheusConf) -> Result<(), ConfigError> {
        conf.validate()?;
        self.current.rcu(|snapshot| {
            let mut next = (**snapshot).clone();
            next.prometheus = conf.clone();
            next
        });
        Ok(())
    }

    /// Serialises the current snapshot to TOML and writes it atomically.
    ///
    /// The snapshot is written to a unique temp file in the same
    /// directory, fsynced, then renamed over `state.toml`, so readers
    /// never observe a half-written file and no temp file is left behind.
    pub fn save(&self) -> Result<(), ConfigError> {
        let file = state_path(&self.dir);
        let text = toml::to_string(&*self.snapshot()).map_err(|err| ConfigError::Save {
            file: file.display().to_string(),
            reason: format!("cannot serialise snapshot: {err}"),
        })?;
        self.write_atomic(&file, text.as_bytes())
    }

    fn write_atomic(&self, file: &Path, bytes: &[u8]) -> Result<(), ConfigError> {
        let save_error = |reason: String| ConfigError::Save {
            file: file.display().to_string(),
            reason,
        };
        self.ensure_data_dir(file)?;
        let counter = SAVE_TMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = self.dir.join(format!(
            ".{STATE_FILE_NAME}.{}.tmp",
            u64::from(std::process::id()) * 1_000_000 + counter
        ));
        let write_result = (|| -> std::io::Result<()> {
            use std::io::Write as _;
            let mut handle = std::fs::File::create(&tmp)?;
            handle.write_all(bytes)?;
            handle.sync_all()?;
            drop(handle);
            std::fs::rename(&tmp, file)?;
            Ok(())
        })();
        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&tmp);
            return Err(save_error(format!("cannot write file: {err}")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_dir(prefix: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let dir = std::env::temp_dir().join(format!(
            "broker-config-{prefix}-{}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        dir
    }

    fn sample_snapshot() -> FullSnapshot {
        FullSnapshot {
            admin_users: AdminUsersConf {
                users: vec![AdminUser {
                    username: "admin".to_string(),
                    password_hash: "hash-admin".to_string(),
                    role: "administrator".to_string(),
                    description: "Default administrator".to_string(),
                    must_change_password: true,
                    mfa_secret: String::new(),
                    mfa_enabled: false,
                    mfa_last_step: None,
                }],
            },
            mqtt_users: MqttUsersConf {
                users: vec![MqttUser {
                    username: "sensor".to_string(),
                    password_hash: "hash-sensor".to_string(),
                    max_connections: None,
                    max_publish_rate: None,
                    max_publish_burst: None,
                }],
                acls: vec![AclConf {
                    username: "sensor".to_string(),
                    topic: "sensors/#".to_string(),
                    action: "all".to_string(),
                    allow: true,
                }],
                tenants: Vec::new(),
            },
            rules: RulesConf {
                rules: vec![RuleEntry {
                    id: "rule-1".to_string(),
                    name: "rule-1".to_string(),
                    tenant: "default".to_string(),
                    topic_filter: "sensors/#".to_string(),
                    sql_query: Some("SELECT * FROM 'sensors/#'".to_string()),
                    enabled: true,
                    actions: Vec::new(),
                }],
            },
            connectors: ConnectorsConf {
                connectors: vec![ConnectorEntry {
                    id: "mysql-main".to_string(),
                    connector_type: "mysql".to_string(),
                    enable: true,
                    config: "mysql://root:pw@127.0.0.1:3306/db".to_string(),
                }],
            },
            authn_chain: AuthnChainConf::default(),
            authn_node_cache: AuthnNodeCacheConf::default(),
            authn_settings: AuthnSettingsConf::default(),
            authz_node_cache: AuthzNodeCacheConf::default(),
            authz_settings: AuthzSettingsConf::default(),
            authz_sources: AuthzSourcesConf::default(),
            api_keys: ApiKeysConf::default(),
            sso_backends: SsoBackendsConf::default(),
            actions: ActionsConf::default(),
            sources: SourcesConf::default(),
            rule_engine: RuleEngineConf::default(),
            transformations: TransformationsConf::default(),
            schemas: SchemasConf::default(),
            cluster_meta: ClusterMetaConf::default(),
            global_zone: GlobalZoneConf::default(),
            prometheus: PrometheusConf::default(),
            namespaces: NamespacesConf::default(),
        }
    }

    #[test]
    fn rule_engine_defaults_match_documented_shape() {
        let defaults = RuleEngineConf::default();
        assert!(defaults.ignore_sys_message);
        assert_eq!(defaults.jq_function_default_timeout, "10s");
        assert_eq!(defaults.jq_timeout_ms(), 10_000);
        assert_eq!(defaults.jq_implementation_module, "jq_nif");
        assert!(!defaults.ssrf.enable);
        assert!(defaults.ssrf.allow_cidrs.is_empty());
        assert_eq!(defaults.ssrf.deny_cidrs.len(), 12);
        assert!(defaults
            .ssrf
            .deny_cidrs
            .contains(&"127.0.0.0/8".to_string()));
        assert_eq!(defaults.ssrf.deny_hosts.len(), 3);
        assert!(defaults.limit_selects_in_namespace);
        defaults.validate().expect("documented defaults validate");
        defaults
            .ssrf
            .validate()
            .expect("documented ssrf defaults validate");
    }

    #[test]
    fn rule_engine_validation_rejects_bad_fields() {
        let good = RuleEngineConf::default();
        for bad in [
            RuleEngineConf {
                jq_function_default_timeout: "soon".to_string(),
                ..good.clone()
            },
            RuleEngineConf {
                jq_function_default_timeout: "0s".to_string(),
                ..good.clone()
            },
            RuleEngineConf {
                jq_function_default_timeout: "2h".to_string(),
                ..good.clone()
            },
            RuleEngineConf {
                jq_implementation_module: "jq_wasm".to_string(),
                ..good.clone()
            },
            RuleEngineConf {
                ssrf: SsrfConf {
                    allow_cidrs: vec!["not-a-cidr".to_string()],
                    ..good.ssrf.clone()
                },
                ..good.clone()
            },
            RuleEngineConf {
                ssrf: SsrfConf {
                    deny_cidrs: vec!["10.0.0.0/33".to_string()],
                    ..good.ssrf.clone()
                },
                ..good.clone()
            },
            RuleEngineConf {
                ssrf: SsrfConf {
                    deny_hosts: vec![String::new()],
                    ..good.ssrf.clone()
                },
                ..good.clone()
            },
        ] {
            let err = bad.validate().expect_err("bad engine config must fail");
            assert!(
                err.to_string().contains("rule_engine"),
                "error must name the root, got: {err}"
            );
        }
    }

    #[test]
    fn admin_validation_rejects_empty_username() {
        let conf = AdminUsersConf {
            users: vec![AdminUser {
                username: String::new(),
                password_hash: "hash".to_string(),
                role: "viewer".to_string(),
                description: String::new(),
                must_change_password: false,
                mfa_secret: String::new(),
                mfa_enabled: false,
                mfa_last_step: None,
            }],
        };
        let err = conf.validate().expect_err("empty username must fail");
        assert!(
            err.to_string().contains("username"),
            "error must name the field, got: {err}"
        );
    }

    #[test]
    fn rules_validation_rejects_duplicate_id() {
        let rule = RuleEntry {
            id: "rule-1".to_string(),
            name: "rule-1".to_string(),
            tenant: "default".to_string(),
            topic_filter: "a/#".to_string(),
            sql_query: Some("SELECT * FROM 'a/#'".to_string()),
            enabled: true,
            actions: Vec::new(),
        };
        let conf = RulesConf {
            rules: vec![rule.clone(), rule],
        };
        let err = conf.validate().expect_err("duplicate rule id must fail");
        let text = err.to_string();
        assert!(
            text.contains("id"),
            "error must name the field, got: {text}"
        );
        assert!(
            text.contains("rule-1"),
            "error must name the key, got: {text}"
        );
    }

    #[test]
    fn builtin_function_actions_survive_the_toml_state_file() {
        let mut properties = BTreeMap::new();
        properties.insert("Message-Expiry-Interval".to_string(), "60".to_string());
        let rule = RuleEntry {
            id: "rule-fn".to_string(),
            name: "rule-fn".to_string(),
            tenant: "default".to_string(),
            topic_filter: "a/#".to_string(),
            sql_query: Some("SELECT * FROM 'a/#'".to_string()),
            enabled: true,
            actions: vec![
                RuleActionEntry::RepublishFunction {
                    args: RepublishArgs {
                        topic: "out/${dev}".to_string(),
                        payload: "v=${v}".to_string(),
                        qos: RepublishQos::Fixed(1),
                        retain: RepublishFlag::Template("${retain}".to_string()),
                        mqtt_properties: properties,
                        user_properties: "${user_properties}".to_string(),
                        direct_dispatch: RepublishFlag::Fixed(true),
                    },
                },
                RuleActionEntry::RepublishFunction {
                    args: RepublishArgs {
                        topic: "out".to_string(),
                        payload: "${payload}".to_string(),
                        qos: RepublishQos::Template("${qos}".to_string()),
                        retain: RepublishFlag::Fixed(false),
                        mqtt_properties: BTreeMap::new(),
                        user_properties: String::new(),
                        direct_dispatch: RepublishFlag::Template("false".to_string()),
                    },
                },
                RuleActionEntry::Console,
            ],
        };
        let conf = RulesConf { rules: vec![rule] };
        conf.validate().expect("rules validate");
        let text = toml::to_string(&conf).expect("serialise rules");
        let back: RulesConf = toml::from_str(&text).expect("parse rules");
        assert_eq!(back, conf, "state file text was: {text}");
    }

    #[test]
    fn connectors_validation_rejects_empty_id() {
        let conf = ConnectorsConf {
            connectors: vec![ConnectorEntry {
                id: String::new(),
                connector_type: "mysql".to_string(),
                enable: true,
                config: String::new(),
            }],
        };
        let err = conf.validate().expect_err("empty connector id must fail");
        assert!(
            err.to_string().contains("id"),
            "error must name the field, got: {err}"
        );
    }

    #[test]
    fn connectors_validation_rejects_unknown_type() {
        let conf = ConnectorsConf {
            connectors: vec![ConnectorEntry {
                id: "c1".to_string(),
                connector_type: "not-a-connector".to_string(),
                enable: true,
                config: String::new(),
            }],
        };
        let err = conf.validate().expect_err("unknown type must fail");
        let text = err.to_string();
        assert!(
            text.contains("connector_type"),
            "error must name the field, got: {text}"
        );
        assert!(
            text.contains("not-a-connector"),
            "error must name the value, got: {text}"
        );
    }

    #[test]
    fn authz_settings_defaults_match_documented_shape() {
        let defaults = AuthzSettingsConf::default();
        assert_eq!(defaults.no_match, "deny");
        assert_eq!(defaults.deny_action, "ignore");
        assert!(defaults.cache.enable);
        assert_eq!(
            defaults.cache.max_size,
            DEFAULT_AUTHZ_SETTINGS_CACHE_MAX_SIZE
        );
        assert_eq!(defaults.cache.max_size, 32);
        assert_eq!(defaults.cache.ttl, "1m");
        assert!(defaults.cache.excludes.is_empty());
        assert!(!defaults.include_mountpoint);
        assert!(!defaults.ignore_backend_failures);
        defaults.validate().expect("documented defaults validate");
        AuthzSettingsCacheConf::default()
            .validate()
            .expect("documented cache defaults validate");
    }

    #[test]
    fn authz_sources_defaults_match_documented_shape() {
        // Default registry: only the built-in database source (W2-17).
        let defaults = AuthzSourcesConf::default();
        assert_eq!(defaults.sources.len(), 1);
        assert_eq!(defaults.sources[0].source_type, "built_in_database");
        assert!(defaults.sources[0].enable);
        defaults.validate().expect("documented defaults validate");
        assert!(is_known_authz_source_type("built_in_database"));
        assert!(!is_known_authz_source_type("nowhere"));
    }

    #[test]
    fn authz_sources_validation_rejects_bad_fields() {
        let good = AuthzSourcesConf::default();
        assert!(good.validate().is_ok());
        // Unknown type.
        let mut bad = good.clone();
        bad.sources.push(AuthzSourceEntry {
            source_type: "nowhere".to_string(),
            enable: true,
            config: String::new(),
        });
        assert!(bad.validate().is_err());
        // Duplicate type.
        let mut bad = good.clone();
        bad.sources.push(AuthzSourceEntry {
            source_type: "built_in_database".to_string(),
            enable: true,
            config: String::new(),
        });
        assert!(bad.validate().is_err());
        // Empty type.
        let mut bad = good.clone();
        bad.sources.push(AuthzSourceEntry {
            source_type: "  ".to_string(),
            enable: true,
            config: String::new(),
        });
        assert!(bad.validate().is_err());
        // Over the cap.
        let mut bad = AuthzSourcesConf { sources: vec![] };
        for source_type in KNOWN_AUTHZ_SOURCE_TYPES.iter().take(MAX_AUTHZ_SOURCES + 1) {
            bad.sources.push(AuthzSourceEntry {
                source_type: source_type.to_string(),
                enable: true,
                config: String::new(),
            });
        }
        assert!(bad.validate().is_err());
    }

    #[test]
    fn sso_backends_defaults_match_documented_shape() {
        // Default registry: empty set (W2-32). An empty set lists as an
        // empty collection, never an error.
        let defaults = SsoBackendsConf::default();
        assert!(defaults.backends.is_empty());
        defaults.validate().expect("documented defaults validate");
    }

    #[test]
    fn sso_backends_validation_rejects_bad_fields() {
        let good = SsoBackendsConf::default();
        assert!(good.validate().is_ok());
        // Empty backend.
        let mut bad = good.clone();
        bad.backends.push(SsoBackendEntry {
            backend: "  ".to_string(),
            enable: true,
            config: String::new(),
        });
        assert!(bad.validate().is_err());
        // Slash in backend.
        let mut bad = good.clone();
        bad.backends.push(SsoBackendEntry {
            backend: "has/slash".to_string(),
            enable: true,
            config: String::new(),
        });
        assert!(bad.validate().is_err());
        // Duplicate backend.
        let mut bad = good.clone();
        bad.backends.push(SsoBackendEntry {
            backend: "oidc".to_string(),
            enable: true,
            config: String::new(),
        });
        bad.backends.push(SsoBackendEntry {
            backend: "oidc".to_string(),
            enable: true,
            config: String::new(),
        });
        assert!(bad.validate().is_err());
        // Over the cap.
        let mut bad = SsoBackendsConf { backends: vec![] };
        for index in 0..=MAX_SSO_BACKENDS {
            bad.backends.push(SsoBackendEntry {
                backend: format!("backend-{index}"),
                enable: true,
                config: String::new(),
            });
        }
        assert!(bad.validate().is_err());
    }

    #[test]
    fn authz_settings_validation_rejects_bad_fields() {
        let good = AuthzSettingsConf::default();
        for bad in [
            AuthzSettingsConf {
                no_match: "sometimes".to_string(),
                ..good.clone()
            },
            AuthzSettingsConf {
                no_match: "DENY".to_string(),
                ..good.clone()
            },
            AuthzSettingsConf {
                deny_action: "log".to_string(),
                ..good.clone()
            },
            AuthzSettingsConf {
                cache: AuthzSettingsCacheConf {
                    max_size: 0,
                    ..good.cache.clone()
                },
                ..good.clone()
            },
            AuthzSettingsConf {
                cache: AuthzSettingsCacheConf {
                    max_size: MAX_AUTHZ_SETTINGS_CACHE_MAX_SIZE + 1,
                    ..good.cache.clone()
                },
                ..good.clone()
            },
            AuthzSettingsConf {
                cache: AuthzSettingsCacheConf {
                    ttl: "soon".to_string(),
                    ..good.cache.clone()
                },
                ..good.clone()
            },
            AuthzSettingsConf {
                cache: AuthzSettingsCacheConf {
                    excludes: vec![String::new()],
                    ..good.cache.clone()
                },
                ..good.clone()
            },
        ] {
            let err = bad.validate().expect_err("bad settings must fail");
            assert!(
                err.to_string().contains("authz_settings"),
                "error must name the root, got: {err}"
            );
        }
    }

    #[test]
    fn round_trip_save_load_preserves_all_roots() {
        let dir = unique_dir("roundtrip");
        let registry = ConfigRegistry::load(&dir).expect("defaults load");
        registry
            .commit(ConfigRoot::AdminUsers(sample_snapshot().admin_users))
            .expect("commit admin");
        registry
            .commit_mqtt_users(sample_snapshot().mqtt_users)
            .expect("commit mqtt");
        registry
            .commit_rules(sample_snapshot().rules)
            .expect("commit rules");
        registry
            .commit_connectors(sample_snapshot().connectors)
            .expect("commit connectors");
        let engine_conf = RuleEngineConf {
            ignore_sys_message: false,
            jq_function_default_timeout: "30s".to_string(),
            ..Default::default()
        };
        registry
            .commit_rule_engine(engine_conf.clone())
            .expect("commit engine config");
        registry.save().expect("save");
        let reloaded = ConfigRegistry::load(&dir).expect("reload");
        let mut expected = sample_snapshot();
        expected.rule_engine = engine_conf;
        assert_eq!(*reloaded.snapshot(), expected);
    }

    #[test]
    fn atomic_write_leaves_no_temp_file_behind() {
        let dir = unique_dir("atomic");
        let registry = ConfigRegistry::load(&dir).expect("defaults load");
        registry
            .commit_admin_users(sample_snapshot().admin_users)
            .expect("commit admin");
        registry.save().expect("save");
        assert!(state_path(&dir).exists(), "state.toml must exist");
        let leftovers: Vec<_> = std::fs::read_dir(&dir)
            .expect("read dir")
            .map(std::io::Result::unwrap)
            .map(|entry| entry.file_name().to_string_lossy().into_owned())
            .filter(|name| name.ends_with(".tmp") || name.contains(".tmp"))
            .collect();
        assert!(
            leftovers.is_empty(),
            "no temp file may remain, found: {leftovers:?}"
        );
    }

    #[test]
    fn corrupt_toml_errors_without_booting_defaults() {
        let dir = unique_dir("corrupt");
        std::fs::write(state_path(&dir), "[[[ not valid toml {{{").expect("write corrupt");
        let err = ConfigRegistry::load(&dir).expect_err("corrupt file must fail");
        let text = err.to_string();
        assert!(
            text.contains(STATE_FILE_NAME),
            "error must name the file, got: {text}"
        );
    }

    #[test]
    fn invalid_snapshot_on_disk_errors_naming_the_field() {
        let dir = unique_dir("invalid");
        let mut snapshot = sample_snapshot();
        snapshot.rules.rules[0].id.clear();
        let text = toml::to_string(&snapshot).expect("serialise invalid");
        std::fs::write(state_path(&dir), text).expect("write invalid");
        let err = ConfigRegistry::load(&dir).expect_err("invalid file must fail");
        let message = err.to_string();
        assert!(
            message.contains(STATE_FILE_NAME),
            "error must name the file, got: {message}"
        );
        assert!(
            message.contains("id"),
            "error must name the field, got: {message}"
        );
    }

    #[test]
    fn concurrent_snapshot_readers_never_observe_half_committed_update() {
        let dir = unique_dir("concurrent");
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("defaults load"));
        let before = registry.snapshot();
        assert!(before.rules.rules.is_empty());
        assert!(before.admin_users.users.is_empty());

        let after_admin = AdminUsersConf {
            users: vec![AdminUser {
                username: "admin".to_string(),
                password_hash: "hash".to_string(),
                role: "administrator".to_string(),
                description: String::new(),
                must_change_password: false,
                mfa_secret: String::new(),
                mfa_enabled: false,
                mfa_last_step: None,
            }],
        };
        let after_rules = RulesConf {
            rules: vec![RuleEntry {
                id: "rule-1".to_string(),
                name: "rule-1".to_string(),
                tenant: "default".to_string(),
                topic_filter: "sensors/#".to_string(),
                sql_query: Some("SELECT * FROM 'sensors/#'".to_string()),
                enabled: true,
                actions: Vec::new(),
            }],
        };

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    for _ in 0..500 {
                        let snapshot = registry.snapshot();
                        let admin_count = snapshot.admin_users.users.len();
                        let rule_count = snapshot.rules.rules.len();
                        // The two roots are committed back-to-back; a reader
                        // must see a coherent snapshot where both commits are
                        // individually whole (each root is 0 or 1 entries,
                        // never torn inside one root).
                        assert!(admin_count <= 1, "torn admin root: {admin_count}");
                        assert!(rule_count <= 1, "torn rules root: {rule_count}");
                        if admin_count == 1 {
                            assert_eq!(snapshot.admin_users.users[0].username, "admin");
                        }
                        if rule_count == 1 {
                            assert_eq!(snapshot.rules.rules[0].id, "rule-1");
                        }
                    }
                })
            })
            .collect();
        registry
            .commit_admin_users(after_admin)
            .expect("commit admin");
        registry.commit_rules(after_rules).expect("commit rules");
        for handle in handles {
            handle.join().expect("reader thread");
        }
        let final_snapshot = registry.snapshot();
        assert_eq!(final_snapshot.admin_users.users.len(), 1);
        assert_eq!(final_snapshot.rules.rules.len(), 1);
    }

    #[test]
    fn commit_rejects_invalid_root_without_changing_visible_state() {
        let dir = unique_dir("commit-guard");
        let registry = ConfigRegistry::load(&dir).expect("defaults load");
        let bad = RulesConf {
            rules: vec![RuleEntry {
                id: String::new(),
                name: String::new(),
                tenant: "default".to_string(),
                topic_filter: "a/#".to_string(),
                sql_query: Some("SELECT 1".to_string()),
                enabled: true,
                actions: Vec::new(),
            }],
        };
        let err = registry
            .commit(ConfigRoot::Rules(bad))
            .expect_err("invalid commit must fail");
        assert!(err.to_string().contains("id"));
        assert!(registry.snapshot().rules.rules.is_empty());
    }

    #[test]
    fn missing_file_boots_validated_defaults() {
        let dir = unique_dir("defaults");
        let registry = ConfigRegistry::load(&dir).expect("missing file loads defaults");
        assert_eq!(*registry.snapshot(), FullSnapshot::default());
    }

    #[test]
    fn cluster_and_zone_roots_validate_and_reject_bad_values() {
        let dir = unique_dir("cluster-zone");
        let registry = ConfigRegistry::load(&dir).expect("defaults load");
        registry
            .commit(ConfigRoot::ClusterMeta(ClusterMetaConf {
                description: "one node".to_string(),
            }))
            .expect("short description commits");
        assert_eq!(registry.snapshot().cluster_meta.description, "one node");
        let long = "x".repeat(MAX_CLUSTER_DESCRIPTION_LEN + 1);
        registry
            .commit(ConfigRoot::ClusterMeta(ClusterMetaConf {
                description: long,
            }))
            .expect_err("long description must fail");
        registry
            .commit(ConfigRoot::GlobalZone(GlobalZoneConf {
                max_inflight: 64,
                max_mqueue_len: None,
                max_topic_alias: 5,
            }))
            .expect("valid zone commits");
        let zone = registry.snapshot().global_zone.clone();
        assert_eq!(zone.max_inflight, 64);
        assert_eq!(zone.max_mqueue_len, None);
        registry
            .commit(ConfigRoot::GlobalZone(GlobalZoneConf {
                max_inflight: 0,
                max_mqueue_len: Some(10),
                max_topic_alias: 5,
            }))
            .expect_err("zero inflight must fail");
        registry
            .commit(ConfigRoot::GlobalZone(GlobalZoneConf {
                max_inflight: 64,
                max_mqueue_len: Some(0),
                max_topic_alias: 5,
            }))
            .expect_err("zero queue cap must fail");
        // A refused commit leaves the kept knobs untouched.
        assert_eq!(registry.snapshot().global_zone.max_inflight, 64);
    }

    #[test]
    fn prometheus_settings_default_validate_commit_and_round_trip() {
        let defaults = PrometheusConf::default();
        defaults.validate().expect("defaults validate");
        assert!(defaults.enable_basic_auth);
        assert_eq!(defaults.latency_buckets, "10ms, 100ms, 1s, 5s, 30s");
        assert_eq!(defaults.collectors.vm_memory, PrometheusCollector::Disabled);
        assert!(!defaults.push_gateway.enable);
        assert_eq!(defaults.push_gateway.method, "put");

        let dir = std::env::temp_dir().join(format!(
            "indramqtt-prometheus-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = ConfigRegistry::load(&dir).expect("scratch registry loads");
        let changed = PrometheusConf {
            enable_basic_auth: false,
            collectors: PrometheusCollectorsConf {
                vm_memory: PrometheusCollector::Enabled,
                ..Default::default()
            },
            push_gateway: PrometheusPushGatewayConf {
                enable: true,
                method: "post".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };
        registry
            .commit(ConfigRoot::Prometheus(changed.clone()))
            .expect("valid settings commit");
        assert_eq!(registry.snapshot().prometheus, changed);
        registry.save().expect("settings save");
        let reloaded = ConfigRegistry::load(&dir).expect("saved registry reloads");
        assert_eq!(reloaded.snapshot().prometheus, changed);
        std::fs::remove_dir_all(&dir).ok();

        for bad in [
            PrometheusConf {
                latency_buckets: "x".repeat(MAX_PROMETHEUS_TEXT_LEN + 1),
                ..PrometheusConf::default()
            },
            PrometheusConf {
                push_gateway: PrometheusPushGatewayConf {
                    method: "delete".to_string(),
                    ..PrometheusPushGatewayConf::default()
                },
                ..PrometheusConf::default()
            },
        ] {
            bad.validate().expect_err("bad settings must fail");
        }
    }

    /// TM-01: files from before per-tenant storage carry no `tenants`
    /// key. They load as the default tenant with the same rows.
    #[test]
    fn pre_change_mqtt_file_loads_as_default_tenant() {
        let parsed: MqttUsersConf =
            toml::from_str("[[users]]\nusername = \"base\"\npassword_hash = \"h\"\n")
                .expect("old file parses");
        assert!(parsed.tenants.is_empty());
        assert!(parsed.validate().is_ok());
        assert_eq!(parsed.users.len(), 1);
    }

    /// TM-01: each bad per-tenant table fails with the field named.
    #[test]
    fn per_tenant_tables_validate_fully_before_apply() {
        fn table(tenant: &str) -> TenantMqttConf {
            TenantMqttConf {
                tenant: tenant.to_string(),
                users: vec![MqttUser {
                    username: "sam".to_string(),
                    password_hash: "h".to_string(),
                    max_connections: None,
                    max_publish_rate: None,
                    max_publish_burst: None,
                }],
                acls: vec![],
            }
        }
        // The default tenant must live in the root fields.
        let mut conf = MqttUsersConf {
            users: vec![],
            acls: vec![],
            tenants: vec![table("default")],
        };
        let err = conf.validate().expect_err("default in tenants must fail");
        assert!(err.to_string().contains("tenant"), "got: {err}");
        // Duplicated tables fail.
        conf.tenants = vec![table("a"), table("a")];
        let err = conf.validate().expect_err("duplicated tenant must fail");
        assert!(err.to_string().contains("tenant"), "got: {err}");
        // Bad ids fail.
        conf.tenants = vec![table("")];
        let err = conf.validate().expect_err("empty tenant must fail");
        assert!(err.to_string().contains("tenant"), "got: {err}");
        // One valid table passes.
        conf.tenants = vec![table("tenant-a")];
        assert!(conf.validate().is_ok());
        // Same username in two tenants passes (independent identities).
        conf.tenants.push(table("tenant-b"));
        assert!(conf.validate().is_ok());
        // Tenant helper accepts ordinary ids and refuses bad ones.
        assert!(is_valid_mqtt_tenant_id("tenant-a"));
        assert!(!is_valid_mqtt_tenant_id(""));
    }
}
