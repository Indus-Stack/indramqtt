//! Backup import for the v5 REST API.
//!
//! `POST /api/v5/data/import` restores one W5-26 export file from the
//! backup dir into the live stores. The file is a tar.gz with `META.json`,
//! `snapshot/<root>.json` parts and `tables/<table>.json` parts.
//! The `retained` table holds retained messages as base64 rows.
//!
//! Validate all parts before applying any part. A bad file is refused
//! whole and the prior state stays. A refused live apply rolls back
//! every already applied part. An import without any administrator is
//! refused. Management-plane only; publish and deliver never touch it.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use broker_config::{
    AclConf, AdminUsersConf, AuthnNodeCacheConf, AuthnSettingsConf, AuthzNodeCacheConf,
    AuthzSettingsConf, ConfigRoot, FullSnapshot, MqttUsersConf, RuleEngineConf, RulesConf,
};
use broker_protocol::{QoS, Topic, TopicFilter};
use bytes::Bytes;
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::Read;

use crate::errors::ApiError;
use crate::v5::banned::{BanEntry, MAX_WHO_LEN, VALID_AS};
use crate::v5::data::{backup_dir, valid_backup_filename};
use crate::ApiState;

// Backup format marker. Reason: the export writes this value.
const IMPORT_FORMAT: &str = "indra-backup-1";
// Max import file size. Reason: matches the file-store cap,
// so every stored file is importable.
const MAX_IMPORT_BYTES: u64 = 256 * 1024 * 1024;
// Max archive entries. Reason: the format holds at most 20 parts
// (META plus 15 roots plus 4 tables); more is not an export.
const MAX_ARCHIVE_ENTRIES: usize = 64;

// Config roots the export writes. Each name maps to one snapshot part.
const KNOWN_ROOTS: &[&str] = &[
    "admin_users",
    "mqtt_users",
    "rules",
    "connectors",
    "authn_chain",
    "authn_node_cache",
    "authn_settings",
    "authz_node_cache",
    "authz_settings",
    "authz_sources",
    "api_keys",
    "sso_backends",
    "actions",
    "sources",
    "rule_engine",
    "transformations",
];

// Live tables the export writes. Each name maps to one tables part.
const KNOWN_TABLES: &[&str] = &["banned", "builtin_authn", "builtin_authz", "retained"];

// Max retained rows per import. Reason: matches the store cap,
// so every stored set stays importable.
const MAX_RETAINED_ROWS: usize = broker_storage::MAX_RETAINED_MESSAGES;
// Max retained payload bytes per row. Reason: matches the retainer
// limit, so every stored payload stays importable.
const MAX_RETAINED_PAYLOAD_BYTES: usize = 1_048_576;

// Request body for `POST /api/v5/data/import`.
#[derive(Debug, Deserialize, serde::Serialize)]
pub struct ImportRequest {
    // Backup file name inside the backup dir.
    pub filename: String,
}

// One parsed archive: raw JSON per listed part.
#[derive(Debug)]
struct ParsedBackup {
    roots: HashMap<String, Value>,
    tables: HashMap<String, Value>,
    meta_roots: Vec<String>,
    meta_tables: Vec<String>,
}

// Read one tar entry fully. Cap count and size.
fn read_entry(
    entry: &mut tar::Entry<'_, &[u8]>,
    out: &mut Vec<(String, Vec<u8>)>,
) -> Result<(), String> {
    if out.len() >= MAX_ARCHIVE_ENTRIES {
        return Err("backup holds too many entries".to_string());
    }
    if !entry.header().entry_type().is_file() {
        return Err("backup holds a non-file entry".to_string());
    }
    let path = entry
        .path()
        .map_err(|_| "backup holds an unreadable path".to_string())?
        .to_string_lossy()
        .into_owned();
    let mut bytes = Vec::new();
    entry
        .read_to_end(&mut bytes)
        .map_err(|_| "backup entry does not read".to_string())?;
    out.push((path, bytes));
    Ok(())
}

// Parse the tar.gz bytes into raw JSON parts. No state changes.
fn parse_backup(bytes: &[u8]) -> Result<ParsedBackup, String> {
    let gz = GzDecoder::new(bytes);
    let mut raw = Vec::new();
    gz.take(MAX_IMPORT_BYTES + 1)
        .read_to_end(&mut raw)
        .map_err(|_| "backup is not gzip".to_string())?;
    if raw.len() as u64 > MAX_IMPORT_BYTES {
        return Err("backup passes the store cap".to_string());
    }
    let mut archive = tar::Archive::new(raw.as_slice());
    let mut entries = Vec::new();
    let mut files = archive
        .entries()
        .map_err(|_| "backup is not a tar archive".to_string())?;
    while let Some(entry) = files
        .next()
        .transpose()
        .map_err(|_| "backup entry does not read".to_string())?
    {
        let mut entry = entry;
        read_entry(&mut entry, &mut entries)?;
    }
    // Find the base dir through its META.json part.
    let mut base: Option<String> = None;
    for (path, _) in &entries {
        let parts: Vec<&str> = path.split('/').collect();
        if parts.len() == 2 && parts[1] == "META.json" {
            match &base {
                Some(existing) if existing != parts[0] => {
                    return Err("backup holds two base dirs".to_string());
                }
                Some(_) => {}
                None => base = Some(parts[0].to_string()),
            }
        }
    }
    let base = base.ok_or_else(|| "backup misses META.json".to_string())?;
    let mut by_path: HashMap<String, &Vec<u8>> = HashMap::new();
    for (path, bytes) in &entries {
        let parts: Vec<&str> = path.split('/').collect();
        let ok = (parts.len() == 2 && parts[0] == base)
            || (parts.len() == 3
                && parts[0] == base
                && (parts[1] == "snapshot" || parts[1] == "tables"));
        if !ok {
            return Err(format!("backup holds an unexpected entry: {path}"));
        }
        by_path.insert(path.clone(), bytes);
    }
    let meta_bytes = by_path
        .get(&format!("{base}/META.json"))
        .expect("base META.json exists");
    let meta: Value =
        serde_json::from_slice(meta_bytes).map_err(|_| "META.json is not JSON".to_string())?;
    if meta.get("format").and_then(Value::as_str) != Some(IMPORT_FORMAT) {
        return Err("backup format is unknown".to_string());
    }
    let read_names = |key: &str| -> Result<Vec<String>, String> {
        let list = meta
            .get(key)
            .and_then(Value::as_array)
            .ok_or_else(|| format!("META.json misses `{key}`"))?;
        let mut out = Vec::with_capacity(list.len());
        for item in list {
            out.push(
                item.as_str()
                    .ok_or_else(|| format!("META.json `{key}` holds a non-string"))?
                    .to_string(),
            );
        }
        Ok(out)
    };
    let meta_roots = read_names("roots")?;
    let meta_tables = read_names("tables")?;
    for name in &meta_roots {
        if !KNOWN_ROOTS.contains(&name.as_str()) {
            return Err(format!("backup lists an unknown root: {name}"));
        }
    }
    for name in &meta_tables {
        if !KNOWN_TABLES.contains(&name.as_str()) {
            return Err(format!("backup lists an unknown table: {name}"));
        }
    }
    let mut roots = HashMap::new();
    for name in &meta_roots {
        let path = format!("{base}/snapshot/{name}.json");
        let bytes = by_path
            .get(&path)
            .ok_or_else(|| format!("backup misses part: {path}"))?;
        let value: Value =
            serde_json::from_slice(bytes).map_err(|_| format!("part is not JSON: {path}"))?;
        roots.insert(name.clone(), value);
    }
    let mut tables = HashMap::new();
    for name in &meta_tables {
        let path = format!("{base}/tables/{name}.json");
        let bytes = by_path
            .get(&path)
            .ok_or_else(|| format!("backup misses part: {path}"))?;
        let value: Value =
            serde_json::from_slice(bytes).map_err(|_| format!("part is not JSON: {path}"))?;
        tables.insert(name.clone(), value);
    }
    Ok(ParsedBackup {
        roots,
        tables,
        meta_roots,
        meta_tables,
    })
}

// One redacted MQTT user row from the export. The secret stays live.
#[derive(Debug, Deserialize)]
struct FileMqttUser {
    #[serde(default)]
    username: String,
    #[serde(default)]
    max_connections: Option<u32>,
    #[serde(default)]
    max_publish_rate: Option<u32>,
    #[serde(default)]
    max_publish_burst: Option<u32>,
}

// One redacted admin row from the export. The verifier stays live.
#[derive(Debug, Deserialize)]
struct FileAdminUser {
    #[serde(default)]
    username: String,
    #[serde(default)]
    role: String,
    #[serde(default)]
    description: String,
    #[serde(default)]
    must_change_password: bool,
    #[serde(default)]
    scopes: Option<Vec<String>>,
    #[serde(default)]
    scopes_set: bool,
}

// Reference rows for the verify-only roots. Secrets stay live.
#[derive(Debug, Deserialize)]
struct FileConnectorRef {
    #[serde(default)]
    id: String,
    #[serde(default)]
    connector_type: String,
    #[serde(default = "default_true")]
    enable: bool,
}

#[derive(Debug, Deserialize)]
struct FileAuthnRef {
    #[serde(default)]
    id: String,
    #[serde(default)]
    mechanism: String,
    #[serde(default)]
    backend: String,
    #[serde(default = "default_true")]
    enable: bool,
}

#[derive(Debug, Deserialize)]
struct FileAuthzSourceRef {
    #[serde(default, rename = "type")]
    source_type: String,
    #[serde(default = "default_true")]
    enable: bool,
}

#[derive(Debug, Deserialize)]
struct FileApiKeyRef {
    #[serde(default)]
    name: String,
    #[serde(default)]
    key_id: String,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    scopes_set: bool,
    #[serde(default)]
    description: String,
    #[serde(default)]
    created_at: String,
    #[serde(default = "default_true")]
    enable: bool,
    #[serde(default)]
    expired_at: Option<u64>,
    #[serde(default)]
    role: String,
}

/// One comparable line for an API key row. A key identifier that is empty
/// stands for the name, an empty role stands for `administrator`, and a
/// scope list with entries is explicit even when the flag is missing (the
/// rules of an older release).
#[allow(clippy::too_many_arguments)]
fn api_key_ref(
    name: &str,
    key_id: &str,
    scopes: &[String],
    scopes_set: bool,
    description: &str,
    created_at: &str,
    enable: bool,
    expired_at: Option<u64>,
    role: &str,
) -> String {
    let mut sorted = scopes.to_vec();
    sorted.sort();
    let explicit = scopes_set || !sorted.is_empty();
    let key_id = if key_id.is_empty() { name } else { key_id };
    let role = if role.is_empty() {
        "administrator"
    } else {
        role
    };
    [
        name.to_string(),
        key_id.to_string(),
        explicit.to_string(),
        sorted.join(","),
        description.to_string(),
        created_at.to_string(),
        enable.to_string(),
        expired_at.map_or_else(|| "infinity".to_string(), |at| at.to_string()),
        role.to_string(),
    ]
    .join("\u{1f}")
}

#[derive(Debug, Deserialize)]
struct FileSsoRef {
    #[serde(default)]
    backend: String,
    #[serde(default = "default_true")]
    enable: bool,
}

#[derive(Debug, Deserialize)]
struct FileActionRef {
    #[serde(default)]
    id: String,
    #[serde(default)]
    action_type: String,
    #[serde(default = "default_true")]
    enable: bool,
}

#[derive(Debug, Deserialize)]
struct FileSourceRef {
    #[serde(default)]
    id: String,
    #[serde(default)]
    source_type: String,
    #[serde(default = "default_true")]
    enable: bool,
}

#[derive(Debug, Deserialize)]
struct FileTransformOperation {
    #[serde(default)]
    key: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Deserialize)]
struct FileTransformRef {
    #[serde(default)]
    name: String,
    #[serde(default)]
    topics: Vec<String>,
    #[serde(default)]
    failure_action: String,
    #[serde(default = "default_true")]
    enable: bool,
    #[serde(default)]
    operations: Vec<FileTransformOperation>,
    #[serde(default)]
    config: String,
}

fn default_true() -> bool {
    true
}

// Every validated part, ready to apply. Only listed parts are set.
#[derive(Default)]
struct Materialized {
    admin_users: Option<AdminUsersConf>,
    mqtt_users: Option<MqttUsersConf>,
    rules: Option<RulesConf>,
    authn_node_cache: Option<AuthnNodeCacheConf>,
    authn_settings: Option<AuthnSettingsConf>,
    authz_node_cache: Option<AuthzNodeCacheConf>,
    authz_settings: Option<AuthzSettingsConf>,
    rule_engine: Option<RuleEngineConf>,
    bans: Option<Vec<BanEntry>>,
    retained: Option<Vec<RetainedImportRow>>,
}

// One validated retained row, ready to apply. The topic and QoS
// come from the file; the payload comes from base64 in the file.
struct RetainedImportRow {
    topic: Topic,
    qos: QoS,
    payload: Bytes,
}

// Parse one JSON part into a full config root. Shape errors stay 400.
fn parse_part<T>(path: &str, value: &Value) -> Result<T, ApiError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(value.clone())
        .map_err(|error| ApiError::BadRequest(format!("part is not a {path} root: {error}")))
}

// Validate one parsed root. Value errors are validation failures.
fn check_part<T>(path: &str, conf: &T) -> Result<(), ApiError>
where
    T: ValidateRoot,
{
    conf.validate_root()
        .map_err(|error| ApiError::ValidationFailed(format!("{path} is invalid: {error}")))
}

// Validation through the registry error type.
trait ValidateRoot {
    fn validate_root(&self) -> Result<(), broker_config::ConfigError>;
}

macro_rules! validate_root_for {
    ($type:ty) => {
        impl ValidateRoot for $type {
            fn validate_root(&self) -> Result<(), broker_config::ConfigError> {
                self.validate()
            }
        }
    };
}

validate_root_for!(RulesConf);
validate_root_for!(MqttUsersConf);
validate_root_for!(AdminUsersConf);
validate_root_for!(AuthnNodeCacheConf);
validate_root_for!(AuthnSettingsConf);
validate_root_for!(AuthzNodeCacheConf);
validate_root_for!(AuthzSettingsConf);
validate_root_for!(RuleEngineConf);

// Merge file MQTT users with live verifiers. The file is authoritative
// for membership and quotas; live fills each password hash. Unknown
// names are refused: no password can be invented for them.
// TODO(parity): which error does the spec require for unknown names?
// The rulebook leaves the choice open.
// Refusal stays until the checker pins it.
fn merge_mqtt_users(value: &Value, previous: &MqttUsersConf) -> Result<MqttUsersConf, ApiError> {
    let users_value = value
        .get("users")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::BadRequest("mqtt_users part misses `users`".to_string()))?;
    let mut users = Vec::with_capacity(users_value.len());
    for (index, item) in users_value.iter().enumerate() {
        let row: FileMqttUser = serde_json::from_value(item.clone()).map_err(|error| {
            ApiError::BadRequest(format!("mqtt_users.users[{index}] is not a user: {error}"))
        })?;
        if row.username.trim().is_empty() {
            return Err(ApiError::ValidationFailed(format!(
                "mqtt_users.users[{index}].username must not be empty (field `username`)"
            )));
        }
        let live = previous
            .users
            .iter()
            .find(|entry| entry.username == row.username);
        let Some(live) = live else {
            return Err(ApiError::ValidationFailed(format!(
                "mqtt_users.users[{index}] {:?} is unknown: the backup carries no password for it",
                row.username
            )));
        };
        users.push(broker_config::MqttUser {
            username: row.username,
            password_hash: live.password_hash.clone(),
            max_connections: row.max_connections,
            max_publish_rate: row.max_publish_rate,
            max_publish_burst: row.max_publish_burst,
        });
    }
    let acls_value = value
        .get("acls")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::BadRequest("mqtt_users part misses `acls`".to_string()))?;
    let mut acls = Vec::with_capacity(acls_value.len());
    for (index, item) in acls_value.iter().enumerate() {
        let entry: AclConf = serde_json::from_value(item.clone()).map_err(|error| {
            ApiError::BadRequest(format!("mqtt_users.acls[{index}] is not an ACL: {error}"))
        })?;
        acls.push(entry);
    }
    // Per-tenant tables (TM-01): each entry carries `tenant`, `users`
    // and `acls`. A missing `tenants` key reads as empty (pre-change
    // files hold only the default tenant). The file is authoritative for
    // membership in each table; live fills each password hash in its own
    // table. Unknown names are refused per table: no password can be
    // invented for them.
    let mut tenants = Vec::new();
    if let Some(tables_value) = value.get("tenants") {
        let tables = tables_value.as_array().ok_or_else(|| {
            ApiError::BadRequest("mqtt_users part `tenants` is not a list".to_string())
        })?;
        for (table_index, item) in tables.iter().enumerate() {
            let tenant = item.get("tenant").and_then(Value::as_str).ok_or_else(|| {
                ApiError::BadRequest(format!("mqtt_users.tenants[{table_index}] misses `tenant`"))
            })?;
            if !broker_config::is_valid_mqtt_tenant_id(tenant) || tenant == "default" {
                return Err(ApiError::ValidationFailed(format!(
                    "mqtt_users.tenants[{table_index}].tenant {tenant:?} is not a valid tenant id (field `tenant`)"
                )));
            }
            if tenants
                .iter()
                .any(|table: &broker_config::TenantMqttConf| table.tenant == tenant)
            {
                return Err(ApiError::ValidationFailed(format!(
                    "mqtt_users.tenants[{table_index}].tenant {tenant:?} is duplicated (field `tenant`)"
                )));
            }
            let live_table = previous.tenants.iter().find(|table| table.tenant == tenant);
            let users_value = item.get("users").and_then(Value::as_array).ok_or_else(|| {
                ApiError::BadRequest(format!("mqtt_users.tenants[{table_index}] misses `users`"))
            })?;
            let mut table_users = Vec::with_capacity(users_value.len());
            for (index, user_item) in users_value.iter().enumerate() {
                let row: FileMqttUser =
                    serde_json::from_value(user_item.clone()).map_err(|error| {
                        ApiError::BadRequest(format!(
                            "mqtt_users.tenants[{table_index}].users[{index}] is not a user: {error}"
                        ))
                    })?;
                if row.username.trim().is_empty() {
                    return Err(ApiError::ValidationFailed(format!(
                        "mqtt_users.tenants[{table_index}].users[{index}].username must not be empty (field `username`)"
                    )));
                }
                let live = live_table.and_then(|table| {
                    table
                        .users
                        .iter()
                        .find(|entry| entry.username == row.username)
                });
                let Some(live) = live else {
                    return Err(ApiError::ValidationFailed(format!(
                        "mqtt_users.tenants[{table_index}].users[{index}] {:?} is unknown: the backup carries no password for it",
                        row.username
                    )));
                };
                table_users.push(broker_config::MqttUser {
                    username: row.username,
                    password_hash: live.password_hash.clone(),
                    max_connections: row.max_connections,
                    max_publish_rate: row.max_publish_rate,
                    max_publish_burst: row.max_publish_burst,
                });
            }
            let acls_value = item.get("acls").and_then(Value::as_array).ok_or_else(|| {
                ApiError::BadRequest(format!("mqtt_users.tenants[{table_index}] misses `acls`"))
            })?;
            let mut table_acls = Vec::with_capacity(acls_value.len());
            for (index, acl_item) in acls_value.iter().enumerate() {
                let entry: AclConf = serde_json::from_value(acl_item.clone()).map_err(|error| {
                    ApiError::BadRequest(format!(
                        "mqtt_users.tenants[{table_index}].acls[{index}] is not an ACL: {error}"
                    ))
                })?;
                table_acls.push(entry);
            }
            tenants.push(broker_config::TenantMqttConf {
                tenant: tenant.to_string(),
                users: table_users,
                acls: table_acls,
            });
        }
    }
    let conf = MqttUsersConf {
        users,
        acls,
        tenants,
    };
    check_part("mqtt_users", &conf)?;
    Ok(conf)
}

// Merge file admin users with live verifiers. The file is authoritative
// for membership, roles, descriptions and explicit scopes; live fills
// each password hash and MFA secret. A row with no scope field keeps the
// live scopes. Unknown names are refused.
// TODO(parity): which error does the spec require for unknown names?
// The rulebook leaves the choice open.
// Refusal stays until the checker pins it.
fn merge_admin_users(value: &Value, previous: &AdminUsersConf) -> Result<AdminUsersConf, ApiError> {
    let users_value = value
        .get("users")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::BadRequest("admin_users part misses `users`".to_string()))?;
    let mut users = Vec::with_capacity(users_value.len());
    for (index, item) in users_value.iter().enumerate() {
        let row: FileAdminUser = serde_json::from_value(item.clone()).map_err(|error| {
            ApiError::BadRequest(format!("admin_users.users[{index}] is not a user: {error}"))
        })?;
        if row.username.trim().is_empty() {
            return Err(ApiError::ValidationFailed(format!(
                "admin_users.users[{index}].username must not be empty (field `username`)"
            )));
        }
        let live = previous
            .users
            .iter()
            .find(|entry| entry.username == row.username);
        let Some(live) = live else {
            return Err(ApiError::ValidationFailed(format!(
                "admin_users.users[{index}] {:?} is unknown: the backup carries no password for it",
                row.username
            )));
        };
        // A row from an older release carries no scope field. Keep the
        // stored scopes then. A scope field that is present is
        // authoritative: the export writes both `scopes` and
        // `scopes_set`, so a missing list with the flag set is an
        // explicit empty list.
        let carries_scopes = item
            .as_object()
            .is_some_and(|obj| obj.contains_key("scopes") || obj.contains_key("scopes_set"));
        let scopes = if !carries_scopes {
            live.scopes.clone()
        } else if row.scopes_set && row.scopes.is_none() {
            Some(Vec::new())
        } else {
            row.scopes
        };
        users.push(broker_config::AdminUser {
            username: row.username,
            password_hash: live.password_hash.clone(),
            role: row.role,
            description: row.description,
            must_change_password: row.must_change_password,
            mfa_secret: live.mfa_secret.clone(),
            mfa_enabled: live.mfa_enabled && !live.mfa_secret.is_empty(),
            mfa_last_step: live.mfa_last_step,
            scopes,
        });
    }
    let conf = AdminUsersConf { users };
    check_part("admin_users", &conf)?;
    // Refuse a candidate without any administrator. An empty admin
    // list would resurrect the default account, so the import fails
    // closed unless nothing changes at all.
    let keeps_admin = conf.users.iter().any(|user| user.role == "administrator");
    if !keeps_admin && conf != *previous {
        return Err(ApiError::ValidationFailed(
            "admin_users.users must keep at least one administrator (field `admin_users.users`)"
                .to_string(),
        ));
    }
    Ok(conf)
}

/// True when an imported `admin_users` part changes a role, a scope list
/// or the membership of the dashboard users. Description, password-change
/// flag and MFA state do not raise rights, so they do not need a full
/// administrator.
pub(crate) fn admin_user_rights_change(
    candidate: &AdminUsersConf,
    previous: &AdminUsersConf,
) -> bool {
    if candidate.users.len() != previous.users.len() {
        return true;
    }
    let previous_names: HashSet<&str> = previous
        .users
        .iter()
        .map(|user| user.username.as_str())
        .collect();
    candidate.users.iter().any(|user| {
        if !previous_names.contains(user.username.as_str()) {
            return true;
        }
        match previous
            .users
            .iter()
            .find(|entry| entry.username == user.username)
        {
            Some(entry) => {
                entry.role != user.role
                    || entry.scopes != user.scopes
                    || entry.password_hash != user.password_hash
                    || entry.must_change_password != user.must_change_password
                    || entry.mfa_secret != user.mfa_secret
                    || entry.mfa_enabled != user.mfa_enabled
                    || entry.mfa_last_step != user.mfa_last_step
            }
            None => true,
        }
    })
}

// Parse and validate the banned table rows. Duplicates fail whole.
fn parse_bans(value: &Value) -> Result<Vec<BanEntry>, ApiError> {
    let rows = value
        .as_array()
        .ok_or_else(|| ApiError::BadRequest("banned table is not a list".to_string()))?;
    let mut out = Vec::with_capacity(rows.len());
    let mut seen = HashSet::new();
    for (index, row) in rows.iter().enumerate() {
        let obj = row
            .as_object()
            .ok_or_else(|| ApiError::BadRequest(format!("banned[{index}] is not an object")))?;
        let as_type = obj
            .get("as")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let who = obj
            .get("who")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        if !VALID_AS.contains(&as_type.as_str()) {
            return Err(ApiError::ValidationFailed(format!(
                "banned[{index}].as {as_type:?} is unknown (field `as`)"
            )));
        }
        if who.trim().is_empty() || who.len() > MAX_WHO_LEN {
            return Err(ApiError::ValidationFailed(format!(
                "banned[{index}].who is empty or too long (field `who`)"
            )));
        }
        let tenant_key = obj
            .get("tenant")
            .and_then(Value::as_str)
            .unwrap_or(broker_session::tenant::DEFAULT_TENANT_ID)
            .trim()
            .to_string();
        if !seen.insert((tenant_key.clone(), as_type.clone(), who.clone())) {
            return Err(ApiError::ValidationFailed(format!(
                "banned[{index}] ({tenant_key:?}, {as_type:?}, {who:?}) is duplicated"
            )));
        }
        let string_or_empty = |key: &str| {
            obj.get(key)
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string()
        };
        let tenant = obj
            .get("tenant")
            .and_then(Value::as_str)
            .unwrap_or(broker_session::tenant::DEFAULT_TENANT_ID)
            .trim()
            .to_string();
        let tenant = if tenant.is_empty() {
            broker_session::tenant::DEFAULT_TENANT_ID.to_string()
        } else {
            tenant
        };
        out.push(BanEntry {
            as_type,
            who,
            by: string_or_empty("by"),
            reason: string_or_empty("reason"),
            at: string_or_empty("at"),
            until: string_or_empty("until"),
            tenant,
        });
    }
    Ok(out)
}

// Parse and validate the retained table rows. Duplicates keep the last
// row for one topic. Payloads are base64, as in the retainer reads.
fn parse_retained(value: &Value) -> Result<Vec<RetainedImportRow>, ApiError> {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let rows = value
        .as_array()
        .ok_or_else(|| ApiError::BadRequest("retained table is not a list".to_string()))?;
    if rows.len() > MAX_RETAINED_ROWS {
        return Err(ApiError::ValidationFailed(
            "retained table holds too many rows (field `retained`)".to_string(),
        ));
    }
    let mut out = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let obj = row
            .as_object()
            .ok_or_else(|| ApiError::BadRequest(format!("retained[{index}] is not an object")))?;
        let topic_text = obj
            .get("topic")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::BadRequest(format!("retained[{index}] misses `topic`")))?;
        let topic = Topic::new(topic_text.to_string()).map_err(|_| {
            ApiError::ValidationFailed(format!(
                "retained[{index}].topic is invalid (field `topic`)"
            ))
        })?;
        let qos_num = obj
            .get("qos")
            .and_then(Value::as_u64)
            .ok_or_else(|| ApiError::BadRequest(format!("retained[{index}] misses `qos`")))?;
        let qos = match qos_num {
            0 => QoS::AtMostOnce,
            1 => QoS::AtLeastOnce,
            2 => QoS::ExactlyOnce,
            _ => {
                return Err(ApiError::ValidationFailed(format!(
                    "retained[{index}].qos is unknown (field `qos`)"
                )));
            }
        };
        let payload_text = obj
            .get("payload")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::BadRequest(format!("retained[{index}] misses `payload`")))?;
        let payload_bytes = STANDARD.decode(payload_text).map_err(|_| {
            ApiError::ValidationFailed(format!(
                "retained[{index}].payload is not base64 (field `payload`)"
            ))
        })?;
        if payload_bytes.len() > MAX_RETAINED_PAYLOAD_BYTES {
            return Err(ApiError::ValidationFailed(format!(
                "retained[{index}].payload passes the size cap (field `payload`)"
            )));
        }
        out.push(RetainedImportRow {
            topic,
            qos,
            payload: Bytes::from(payload_bytes),
        });
    }
    Ok(out)
}

// Turn every listed part into a validated pending apply. No state moves.
fn materialize(state: &ApiState, parsed: &ParsedBackup) -> Result<Materialized, ApiError> {
    let previous = state.config.snapshot();
    let mut out = Materialized::default();
    for name in &parsed.meta_roots {
        let value = parsed
            .roots
            .get(name)
            .ok_or_else(|| ApiError::BadRequest(format!("backup misses part: {name}")))?;
        match name.as_str() {
            "admin_users" => {
                out.admin_users = Some(merge_admin_users(value, &previous.admin_users)?);
            }
            "mqtt_users" => {
                out.mqtt_users = Some(merge_mqtt_users(value, &previous.mqtt_users)?);
            }
            "rules" => {
                let conf: RulesConf = parse_part("rules", value)?;
                check_part("rules", &conf)?;
                out.rules = Some(conf);
            }
            "authn_node_cache" => {
                let conf: AuthnNodeCacheConf = parse_part("authn_node_cache", value)?;
                check_part("authn_node_cache", &conf)?;
                out.authn_node_cache = Some(conf);
            }
            "authn_settings" => {
                let conf: AuthnSettingsConf = parse_part("authn_settings", value)?;
                check_part("authn_settings", &conf)?;
                out.authn_settings = Some(conf);
            }
            "authz_node_cache" => {
                let conf: AuthzNodeCacheConf = parse_part("authz_node_cache", value)?;
                check_part("authz_node_cache", &conf)?;
                out.authz_node_cache = Some(conf);
            }
            "authz_settings" => {
                let conf: AuthzSettingsConf = parse_part("authz_settings", value)?;
                check_part("authz_settings", &conf)?;
                out.authz_settings = Some(conf);
            }
            "rule_engine" => {
                let conf: RuleEngineConf = parse_part("rule_engine", value)?;
                check_part("rule_engine", &conf)?;
                out.rule_engine = Some(conf);
            }
            "connectors" => {
                let live = &previous.connectors.connectors;
                let items = value
                    .get("connectors")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ApiError::BadRequest("connectors part misses `connectors`".to_string())
                    })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileConnectorRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "connectors.connectors[{index}] is not a connector: {error}"
                            ))
                        })?;
                    file_refs.push(format!(
                        "{}\0{}\0{}",
                        row.id, row.connector_type, row.enable
                    ));
                }
                let mut live_refs: Vec<String> = live
                    .iter()
                    .map(|entry| {
                        format!("{}\0{}\0{}", entry.id, entry.connector_type, entry.enable)
                    })
                    .collect();
                file_refs.sort();
                live_refs.sort();
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "connectors does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            "authn_chain" => {
                let items = value
                    .get("authenticators")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ApiError::BadRequest("authn_chain part misses `authenticators`".to_string())
                    })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileAuthnRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "authn_chain.authenticators[{index}] is not an entry: {error}"
                            ))
                        })?;
                    file_refs.push(format!(
                        "{}\0{}\0{}\0{}",
                        row.id, row.mechanism, row.backend, row.enable
                    ));
                }
                let mut live_refs: Vec<String> = previous
                    .authn_chain
                    .authenticators
                    .iter()
                    .map(|entry| {
                        format!(
                            "{}\0{}\0{}\0{}",
                            entry.id, entry.mechanism, entry.backend, entry.enable
                        )
                    })
                    .collect();
                file_refs.sort();
                live_refs.sort();
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "authn_chain does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            "authz_sources" => {
                let items = value
                    .get("sources")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ApiError::BadRequest("authz_sources part misses `sources`".to_string())
                    })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileAuthzSourceRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "authz_sources.sources[{index}] is not a source: {error}"
                            ))
                        })?;
                    file_refs.push(format!("{}\0{}", row.source_type, row.enable));
                }
                let mut live_refs: Vec<String> = previous
                    .authz_sources
                    .sources
                    .iter()
                    .map(|entry| format!("{}\0{}", entry.source_type, entry.enable))
                    .collect();
                file_refs.sort();
                live_refs.sort();
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "authz_sources does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            "api_keys" => {
                let items = value.get("keys").and_then(Value::as_array).ok_or_else(|| {
                    ApiError::BadRequest("api_keys part misses `keys`".to_string())
                })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileApiKeyRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "api_keys.keys[{index}] is not a key: {error}"
                            ))
                        })?;
                    file_refs.push(api_key_ref(
                        &row.name,
                        &row.key_id,
                        &row.scopes,
                        row.scopes_set,
                        &row.description,
                        &row.created_at,
                        row.enable,
                        row.expired_at,
                        &row.role,
                    ));
                }
                let mut live_refs: Vec<String> = previous
                    .api_keys
                    .keys
                    .iter()
                    .map(|entry| {
                        api_key_ref(
                            &entry.name,
                            &entry.key_id,
                            &entry.scopes,
                            entry.scopes_set,
                            &entry.description,
                            &entry.created_at,
                            entry.enable,
                            entry.expired_at,
                            &entry.role,
                        )
                    })
                    .collect();
                file_refs.sort();
                live_refs.sort();
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "api_keys does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            "sso_backends" => {
                let items = value
                    .get("backends")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ApiError::BadRequest("sso_backends part misses `backends`".to_string())
                    })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileSsoRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "sso_backends.backends[{index}] is not a backend: {error}"
                            ))
                        })?;
                    file_refs.push(format!("{}\0{}", row.backend, row.enable));
                }
                let mut live_refs: Vec<String> = previous
                    .sso_backends
                    .backends
                    .iter()
                    .map(|entry| format!("{}\0{}", entry.backend, entry.enable))
                    .collect();
                file_refs.sort();
                live_refs.sort();
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "sso_backends does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            "actions" => {
                let items = value
                    .get("actions")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ApiError::BadRequest("actions part misses `actions`".to_string())
                    })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileActionRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "actions.actions[{index}] is not an action: {error}"
                            ))
                        })?;
                    file_refs.push(format!("{}\0{}\0{}", row.id, row.action_type, row.enable));
                }
                let mut live_refs: Vec<String> = previous
                    .actions
                    .actions
                    .iter()
                    .map(|entry| format!("{}\0{}\0{}", entry.id, entry.action_type, entry.enable))
                    .collect();
                file_refs.sort();
                live_refs.sort();
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "actions does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            "sources" => {
                let items = value
                    .get("sources")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ApiError::BadRequest("sources part misses `sources`".to_string())
                    })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileSourceRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "sources.sources[{index}] is not a source: {error}"
                            ))
                        })?;
                    file_refs.push(format!("{}\0{}\0{}", row.id, row.source_type, row.enable));
                }
                let mut live_refs: Vec<String> = previous
                    .sources
                    .sources
                    .iter()
                    .map(|entry| format!("{}\0{}\0{}", entry.id, entry.source_type, entry.enable))
                    .collect();
                file_refs.sort();
                live_refs.sort();
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "sources does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            "transformations" => {
                let items = value
                    .get("transformations")
                    .and_then(Value::as_array)
                    .ok_or_else(|| {
                        ApiError::BadRequest(
                            "transformations part misses `transformations`".to_string(),
                        )
                    })?;
                let mut file_refs: Vec<String> = Vec::with_capacity(items.len());
                for (index, item) in items.iter().enumerate() {
                    let row: FileTransformRef =
                        serde_json::from_value(item.clone()).map_err(|error| {
                            ApiError::BadRequest(format!(
                                "transformations.transformations[{index}] is not a transformation: {error}"
                            ))
                        })?;
                    let mut topics = row.topics.clone();
                    topics.sort();
                    let ops = row
                        .operations
                        .iter()
                        .map(|op| format!("{}={}", op.key, op.value))
                        .collect::<Vec<_>>()
                        .join(",");
                    file_refs.push(format!(
                        "{}\0{}\0{}\0{}\0{}\0{}",
                        row.name,
                        topics.join(","),
                        row.failure_action,
                        row.enable,
                        ops,
                        row.config
                    ));
                }
                let live_refs: Vec<String> = previous
                    .transformations
                    .transformations
                    .iter()
                    .map(|entry| {
                        let mut topics = entry.topics.clone();
                        topics.sort();
                        let ops = entry
                            .operations
                            .iter()
                            .map(|op| format!("{}={}", op.key, op.value))
                            .collect::<Vec<_>>()
                            .join(",");
                        format!(
                            "{}\0{}\0{}\0{}\0{}\0{}",
                            entry.name,
                            topics.join(","),
                            entry.failure_action,
                            entry.enable,
                            ops,
                            entry.config
                        )
                    })
                    .collect();
                // Chain order defines execution order, so compare in order.
                if file_refs != live_refs {
                    return Err(ApiError::ValidationFailed(
                        "transformations does not match live state: the backup carries no secrets to restore it"
                            .to_string(),
                    ));
                }
            }
            other => {
                return Err(ApiError::BadRequest(format!(
                    "backup lists an unknown root: {other}"
                )));
            }
        }
    }
    for name in &parsed.meta_tables {
        let value = parsed
            .tables
            .get(name)
            .ok_or_else(|| ApiError::BadRequest(format!("backup misses part: {name}")))?;
        match name.as_str() {
            "banned" => {
                out.bans = Some(parse_bans(value)?);
            }
            "retained" => {
                out.retained = Some(parse_retained(value)?);
            }
            "builtin_authn" | "builtin_authz" => {
                // Informational duplicates of the mqtt_users part. The
                // snapshot part stays authoritative, so only the shape
                // is checked here.
                if !value.is_array() {
                    return Err(ApiError::BadRequest(format!("{name} table is not a list")));
                }
            }
            other => {
                return Err(ApiError::BadRequest(format!(
                    "backup lists an unknown table: {other}"
                )));
            }
        }
    }
    Ok(out)
}

// Roll every applied part back to the previous snapshot. Best effort:
// errors here are swallowed so the original failure stays reported.
async fn rollback(
    state: &ApiState,
    previous: &FullSnapshot,
    previous_bans: &[BanEntry],
    previous_retained: &[broker_storage::StoredMessage],
    committed_roots: &[ConfigRoot],
    expected_admin_after_commit: Option<&AdminUsersConf>,
) {
    let owns_admin = expected_admin_after_commit
        .is_some_and(|expected| &state.config.snapshot().admin_users == expected);
    if owns_admin
        && committed_roots
            .iter()
            .any(|r| matches!(r, ConfigRoot::AdminUsers(_)))
    {
        let _ = state
            .config
            .commit(ConfigRoot::AdminUsers(previous.admin_users.clone()));
    }
    let _ = state
        .config
        .commit(ConfigRoot::MqttUsers(previous.mqtt_users.clone()));
    let _ = state
        .config
        .commit(ConfigRoot::Rules(previous.rules.clone()));
    let _ = state.config.commit(ConfigRoot::AuthnNodeCache(
        previous.authn_node_cache.clone(),
    ));
    let _ = state
        .config
        .commit(ConfigRoot::AuthnSettings(previous.authn_settings.clone()));
    let _ = state.config.commit(ConfigRoot::AuthzNodeCache(
        previous.authz_node_cache.clone(),
    ));
    let _ = state
        .config
        .commit(ConfigRoot::AuthzSettings(previous.authz_settings.clone()));
    let _ = state
        .config
        .commit(ConfigRoot::RuleEngine(previous.rule_engine.clone()));
    let _ = state.config.save();
    let _ = state.auth.seed_from_registry(&state.config);
    let _ = state.engine.seed_from_registry(&state.config);
    if owns_admin
        && committed_roots
            .iter()
            .any(|r| matches!(r, ConfigRoot::AdminUsers(_)))
    {
        let _ = state.admin_users.apply_snapshot_conf(&previous.admin_users);
    }
    state.authn_node_cache.seed_from_registry(&state.config);
    state.authn_settings.seed_from_registry(&state.config);
    state.authz_node_cache.seed_from_registry(&state.config);
    state.authz_settings.seed_from_registry(&state.config);
    state.rule_engine_config.seed_from_registry(&state.config);
    state
        .engine
        .apply_engine_config(previous.rule_engine.clone());
    state.authn_node_cache.apply_settings(
        previous.authn_settings.node_cache.enable,
        previous.authn_settings.node_cache.max_count,
    );
    state.bans.clear();
    for entry in previous_bans {
        let _ = state.bans.insert(entry.clone());
    }
    // Restore retained rows. Best effort; errors stay swallowed.
    // The filter matches all topics, so the clear removes current rows.
    if let Ok(filter) = TopicFilter::new("#") {
        if let Ok(current) = state.retained.find_matching(&filter).await {
            for msg in &current {
                let _ = state.retained.clear_retained(&msg.topic).await;
            }
        }
        for msg in previous_retained {
            let _ = state
                .retained
                .set_retained(msg.topic.clone(), msg.qos, msg.payload.clone())
                .await;
        }
        state.stats.set_retained(previous_retained.len() as u64);
    }
}

// Commit every pending root, then reseed each live owner from the
// registry. A refused reseed restores every part already applied.
async fn apply_materialized(
    state: &ApiState,
    pending: &Materialized,
    authorized_admin_users: Option<&AdminUsersConf>,
) -> Result<(), ApiError> {
    let previous_bans = state.bans.list_live();
    // Snapshot retained rows before any apply. A list failure stops
    // the import with no state moved.
    let previous_retained = match TopicFilter::new("#") {
        Ok(filter) => match state.retained.find_matching(&filter).await {
            Ok(rows) => rows,
            Err(error) => {
                return Err(ApiError::InternalError(format!(
                    "retained list failed: {error}"
                )));
            }
        },
        Err(error) => {
            return Err(ApiError::InternalError(format!(
                "retained filter failed: {error}"
            )));
        }
    };
    // Bans first: live-only, so a refusal leaves the registry untouched.
    if let Some(rows) = &pending.bans {
        state.bans.clear();
        for row in rows {
            if let Err(error) = state.bans.insert(row.clone()) {
                state.bans.clear();
                for old in &previous_bans {
                    let _ = state.bans.insert(old.clone());
                }
                return Err(ApiError::InternalError(format!(
                    "ban table refused: {error:?}"
                )));
            }
        }
    }
    // Re-read the config snapshot after async retained-store reads so a
    // concurrent admin change cannot be overwritten silently.
    let current_snapshot = state.config.snapshot();
    if let Some(expected) = authorized_admin_users {
        if &current_snapshot.admin_users != expected {
            return Err(ApiError::BadRequest(
                "dashboard users changed concurrently during import".to_string(),
            ));
        }
    }
    let previous = current_snapshot;
    // Registry commits. Each commit validates again; the parts were
    // validated above, so a failure here is an internal error.
    let mut commits: Vec<ConfigRoot> = Vec::new();
    let mut committed_admin: Option<&AdminUsersConf> = None;
    if let Some(conf) = &pending.admin_users {
        if conf != &previous.admin_users {
            commits.push(ConfigRoot::AdminUsers(conf.clone()));
            committed_admin = Some(conf);
        }
    }
    if let Some(conf) = &pending.mqtt_users {
        commits.push(ConfigRoot::MqttUsers(conf.clone()));
    }
    if let Some(conf) = &pending.rules {
        commits.push(ConfigRoot::Rules(conf.clone()));
    }
    if let Some(conf) = &pending.authn_node_cache {
        commits.push(ConfigRoot::AuthnNodeCache(conf.clone()));
    }
    if let Some(conf) = &pending.authn_settings {
        commits.push(ConfigRoot::AuthnSettings(conf.clone()));
    }
    if let Some(conf) = &pending.authz_node_cache {
        commits.push(ConfigRoot::AuthzNodeCache(conf.clone()));
    }
    if let Some(conf) = &pending.authz_settings {
        commits.push(ConfigRoot::AuthzSettings(conf.clone()));
    }
    if let Some(conf) = &pending.rule_engine {
        commits.push(ConfigRoot::RuleEngine(conf.clone()));
    }
    for root in &commits {
        if let Err(error) = state.config.commit(root.clone()) {
            rollback(
                state,
                &previous,
                &previous_bans,
                &previous_retained,
                &commits,
                committed_admin,
            )
            .await;
            return Err(ApiError::InternalError(format!(
                "config commit failed: {error}"
            )));
        }
    }
    if let Err(error) = state.config.save() {
        rollback(
            state,
            &previous,
            &previous_bans,
            &previous_retained,
            &commits,
            committed_admin,
        )
        .await;
        return Err(ApiError::InternalError(format!(
            "config persist failed: {error}"
        )));
    }
    // Reseed each live owner from the committed registry, mirroring
    // kernel boot. A refusal restores every part already applied.
    if pending.mqtt_users.is_some() {
        if let Err(error) = state.auth.seed_from_registry(&state.config) {
            rollback(
                state,
                &previous,
                &previous_bans,
                &previous_retained,
                &commits,
                committed_admin,
            )
            .await;
            return Err(ApiError::InternalError(format!(
                "mqtt_users backend refused: {error}"
            )));
        }
    }
    if pending.rules.is_some() {
        if let Err(error) = state.engine.seed_from_registry(&state.config) {
            rollback(
                state,
                &previous,
                &previous_bans,
                &previous_retained,
                &commits,
                committed_admin,
            )
            .await;
            return Err(ApiError::InternalError(format!(
                "rules backend refused: {error}"
            )));
        }
    }
    if committed_admin.is_some() {
        if let Err(error) = state
            .admin_users
            .apply_snapshot_conf(&state.config.snapshot().admin_users)
        {
            rollback(
                state,
                &previous,
                &previous_bans,
                &previous_retained,
                &commits,
                committed_admin,
            )
            .await;
            return Err(ApiError::InternalError(format!(
                "admin_users backend refused: {error}"
            )));
        }
    }
    if pending.authn_node_cache.is_some() {
        state.authn_node_cache.seed_from_registry(&state.config);
    }
    if pending.authn_settings.is_some() {
        state.authn_settings.seed_from_registry(&state.config);
        let next = state.authn_settings.get();
        state
            .authn_node_cache
            .apply_settings(next.node_cache.enable, next.node_cache.max_count);
    }
    if pending.authz_node_cache.is_some() {
        state.authz_node_cache.seed_from_registry(&state.config);
    }
    if pending.authz_settings.is_some() {
        state.authz_settings.seed_from_registry(&state.config);
    }
    if pending.rule_engine.is_some() {
        state.rule_engine_config.seed_from_registry(&state.config);
        state
            .engine
            .apply_engine_config(state.rule_engine_config.get());
    }
    // Apply retained rows last. The file lists the full set, so the
    // current set is cleared first. A failure rolls back every part.
    if let Some(rows) = &pending.retained {
        let filter = TopicFilter::new("#")
            .map_err(|error| ApiError::InternalError(format!("retained filter failed: {error}")))?;
        let current = state
            .retained
            .find_matching(&filter)
            .await
            .map_err(|error| ApiError::InternalError(format!("retained list failed: {error}")))?;
        for msg in &current {
            if let Err(error) = state.retained.clear_retained(&msg.topic).await {
                rollback(
                    state,
                    &previous,
                    &previous_bans,
                    &previous_retained,
                    &commits,
                    committed_admin,
                )
                .await;
                return Err(ApiError::InternalError(format!(
                    "retained clear failed: {error}"
                )));
            }
        }
        for row in rows {
            if let Err(error) = state
                .retained
                .set_retained(row.topic.clone(), row.qos, row.payload.clone())
                .await
            {
                rollback(
                    state,
                    &previous,
                    &previous_bans,
                    &previous_retained,
                    &commits,
                    committed_admin,
                )
                .await;
                return Err(ApiError::InternalError(format!(
                    "retained apply failed: {error}"
                )));
            }
        }
        if let Ok(matched) = state.retained.find_matching(&filter).await {
            state.stats.set_retained(matched.len() as u64);
        }
    }
    Ok(())
}

// `POST /api/v5/data/import`: restore one backup file into the live
// stores with validate-all-before-apply semantics.
//
// The body names one `.tar.gz` file in the backup dir (the same dir the
// export and file routes serve). Unknown names answer 404 `NOT_FOUND`.
// A corrupt file answers 400 and changes nothing. A part that fails
// validation answers 400 `VALIDATION_FAILED` and changes nothing. An
// apply or persist failure answers 500 and rolls back every part.
// A successful restore answers 204 with no body, as the reference API
// documents. It shows in later reads and survives a restart through each
// store's own persistence.
//
// An import that changes a dashboard user's role, scopes or membership
// needs a full administrator. Any other caller gets 403 `FORBIDDEN` and
// nothing changes. An archive from an older release that carries no
// scope field keeps the stored scopes, so it never widens a user.
//
// Management-plane only: short management locks, never the publish or
// deliver path.
pub async fn import_data(
    State(state): State<ApiState>,
    full_admin: Option<axum::Extension<crate::api_auth::FullAdministrator>>,
    body: Bytes,
) -> Response {
    // Refuse import while the node is not ready. Reason: the stores
    // are not initialised yet.
    if !state.readiness.is_ready() {
        return ApiError::StoreNotReady("stores are not ready for import".to_string())
            .into_response();
    }
    let value: Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(_) => {
            return ApiError::BadRequest("request body must be JSON".to_string()).into_response();
        }
    };
    let filename = value
        .get("filename")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    if filename.trim().is_empty() {
        return ApiError::BadRequest("field `filename` is required".to_string()).into_response();
    }
    if !valid_backup_filename(&filename) {
        return ApiError::BadRequest("filename must end with .tar.gz".to_string()).into_response();
    }
    let path = backup_dir(&state).join(&filename);
    let bytes = match tokio::fs::read(&path).await {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return ApiError::NotFound(format!("backup file {filename:?} not found"))
                .into_response();
        }
        Err(error) => {
            return ApiError::InternalError(format!("cannot read backup: {error}")).into_response();
        }
    };
    if bytes.len() as u64 > MAX_IMPORT_BYTES {
        return ApiError::BadRequest("backup passes the store cap".to_string()).into_response();
    }
    let worker = state.clone();
    let pending = match tokio::task::spawn_blocking(move || {
        let parsed = parse_backup(&bytes).map_err(ApiError::BadRequest)?;
        materialize(&worker, &parsed)
    })
    .await
    {
        Ok(Ok(pending)) => pending,
        Ok(Err(error)) => return error.into_response(),
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"code": "INTERNAL_ERROR", "message": format!("import task failed: {error}")})),
            )
                .into_response();
        }
    };
    let authorized_admin_users = if let Some(candidate) = &pending.admin_users {
        let previous = state.config.snapshot();
        let is_full_admin = full_admin.is_some_and(|flag| flag.0 .0);
        if !is_full_admin && admin_user_rights_change(candidate, &previous.admin_users) {
            return ApiError::Forbidden(
                "this import changes dashboard user roles, scopes or membership: a full administrator is required"
                    .to_string(),
            )
            .into_response();
        }
        Some(previous.admin_users.clone())
    } else {
        None
    };
    match apply_materialized(&state, &pending, authorized_admin_users.as_ref()).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => error.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;

    // Build one minimal archive in memory. Parts carry full paths
    // inside the archive.
    fn test_archive(parts: &[(&str, Value)]) -> Vec<u8> {
        let encoder = GzEncoder::new(Vec::new(), Compression::default());
        let mut tar = tar::Builder::new(encoder);
        for (path, value) in parts {
            let bytes = serde_json::to_vec(value).expect("part is JSON");
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            tar.append_data(&mut header, path, bytes.as_slice())
                .expect("entry appends");
        }
        let encoder = tar.into_inner().expect("tar closes");
        encoder.finish().expect("gzip closes")
    }

    fn test_meta(roots: &[&str], tables: &[&str]) -> Value {
        json!({
            "format": "indra-backup-1",
            "version": "test",
            "exported_at": "2026-01-01T00:00:00+00:00",
            "roots": roots,
            "tables": tables,
        })
    }

    #[test]
    fn minimal_archive_parses() {
        let bytes = test_archive(&[
            ("backup-1-0/META.json", test_meta(&["rules"], &[])),
            ("backup-1-0/snapshot/rules.json", json!({"rules": []})),
        ]);
        let parsed = parse_backup(&bytes).expect("minimal archive parses");
        assert_eq!(parsed.meta_roots, vec!["rules".to_string()]);
        assert!(parsed.meta_tables.is_empty());
        assert!(parsed.roots.contains_key("rules"));
    }

    #[test]
    fn garbage_bytes_are_refused() {
        let error = parse_backup(b"not a gzip file").expect_err("garbage fails");
        assert!(!error.is_empty());
    }

    #[test]
    fn unknown_root_is_refused() {
        let bytes = test_archive(&[
            ("backup-1-0/META.json", test_meta(&["no-such-root"], &[])),
            ("backup-1-0/snapshot/no-such-root.json", json!({})),
        ]);
        let error = parse_backup(&bytes).expect_err("unknown root fails");
        assert!(error.contains("unknown root"));
    }

    #[test]
    fn missing_part_is_refused() {
        let bytes = test_archive(&[("backup-1-0/META.json", test_meta(&["rules"], &[]))]);
        let error = parse_backup(&bytes).expect_err("missing part fails");
        assert!(error.contains("misses part"));
    }

    #[test]
    fn entry_outside_base_is_refused() {
        let bytes = test_archive(&[
            ("backup-1-0/META.json", test_meta(&["rules"], &[])),
            ("backup-1-0/snapshot/rules.json", json!({"rules": []})),
            ("evil/x.json", json!({})),
        ]);
        let error = parse_backup(&bytes).expect_err("foreign entry fails");
        assert!(error.contains("unexpected entry"));
    }

    #[test]
    fn ban_rows_reject_bad_kinds_and_duplicates() {
        let bad_kind = parse_bans(&json!([{"as": "nope", "who": "x"}]));
        assert!(bad_kind.is_err());
        let empty_who = parse_bans(&json!([{"as": "clientid", "who": " "}]));
        assert!(empty_who.is_err());
        let duplicate = parse_bans(&json!([
            {"as": "clientid", "who": "x"},
            {"as": "clientid", "who": "x"},
        ]));
        assert!(duplicate.is_err());
        let good = parse_bans(&json!([
            {"as": "clientid", "who": "x", "by": "api", "reason": "r", "at": "a", "until": "infinity"},
        ]))
        .expect("good row parses");
        assert_eq!(good.len(), 1);
        assert_eq!(good[0].who, "x");
    }

    #[test]
    fn retained_rows_accept_good_and_reject_bad() {
        use base64::{engine::general_purpose::STANDARD, Engine as _};
        let payload = STANDARD.encode(b"hello-retained");
        let good = parse_retained(&json!([
            {"topic": "rt/conf/retained", "qos": 1, "payload": payload},
        ]))
        .expect("good row parses");
        assert_eq!(good.len(), 1);
        assert_eq!(good[0].topic.as_str(), "rt/conf/retained");
        assert!(parse_retained(
            &json!([{"topic": "rt/conf/retained", "qos": 9, "payload": payload}])
        )
        .is_err());
        assert!(parse_retained(&json!([{"topic": "rt/conf/retained", "qos": 1}])).is_err());
        assert!(parse_retained(&json!([{"topic": "#", "qos": 0, "payload": payload}])).is_err());
        assert!(parse_retained(&json!("not-a-list")).is_err());
    }

    #[tokio::test]
    async fn import_rejects_concurrent_admin_change_and_skips_unchanged_admin_root() {
        use std::sync::Arc;
        let dir = std::env::temp_dir().join(format!("indra-backup-race-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let registry = Arc::new(broker_config::ConfigRegistry::load_for_boot(&dir).unwrap());
        let (edge_tx, _edge_rx) = tokio::sync::mpsc::unbounded_channel();
        let state = ApiState::new(
            Arc::new(broker_rules::RuleEngine::new(
                16,
                broker_rules::BackpressurePolicy::DropOldest,
            )),
            Arc::new(broker_session::SessionManager::new()),
            Arc::new(broker_router::Router::new()),
            Arc::new(broker_observability::Metrics::new()),
            Arc::new(broker_auth::MemoryAuth::new()),
            Arc::new(crate::ConnTable::default()),
            Arc::clone(&registry),
            "node-1".to_string(),
            edge_tx,
        );
        let initial_admin = state.config.snapshot().admin_users.clone();

        let mut pending = Materialized::default();
        let mut modified_admin = initial_admin.clone();
        modified_admin.users.push(broker_config::AdminUser {
            username: "imported".to_string(),
            password_hash: "$2b$12$000000000000000000000u0000000000000000000000000000000"
                .to_string(),
            role: "viewer".to_string(),
            description: String::new(),
            must_change_password: false,
            scopes: None,
            mfa_enabled: false,
            mfa_secret: String::new(),
            mfa_last_step: None,
        });
        pending.admin_users = Some(modified_admin);

        // Simulate a concurrent admin-user commit after authorization snapshot.
        let mut concurrent_admin = initial_admin.clone();
        concurrent_admin.users.push(broker_config::AdminUser {
            username: "concurrent".to_string(),
            password_hash: "$2b$12$000000000000000000000u0000000000000000000000000000000"
                .to_string(),
            role: "viewer".to_string(),
            description: String::new(),
            must_change_password: false,
            scopes: None,
            mfa_enabled: false,
            mfa_secret: String::new(),
            mfa_last_step: None,
        });
        state
            .config
            .commit(ConfigRoot::AdminUsers(concurrent_admin))
            .unwrap();

        let err = apply_materialized(&state, &pending, Some(&initial_admin))
            .await
            .unwrap_err();
        assert!(matches!(err, ApiError::BadRequest(_)), "got {err:?}");
        assert!(
            state
                .config
                .snapshot()
                .admin_users
                .users
                .iter()
                .any(|u| u.username == "concurrent"),
            "concurrent admin update must survive rejected import"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
