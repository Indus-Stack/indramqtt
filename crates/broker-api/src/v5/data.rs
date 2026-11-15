//! Backup files for the v5 REST API.
//!
//! `POST /api/v5/data/export` writes one backup file. The file holds a
//! redacted config snapshot plus live table rows. The file excludes
//! secrets: each secret field shows its owner reference only.
//! `GET /api/v5/data/files` lists the stored files with paging.
//! `POST /api/v5/data/files` stores one uploaded file.
//! `DELETE /api/v5/data/files/{filename}` removes one stored file.
//! `GET /api/v5/data/files/{filename}` serves one stored file.
//! Files collect in `<data-dir>/backup`. The store keeps at most
//! `MAX_EXPORT_FILES` files and `MAX_EXPORT_BYTES` bytes, and it drops
//! files older than `RETENTION_DAYS` days. The store deletes the oldest
//! files first.
//! Files live on disk, so they survive a restart. The handlers read one
//! lock-free registry snapshot plus short management locks. They read the
//! ban store under its write lock. The CONNECT check and the publish
//! check use the same ban store. A blocking thread builds the archive.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use broker_config::FullSnapshot;
use bytes::Bytes;
use flate2::{write::GzEncoder, Compression};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tar::Builder;

use crate::errors::ApiError;
use crate::node_scope::resolve_node;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

// Max export files kept on disk. Reason: the spec caps the file store.
const MAX_EXPORT_FILES: usize = 16;
// Max bytes kept across export files. Reason: the spec caps disk use.
const MAX_EXPORT_BYTES: u64 = 256 * 1024 * 1024;
// Max file age in days. Reason: the spec sets 7 days default retention.
const RETENTION_DAYS: u64 = 7;
// Retention in seconds, derived from the day bound above.
const RETENTION_SECS: u64 = RETENTION_DAYS * 86_400;
// Download content type. Reason: the spec serves raw file bytes.
const BACKUP_DOWNLOAD_CONTENT_TYPE: &str = "application/octet-stream";
// Legacy node name. Reason: v5 rows render this name today.
use crate::node_scope::local_node_name;
// Longest accepted file name. Reason: the filesystem caps one name.
const MAX_FILENAME_LEN: usize = 255;

// Config snapshot roots the export serves.
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

// Live tables the export serves. Each name maps to a live store read.
const KNOWN_TABLES: &[&str] = &["banned", "builtin_authn", "builtin_authz"];

// Export counter. It keeps file names unique within one second.
static EXPORT_COUNTER: AtomicU64 = AtomicU64::new(0);

// Request body for `POST /api/v5/data/export`. Empty lists select
// every documented root and table.
#[derive(Debug, Default, Deserialize)]
struct ExportRequest {
    #[serde(default)]
    root_keys: Vec<String>,
    #[serde(default)]
    table_sets: Vec<String>,
}

// Current epoch seconds. It returns zero when the clock fails.
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|span| span.as_secs())
        .unwrap_or(0)
}

// Unique backup file name. It sorts in creation order. The counter has a
// fixed width, so 9 sorts before 10 in a text compare.
fn backup_filename() -> String {
    let slot = EXPORT_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("backup-{}-{slot:010}.tar.gz", now_secs())
}

// Split the request into (roots, tables). Unknown names return as
// invalid. Empty lists select every documented name.
fn resolve_selection(req: &ExportRequest) -> Result<(Vec<&str>, Vec<&str>), Vec<String>> {
    let mut invalid = Vec::new();
    for name in &req.root_keys {
        if !KNOWN_ROOTS.contains(&name.as_str()) {
            invalid.push(name.clone());
        }
    }
    for name in &req.table_sets {
        if !KNOWN_TABLES.contains(&name.as_str()) {
            invalid.push(name.clone());
        }
    }
    if !invalid.is_empty() {
        return Err(invalid);
    }
    let roots = if req.root_keys.is_empty() {
        KNOWN_ROOTS.to_vec()
    } else {
        req.root_keys.iter().map(String::as_str).collect()
    };
    let tables = if req.table_sets.is_empty() {
        KNOWN_TABLES.to_vec()
    } else {
        req.table_sets.iter().map(String::as_str).collect()
    };
    Ok((roots, tables))
}

// Redacted JSON for one config root. Secret fields stay out; the owner
// reference stays in. Opaque `config` blobs may hold passwords, so the
// export keeps the entry id and kind only.
fn redacted_root(name: &str, snapshot: &FullSnapshot) -> Value {
    match name {
        "admin_users" => json!({
            "users": snapshot.admin_users.users.iter().map(|user| json!({
                "username": user.username,
                "role": user.role,
                "description": user.description,
                "must_change_password": user.must_change_password,
                "mfa_enabled": user.mfa_enabled,
                "scopes": user.scopes,
                "scopes_set": user.scopes.is_some(),
            })).collect::<Vec<_>>(),
        }),
        "mqtt_users" => json!({
            "users": snapshot.mqtt_users.users.iter().map(|user| json!({
                "username": user.username,
                "max_connections": user.max_connections,
                "max_publish_rate": user.max_publish_rate,
                "max_publish_burst": user.max_publish_burst,
            })).collect::<Vec<_>>(),
            "acls": snapshot.mqtt_users.acls,
            // Every tenant ships (TM-01): usernames and quotas carry no
            // secrets, so the redacted export keeps them all.
            "tenants": snapshot.mqtt_users.tenants.iter().map(|table| json!({
                "tenant": table.tenant,
                "users": table.users.iter().map(|user| json!({
                    "username": user.username,
                    "max_connections": user.max_connections,
                    "max_publish_rate": user.max_publish_rate,
                    "max_publish_burst": user.max_publish_burst,
                })).collect::<Vec<_>>(),
                "acls": table.acls,
            })).collect::<Vec<_>>(),
        }),
        "rules" => json!(snapshot.rules),
        "connectors" => json!({
            "connectors": snapshot.connectors.connectors.iter().map(|entry| json!({
                "id": entry.id,
                "connector_type": entry.connector_type,
                "enable": entry.enable,
            })).collect::<Vec<_>>(),
        }),
        "authn_chain" => json!({
            "authenticators": snapshot.authn_chain.authenticators.iter().map(|entry| json!({
                "id": entry.id,
                "mechanism": entry.mechanism,
                "backend": entry.backend,
                "enable": entry.enable,
            })).collect::<Vec<_>>(),
        }),
        "authn_node_cache" => json!(snapshot.authn_node_cache),
        "authn_settings" => json!(snapshot.authn_settings),
        "authz_node_cache" => json!(snapshot.authz_node_cache),
        "authz_settings" => json!(snapshot.authz_settings),
        "authz_sources" => json!({
            "sources": snapshot.authz_sources.sources.iter().map(|entry| json!({
                "type": entry.source_type,
                "enable": entry.enable,
            })).collect::<Vec<_>>(),
        }),
        "api_keys" => json!({
            "keys": snapshot.api_keys.keys.iter().map(|entry| json!({
                "name": entry.name,
                "key_id": entry.key_id,
                "scopes": entry.scopes,
                "scopes_set": entry.scopes_set,
                "description": entry.description,
                "created_at": entry.created_at,
                "enable": entry.enable,
                "expired_at": entry.expired_at,
                "role": entry.role,
            })).collect::<Vec<_>>(),
        }),
        "sso_backends" => json!({
            "backends": snapshot.sso_backends.backends.iter().map(|entry| json!({
                "backend": entry.backend,
                "enable": entry.enable,
            })).collect::<Vec<_>>(),
        }),
        "actions" => json!({
            "actions": snapshot.actions.actions.iter().map(|entry| json!({
                "id": entry.id,
                "action_type": entry.action_type,
                "enable": entry.enable,
            })).collect::<Vec<_>>(),
        }),
        "sources" => json!({
            "sources": snapshot.sources.sources.iter().map(|entry| json!({
                "id": entry.id,
                "source_type": entry.source_type,
                "enable": entry.enable,
            })).collect::<Vec<_>>(),
        }),
        "rule_engine" => json!(snapshot.rule_engine),
        "transformations" => json!({
            "transformations": snapshot.transformations.transformations.iter().map(|entry| json!({
                "name": entry.name,
                "topics": entry.topics,
                "failure_action": entry.failure_action,
                "enable": entry.enable,
                "operations": entry.operations.iter().map(|op| json!({
                    "key": op.key,
                    "value": op.value,
                })).collect::<Vec<_>>(),
                "config": entry.config,
            })).collect::<Vec<_>>(),
        }),
        _ => Value::Null,
    }
}

// Live rows for one table. Each arm reads its live store, so a table
// with no live store behind it cannot be served.
fn live_table(name: &str, state: &ApiState) -> Value {
    match name {
        "banned" => json!(state
            .bans
            .list_live()
            .iter()
            .map(|entry| json!({
                "as": entry.as_type,
                "who": entry.who,
                "by": entry.by,
                "reason": entry.reason,
                "at": entry.at,
                "until": entry.until,
            }))
            .collect::<Vec<_>>()),
        "builtin_authn" => json!(state
            .auth
            .all_users_with_tenants()
            .iter()
            .map(|(tenant, username)| json!({ "tenant": tenant, "username": username }))
            .collect::<Vec<_>>()),
        "builtin_authz" => json!(state
            .auth
            .all_rules_with_tenants()
            .iter()
            .map(|(tenant, rule)| {
                let mut value = serde_json::to_value(rule).unwrap_or(serde_json::json!({}));
                if let Some(map) = value.as_object_mut() {
                    map.insert("tenant".to_string(), serde_json::json!(tenant));
                }
                value
            })
            .collect::<Vec<_>>()),
        _ => Value::Null,
    }
}

// One export payload: meta plus snapshot roots plus live tables.
struct ExportPayload {
    base: String,
    meta: Value,
    snapshot: Vec<(String, Value)>,
    tables: Vec<(String, Value)>,
}

// Build the tar.gz bytes. Runs on a blocking thread, off the async
// workers and off the publish path.
fn build_archive(payload: &ExportPayload) -> std::io::Result<Vec<u8>> {
    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut tar = Builder::new(encoder);
    let mut serialise = |path: String, value: &Value| -> std::io::Result<()> {
        let bytes = serde_json::to_vec_pretty(value).map_err(std::io::Error::other)?;
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(now_secs());
        tar.append_data(&mut header, path, bytes.as_slice())?;
        Ok(())
    };
    serialise(format!("{}/META.json", payload.base), &payload.meta)?;
    for (name, value) in &payload.snapshot {
        serialise(format!("{}/snapshot/{name}.json", payload.base), value)?;
    }
    for (name, value) in &payload.tables {
        serialise(format!("{}/tables/{name}.json", payload.base), value)?;
    }
    let encoder = tar.into_inner()?;
    encoder.finish()
}

// One file entry seen in the backup dir.
#[derive(Clone)]
struct BackupFile {
    name: String,
    size: u64,
    modified_secs: u64,
}

// Scan the backup dir. Missing dir reads as empty, never an error.
async fn scan_backups(dir: &std::path::Path) -> Vec<BackupFile> {
    let mut out = Vec::new();
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return out;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".tar.gz") {
            continue;
        }
        let Ok(meta) = entry.metadata().await else {
            continue;
        };
        if !meta.is_file() {
            continue;
        }
        let modified_secs = meta
            .modified()
            .ok()
            .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
            .map(|span| span.as_secs())
            .unwrap_or(0);
        out.push(BackupFile {
            name,
            size: meta.len(),
            modified_secs,
        });
    }
    out.sort_by_key(|file| file.modified_secs);
    out
}

// Drop oldest files until count, bytes and age fit the bounds.
async fn enforce_bounds(dir: &std::path::Path) {
    let mut files = scan_backups(dir).await;
    let cutoff = now_secs().saturating_sub(RETENTION_SECS);
    let mut live: Vec<BackupFile> = Vec::with_capacity(files.len());
    for file in files.drain(..) {
        if file.modified_secs < cutoff {
            tokio::fs::remove_file(dir.join(&file.name)).await.ok();
        } else {
            live.push(file);
        }
    }
    while live.len() > MAX_EXPORT_FILES {
        let oldest = live.remove(0);
        tokio::fs::remove_file(dir.join(&oldest.name)).await.ok();
    }
    let mut total: u64 = live.iter().map(|file| file.size).sum();
    while total > MAX_EXPORT_BYTES {
        if live.is_empty() {
            break;
        }
        let oldest = live.remove(0);
        total = total.saturating_sub(oldest.size);
        tokio::fs::remove_file(dir.join(&oldest.name)).await.ok();
    }
}

// Backup dir for this node. Files persist across restarts there.
pub(crate) fn backup_dir(state: &ApiState) -> PathBuf {
    state.config.dir().join("backup")
}

// `POST /api/v5/data/export`: write one backup file and report its fields.
// Answers the documented file fields: `created_at`, `filename`, `node`
// plus the true file `size`. An empty body selects every root and table.
pub async fn export_data(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: Value = if body.is_empty() {
        Value::Object(Default::default())
    } else {
        match serde_json::from_slice(&body) {
            Ok(value) => value,
            Err(_) => {
                return ApiError::BadRequest("request body must be JSON".to_string())
                    .into_response()
            }
        }
    };
    let req: ExportRequest = match serde_json::from_value(value) {
        Ok(req) => req,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid export body: {error}")).into_response();
        }
    };
    let (roots, tables) = match resolve_selection(&req) {
        Ok(selection) => selection,
        Err(invalid) => {
            return ApiError::BadRequest(format!(
                "unknown tables or roots: {}",
                invalid.join(", ")
            ))
            .into_response();
        }
    };

    // One lock-free snapshot load plus short live-store reads. The ban
    // read holds the ban write lock briefly.
    let snapshot = state.config.snapshot();
    let snapshot_parts: Vec<(String, Value)> = roots
        .iter()
        .map(|name| (name.to_string(), redacted_root(name, &snapshot)))
        .collect();
    let table_parts: Vec<(String, Value)> = tables
        .iter()
        .map(|name| (name.to_string(), live_table(name, &state)))
        .collect();

    let filename = backup_filename();
    let base = filename.trim_end_matches(".tar.gz").to_string();
    let payload = ExportPayload {
        base: base.clone(),
        meta: json!({
            "format": "indra-backup-1",
            "version": env!("CARGO_PKG_VERSION"),
            "exported_at": chrono::Utc::now().to_rfc3339(),
            "roots": roots,
            "tables": tables,
        }),
        snapshot: snapshot_parts,
        tables: table_parts,
    };
    let bytes = match tokio::task::spawn_blocking(move || build_archive(&payload)).await {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(
                    json!({"code": "EXPORT_FAILED", "message": format!("export failed: {error}")}),
                ),
            )
                .into_response();
        }
        Err(error) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"code": "EXPORT_FAILED", "message": format!("export task failed: {error}")})),
            )
                .into_response();
        }
    };

    // Write to a temp file first, then rename. Readers never see a
    // half-written backup.
    let dir = backup_dir(&state);
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot use backup dir: {error}")})),
        )
            .into_response();
    }
    let tmp = dir.join(format!("{base}.part"));
    if let Err(error) = tokio::fs::write(&tmp, &bytes).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot write backup: {error}")})),
        )
            .into_response();
    }
    if let Err(error) = tokio::fs::rename(&tmp, dir.join(&filename)).await {
        tokio::fs::remove_file(&tmp).await.ok();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot publish backup: {error}")})),
        )
            .into_response();
    }
    enforce_bounds(&dir).await;

    // True file facts. The time is now, the size is the written bytes,
    // the node is this process. No field is a stand-in.
    let size = bytes.len() as u64;
    let created_at = chrono::Utc::now().to_rfc3339();
    let node = local_node_name().to_string();
    (
        StatusCode::OK,
        Json(json!({
            "created_at": created_at,
            "filename": filename,
            "node": node,
            "size": size,
        })),
    )
        .into_response()
}

// Accept one uploaded file name. Reject path parts and bad suffix.
pub(crate) fn valid_backup_filename(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_FILENAME_LEN {
        return false;
    }
    if !name.ends_with(".tar.gz") {
        return false;
    }
    if name.contains('/') || name.contains('\\') {
        return false;
    }
    if name.contains("..") {
        return false;
    }
    if name.starts_with('.') {
        return false;
    }
    // Only letters, digits, `.`, `_` and `-`. The name goes into a file
    // path and into the `content-disposition` header of the download, and
    // a quote mark or a control character must not get into the two.
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return false;
    }
    true
}

// Format one epoch time as RFC 3339. Use epoch on bad input.
fn created_at_rfc3339(secs: u64) -> String {
    match chrono::DateTime::from_timestamp(secs as i64, 0) {
        Some(time) => time.with_timezone(&chrono::Utc).to_rfc3339(),
        None => chrono::DateTime::from_timestamp(0, 0)
            .expect("epoch exists")
            .with_timezone(&chrono::Utc)
            .to_rfc3339(),
    }
}

// `GET /api/v5/data/files`: paged list of stored backup files.
//
// Serve the real files from the backup dir. Sort by age, oldest
// first. An empty dir reads as an empty page, never an error.
// Management-plane only; publish and deliver never touch it.
pub async fn list_backup_files(State(state): State<ApiState>, params: PageParams) -> Response {
    let dir = backup_dir(&state);
    let files = scan_backups(&dir).await;
    let total = files.len();
    let page_items = paginate(&files, params.page, params.limit);
    let data: Vec<Value> = page_items
        .iter()
        .map(|file| {
            json!({
                "filename": file.name,
                "size": file.size,
                "created_at": created_at_rfc3339(file.modified_secs),
                "node": state.node_id,
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

// `POST /api/v5/data/files`: store one uploaded backup file.
//
// Take JSON `{filename, content}` where `content` holds base64
// bytes. Write through a temp file, then rename. A known name
// fails with `ALREADY_EXISTS`. Bad names or bad bytes fail with
// `BAD_REQUEST`. Management-plane only; publish never touches it.
pub async fn upload_backup_file(State(state): State<ApiState>, body: Bytes) -> Response {
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
    let encoded = value
        .get("content")
        .or_else(|| value.get("file"))
        .and_then(Value::as_str)
        .unwrap_or("");
    if !valid_backup_filename(&filename) {
        return ApiError::BadRequest("filename must end with .tar.gz".to_string()).into_response();
    }
    if encoded.is_empty() {
        return ApiError::BadRequest("content must not be empty".to_string()).into_response();
    }
    use base64::Engine as _;
    let bytes = match base64::engine::general_purpose::STANDARD.decode(encoded.trim()) {
        Ok(bytes) => bytes,
        Err(_) => {
            return ApiError::BadRequest("content must be base64".to_string()).into_response();
        }
    };
    if bytes.is_empty() {
        return ApiError::BadRequest("content must not be empty".to_string()).into_response();
    }
    // One file cannot pass the store cap. Reason: the spec caps
    // total disk use, so one file above it never fits.
    if bytes.len() as u64 > MAX_EXPORT_BYTES {
        return ApiError::BadRequest("content passes the store cap".to_string()).into_response();
    }
    let dir = backup_dir(&state);
    if let Err(error) = tokio::fs::create_dir_all(&dir).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot use backup dir: {error}")})),
        )
            .into_response();
    }
    let target = dir.join(&filename);
    if tokio::fs::metadata(&target).await.is_ok() {
        return ApiError::AlreadyExists(format!("backup file {filename:?} already exists"))
            .into_response();
    }
    // Write to a temp file first, then rename. Readers never see a
    // half-written file.
    let tmp = dir.join(format!("{filename}.part"));
    if let Err(error) = tokio::fs::write(&tmp, &bytes).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot write backup: {error}")})),
        )
            .into_response();
    }
    if let Err(error) = tokio::fs::rename(&tmp, &target).await {
        tokio::fs::remove_file(&tmp).await.ok();
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot publish backup: {error}")})),
        )
            .into_response();
    }
    enforce_bounds(&dir).await;
    StatusCode::NO_CONTENT.into_response()
}

// `DELETE /api/v5/data/files/{filename}`: remove one stored file.
//
// A known name deletes and answers 204. An unknown name answers
// 404 with the documented shape. A bad name answers 400.
// Management-plane only; publish and deliver never touch it.
pub async fn delete_backup_file(
    State(state): State<ApiState>,
    Path(filename): Path<String>,
) -> Response {
    if !valid_backup_filename(&filename) {
        return ApiError::BadRequest("filename must end with .tar.gz".to_string()).into_response();
    }
    let path = backup_dir(&state).join(&filename);
    match tokio::fs::remove_file(&path).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            ApiError::NotFound(format!("backup file {filename:?} not found")).into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot delete backup: {error}")})),
        )
            .into_response(),
    }
}

// `GET /api/v5/data/files/{filename}`: serve one stored file.
//
// Read the same backup dir the export, upload and delete routes use.
// A known name returns its bytes with the download content type.
// An unknown name returns 404 `NOT_FOUND`. A bad name returns 400
// `BAD_REQUEST`. An unknown `node` query value returns 404 `NOT_FOUND`.
//
// Management-plane only; publish and deliver never touch it. One
// bounded read serves one request; the file stays below the store cap.
pub async fn download_backup_file(
    State(state): State<ApiState>,
    Path(filename): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    if let Some(node) = query.get("node") {
        if resolve_node(&state.node_id, node).is_err()
            && resolve_node(local_node_name(), node).is_err()
        {
            return resolve_node(&state.node_id, node)
                .expect_err("node already checked as unknown")
                .into_response();
        }
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
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"code": "EXPORT_FAILED", "message": format!("cannot read backup: {error}")})),
            )
                .into_response();
        }
    };
    let disposition = format!("attachment; filename=\"{filename}\"");
    (
        StatusCode::OK,
        [
            ("content-type", BACKUP_DOWNLOAD_CONTENT_TYPE.to_string()),
            ("content-disposition", disposition),
        ],
        bytes,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_selection_selects_everything() {
        let req = ExportRequest {
            root_keys: Vec::new(),
            table_sets: Vec::new(),
        };
        let (roots, tables) = resolve_selection(&req).expect("empty selects all");
        assert_eq!(roots.len(), KNOWN_ROOTS.len());
        assert_eq!(tables, KNOWN_TABLES);
    }

    #[test]
    fn unknown_names_are_invalid() {
        let req = ExportRequest {
            root_keys: vec!["no-such-root".to_string()],
            table_sets: vec!["no-such-table".to_string()],
        };
        let invalid = resolve_selection(&req).expect_err("unknown names fail");
        assert_eq!(invalid.len(), 2);
    }

    #[test]
    fn backup_names_are_unique_and_sorted() {
        let first = backup_filename();
        let second = backup_filename();
        assert!(first.starts_with("backup-"));
        assert!(first.ends_with(".tar.gz"));
        assert_ne!(first, second);
        assert!(first < second);
    }

    // Secrets stay out of every redacted root. Each secret field holds
    // the same sentinel, so one check covers all of them.
    #[test]
    fn redaction_drops_every_secret() {
        const SENTINEL: &str = "SENTINEL-SECRET-9f31";
        let mut snapshot = FullSnapshot::default();
        snapshot.admin_users.users.push(broker_config::AdminUser {
            username: "root".to_string(),
            password_hash: SENTINEL.to_string(),
            role: "administrator".to_string(),
            description: String::new(),
            must_change_password: false,
            mfa_secret: SENTINEL.to_string(),
            mfa_enabled: false,
            mfa_last_step: None,
            scopes: None,
        });
        snapshot.mqtt_users.users.push(broker_config::MqttUser {
            username: "sensor".to_string(),
            password_hash: SENTINEL.to_string(),
            max_connections: None,
            max_publish_rate: None,
            max_publish_burst: None,
        });
        snapshot
            .connectors
            .connectors
            .push(broker_config::ConnectorEntry {
                id: "pg".to_string(),
                connector_type: "pgsql".to_string(),
                enable: true,
                config: SENTINEL.to_string(),
            });
        snapshot
            .authn_chain
            .authenticators
            .push(broker_config::AuthnEntry {
                id: "a".to_string(),
                mechanism: "password_based".to_string(),
                backend: "built_in_database".to_string(),
                enable: true,
                config: SENTINEL.to_string(),
            });
        snapshot
            .authz_sources
            .sources
            .push(broker_config::AuthzSourceEntry {
                source_type: "file".to_string(),
                enable: true,
                config: SENTINEL.to_string(),
            });
        snapshot.api_keys.keys.push(broker_config::ApiKeyEntry {
            name: "key-1".to_string(),
            key_hash: SENTINEL.to_string(),
            key_id: "0123456789abcdef".to_string(),
            scopes: vec!["audit".to_string()],
            scopes_set: true,
            description: String::new(),
            created_at: String::new(),
            enable: false,
            expired_at: Some(4_102_444_800),
            role: "viewer".to_string(),
        });
        snapshot
            .sso_backends
            .backends
            .push(broker_config::SsoBackendEntry {
                backend: "sso-1".to_string(),
                enable: true,
                config: SENTINEL.to_string(),
            });
        snapshot.actions.actions.push(broker_config::ActionEntry {
            id: "act-1".to_string(),
            action_type: "log".to_string(),
            enable: true,
            config: SENTINEL.to_string(),
        });
        snapshot.sources.sources.push(broker_config::SourceEntry {
            id: "src-1".to_string(),
            source_type: "mqtt".to_string(),
            enable: true,
            config: SENTINEL.to_string(),
        });

        for name in KNOWN_ROOTS {
            let value = redacted_root(name, &snapshot);
            let text = serde_json::to_string(&value).expect("redacted root is JSON");
            assert!(!text.contains(SENTINEL), "root {name} leaks a secret");
        }

        // References stay in: owner names survive the redaction.
        let admins = redacted_root("admin_users", &snapshot);
        assert!(admins.to_string().contains("root"));
        let users = redacted_root("mqtt_users", &snapshot);
        assert!(users.to_string().contains("sensor"));
        let connectors = redacted_root("connectors", &snapshot);
        assert!(connectors.to_string().contains("pg"));

        // The non-secret fields of an API key stay in; the hash does not.
        let keys = redacted_root("api_keys", &snapshot);
        let row = &keys["keys"][0];
        assert_eq!(row["name"], json!("key-1"));
        assert_eq!(row["key_id"], json!("0123456789abcdef"));
        assert_eq!(row["scopes"], json!(["audit"]));
        assert_eq!(row["scopes_set"], json!(true));
        assert_eq!(row["enable"], json!(false));
        assert_eq!(row["expired_at"], json!(4_102_444_800u64));
        assert_eq!(row["role"], json!("viewer"));
        assert!(row.get("key_hash").is_none());
    }

    // The archive holds every payload part under the backup base dir.
    #[test]
    fn archive_round_trip_keeps_parts() {
        let payload = ExportPayload {
            base: "backup-1-0".to_string(),
            meta: json!({"format": "indra-backup-1"}),
            snapshot: vec![("rules".to_string(), json!({"rules": []}))],
            tables: vec![("banned".to_string(), json!([]))],
        };
        let bytes = build_archive(&payload).expect("archive builds");
        assert!(!bytes.is_empty());

        let gz = flate2::read::GzDecoder::new(bytes.as_slice());
        let mut archive = tar::Archive::new(gz);
        let mut names = Vec::new();
        for entry in archive.entries().expect("archive reads") {
            let entry = entry.expect("entry reads");
            names.push(
                entry
                    .path()
                    .expect("entry path")
                    .to_string_lossy()
                    .into_owned(),
            );
        }
        names.sort();
        assert_eq!(
            names,
            vec![
                "backup-1-0/META.json",
                "backup-1-0/snapshot/rules.json",
                "backup-1-0/tables/banned.json",
            ]
        );
    }

    // Unique scratch dir for bound tests. It uses the process id plus
    // an atomic slot, so parallel tests never share a dir.
    fn bound_test_dir(prefix: &str) -> std::path::PathBuf {
        static SLOT: AtomicU64 = AtomicU64::new(0);
        let slot = SLOT.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "indramqtt-w526-{prefix}-{}-{slot}",
            std::process::id()
        ))
    }

    // Write one dummy backup file with the given size and age.
    // Age is seconds before now. Size uses sparse extension.
    fn write_dummy_file(dir: &std::path::Path, name: &str, size: u64, age_secs: u64) {
        use std::time::{Duration, SystemTime};
        let path = dir.join(name);
        std::fs::write(&path, b"dummy").expect("dummy file writes");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .expect("dummy file opens");
        file.set_len(size).expect("dummy size extends");
        let mtime = SystemTime::now()
            .checked_sub(Duration::from_secs(age_secs))
            .expect("age precedes now");
        file.set_modified(mtime).expect("mtime applies");
    }

    // The file-count bound deletes the oldest files first. It keeps at
    // most MAX_EXPORT_FILES files.
    #[tokio::test]
    async fn bounds_enforce_file_count() {
        let dir = bound_test_dir("count");
        std::fs::create_dir_all(&dir).expect("scratch dir forms");
        for index in 0..(MAX_EXPORT_FILES + 2) {
            let age = (MAX_EXPORT_FILES + 2 - index) as u64;
            write_dummy_file(&dir, &format!("backup-count-{index:03}.tar.gz"), 8, age);
        }
        enforce_bounds(&dir).await;
        let kept = scan_backups(&dir).await;
        assert!(kept.len() <= MAX_EXPORT_FILES);
        assert!(!kept
            .iter()
            .any(|file| file.name == "backup-count-000.tar.gz"));
        assert!(!kept
            .iter()
            .any(|file| file.name == "backup-count-001.tar.gz"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // The byte bound deletes the oldest files first. It keeps total
    // bytes at or below MAX_EXPORT_BYTES.
    #[tokio::test]
    async fn bounds_enforce_total_bytes() {
        let dir = bound_test_dir("bytes");
        std::fs::create_dir_all(&dir).expect("scratch dir forms");
        let big = MAX_EXPORT_BYTES / 2 + 8 * 1024 * 1024;
        write_dummy_file(&dir, "backup-bytes-000.tar.gz", big, 30);
        write_dummy_file(&dir, "backup-bytes-001.tar.gz", big, 20);
        write_dummy_file(&dir, "backup-bytes-002.tar.gz", 8, 10);
        enforce_bounds(&dir).await;
        let kept = scan_backups(&dir).await;
        let total: u64 = kept.iter().map(|file| file.size).sum();
        assert!(total <= MAX_EXPORT_BYTES);
        assert!(!kept
            .iter()
            .any(|file| file.name == "backup-bytes-000.tar.gz"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // The age bound deletes files older than retention. It keeps recent
    // files.
    #[tokio::test]
    async fn bounds_enforce_retention_age() {
        let dir = bound_test_dir("age");
        std::fs::create_dir_all(&dir).expect("scratch dir forms");
        let old_age = RETENTION_SECS + 3_600;
        write_dummy_file(&dir, "backup-old-000.tar.gz", 8, old_age);
        write_dummy_file(&dir, "backup-new-000.tar.gz", 8, 10);
        enforce_bounds(&dir).await;
        let kept = scan_backups(&dir).await;
        assert!(!kept.iter().any(|file| file.name == "backup-old-000.tar.gz"));
        assert!(kept.iter().any(|file| file.name == "backup-new-000.tar.gz"));
        std::fs::remove_dir_all(&dir).ok();
    }

    // The export ban read keeps ban checks usable. It reads under the
    // write lock, and the CONNECT check plus the publish check use the
    // same store.
    #[test]
    fn ban_read_keeps_ban_checks_live() {
        use crate::v5::banned::{BanEntry, BanStore};
        let store = BanStore::new();
        store
            .insert(BanEntry {
                as_type: "clientid".to_string(),
                who: "w5-26-probe".to_string(),
                by: "api".to_string(),
                reason: "probe".to_string(),
                at: "2026-01-01T00:00:00+00:00".to_string(),
                until: "infinity".to_string(),
                tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
            })
            .expect("probe ban inserts");
        let live = store.list_live();
        assert!(live.iter().any(|entry| entry.who == "w5-26-probe"));
        assert!(store.is_banned("w5-26-probe", None, None));
        assert!(!store.is_banned("w5-26-other", None, None));
        let live_again = store.list_live();
        assert!(live_again.iter().any(|entry| entry.who == "w5-26-probe"));
        assert!(store.is_banned("w5-26-probe", None, None));
    }
}
