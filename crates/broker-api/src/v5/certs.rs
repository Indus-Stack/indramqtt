//! Managed certificate bundles for the v5 management API.
//!
//! Covers the eight `TLS Management` operations: list bundles, read one
//! bundle, upload files and delete a bundle or one file, each for the
//! global namespace and for one managed namespace. The shapes follow the
//! reference API: a list reads `[{"name"}]`, a read renders
//! `{kind: {"path"}}`, an upload answers 204 and a delete answers 204.
//!
//! Storage: files collect under `<data-dir>/certs/global/<bundle>/` and
//! `<data-dir>/certs/ns/<namespace>/<bundle>/`. Each kind has a fixed
//! file name chosen by the broker, never by the caller: `key.pem`,
//! `chain.pem`, `ca.pem`, `acc-key.pem` and `key-password`. Reads render
//! server paths only, never file contents. Key material (`key.pem`,
//! `acc-key.pem`, `key-password`) stores with file mode 600; the chain
//! and the CA store with mode 644.
//!
//! Validation: an upload is checked before anything is stored. Each file
//! must stay at or below [`MAX_CERT_FILE_BYTES`] bytes. The chain and
//! the CA must hold at least one parseable certificate. The key and the
//! account key must hold a private key. When the bundle then holds both
//! a key and a chain, the key must match the certificate. Unknown kinds,
//! empty bodies and bad names fail with `BAD_REQUEST`.
//!
//! Names hold letters, digits, `.`, `_` and `-` only, so a name is one
//! safe path segment. The broker never builds a path from caller text
//! beyond that segment.
//!
//! Store bounds (stated here and enforced below):
//! - at most [`MAX_CERT_BUNDLES`] bundles per namespace; creates past
//!   the cap fail with `BAD_REQUEST` instead of growing the directory
//!   without limit;
//! - at most [`MAX_CERT_NAMESPACES`] namespaces; uploads to a new name
//!   past the cap fail with `BAD_REQUEST`;
//! - each file holds at most [`MAX_CERT_FILE_BYTES`] bytes, so one
//!   upload cannot balloon memory or disk;
//! - names hold at most [`MAX_NAME_LEN`] bytes, so one path stays short.
//!
//! Listeners do not use these bundles yet; that wiring is a later ticket
//! and is out of scope here. A delete therefore never finds a referencing
//! configuration today. Management-plane only; publish and deliver never
//! touch these files.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::errors::ApiError;
use crate::ApiState;

/// Largest accepted upload file in bytes (1 MiB). Reason: a PEM chain
/// with a key stays far below 1 MiB, and the cap
/// bounds memory per request plus disk per file.
const MAX_CERT_FILE_BYTES: usize = 1_048_576;
/// Largest bundle count in one namespace. Reason: each list reads one
/// directory per request, so 128 entries bound the scan to one short read.
/// Creates past the cap fail.
const MAX_CERT_BUNDLES: usize = 128;
/// Largest managed-namespace count. Reason: each namespace is one
/// directory and uploads count them, so 64 bounds the count and the
/// disk use. Uploads past the cap fail.
const MAX_CERT_NAMESPACES: usize = 64;
/// Longest accepted bundle or namespace name in bytes. Reason: a name
/// becomes one path segment and the filesystem caps a segment at 255,
/// so 128 keeps paths short with room for parent prefixes.
/// Longer names are rejected.
const MAX_NAME_LEN: usize = 128;

/// File name for an uploaded private key. Mode 600 (see [`KEY_MODE`]).
const FILENAME_KEY: &str = "key.pem";
/// File name for an uploaded certificate chain.
const FILENAME_CHAIN: &str = "chain.pem";
/// File name for an uploaded CA file.
const FILENAME_CA: &str = "ca.pem";
/// File name for an uploaded account key. Mode 600 (see [`KEY_MODE`]).
const FILENAME_ACC_KEY: &str = "acc-key.pem";
/// File name for an uploaded key password. Mode 600 (see [`KEY_MODE`]).
const FILENAME_KEY_PASSWORD: &str = "key-password";
/// File mode for key material. Reason: the key decrypts traffic, so only
/// the owner reads it.
const KEY_MODE: u32 = 0o600;
/// File mode for public certificate files. Reason: a chain verifies
/// peers, so readers need it.
const CERT_MODE: u32 = 0o644;

/// Counter for upload temp names. It keeps temp names unique.
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// One uploaded file kind: the API name, the fixed file name and whether
/// the file holds key material (mode 600) or a certificate (mode 644).
struct FileKind {
    api: &'static str,
    filename: &'static str,
    secret: bool,
}

/// The five file kinds of the reference API.
const FILE_KINDS: [FileKind; 5] = [
    FileKind {
        api: "key",
        filename: FILENAME_KEY,
        secret: true,
    },
    FileKind {
        api: "chain",
        filename: FILENAME_CHAIN,
        secret: false,
    },
    FileKind {
        api: "ca",
        filename: FILENAME_CA,
        secret: false,
    },
    FileKind {
        api: "acc_key",
        filename: FILENAME_ACC_KEY,
        secret: true,
    },
    FileKind {
        api: "key_password",
        filename: FILENAME_KEY_PASSWORD,
        secret: true,
    },
];

/// Finds the file kind for an API name.
fn kind_for(api: &str) -> Option<&'static FileKind> {
    FILE_KINDS.iter().find(|kind| kind.api == api)
}

/// True when the name is one safe path segment: 1 to [`MAX_NAME_LEN`]
/// bytes of letters, digits, `.`, `_` and `-`, never `.` or `..`.
/// Reason: the name becomes a directory name under the data directory,
/// so anything else could escape it.
fn valid_name(name: &str) -> bool {
    if name.is_empty() || name.len() > MAX_NAME_LEN {
        return false;
    }
    if name == "." || name == ".." {
        return false;
    }
    name.bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Root directory for all managed bundles.
fn certs_root(state: &ApiState) -> PathBuf {
    state.config.dir().join("certs")
}

/// Directory for one namespace: `global` or `ns/<namespace>`.
/// An upload creates the namespace on demand.
/// Every valid name works without a registry.
fn namespace_dir(state: &ApiState, namespace: Option<&str>) -> PathBuf {
    match namespace {
        None => certs_root(state).join("global"),
        Some(ns) => certs_root(state).join("ns").join(ns),
    }
}

/// Directory for one bundle.
fn bundle_dir(state: &ApiState, namespace: Option<&str>, bundle: &str) -> PathBuf {
    namespace_dir(state, namespace).join(bundle)
}

/// Counts the direct child directories of `dir`. A missing directory
/// counts zero, never an error.
async fn count_dirs(dir: &std::path::Path) -> usize {
    let Ok(mut entries) = tokio::fs::read_dir(dir).await else {
        return 0;
    };
    let mut count = 0;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if entry.file_type().await.is_ok_and(|kind| kind.is_dir()) {
            count += 1;
        }
    }
    count
}

/// True when the path is a regular file.
async fn is_file(path: &std::path::Path) -> bool {
    tokio::fs::metadata(path)
        .await
        .is_ok_and(|meta| meta.is_file())
}

/// Parses PEM certificate bytes into DER blocks. Each block must parse
/// as an X.509 certificate. Fails when no block is present or a block
/// does not parse.
fn parse_certificates(pem: &[u8]) -> Result<Vec<Vec<u8>>, String> {
    use rustls::pki_types::pem::PemObject as _;
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls::pki_types::CertificateDer::pem_slice_iter(pem)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| "file is not valid PEM".to_string())?;
    if certs.is_empty() {
        return Err("file holds no certificate".to_string());
    }
    let blocks: Vec<Vec<u8>> = certs.into_iter().map(|c| c.to_vec()).collect();
    for block in &blocks {
        use x509_parser::prelude::{FromDer, X509Certificate};
        X509Certificate::from_der(block)
            .map_err(|_| "file holds no valid certificate".to_string())?;
    }
    Ok(blocks)
}

/// Finds the first private key in PEM bytes. Fails when no private key
/// is present or the PEM does not parse. Encrypted keys have no item
/// here, so they fail closed as missing.
/// The broker refuses an encrypted key.
/// The pair cannot be checked without the password.
fn parse_private_key(pem: &[u8]) -> Result<rustls::pki_types::PrivateKeyDer<'static>, String> {
    use rustls::pki_types::pem::PemObject as _;
    let mut keys = rustls::pki_types::PrivateKeyDer::pem_slice_iter(pem);
    match keys.next() {
        Some(Ok(key)) => Ok(key),
        Some(Err(_)) => Err("file is not valid PEM".to_string()),
        None => Err("file holds no private key".to_string()),
    }
}

/// True when the private key matches the first certificate of the chain.
/// Uses the maintained TLS library: it loads the pair the same way the
/// listener does and refuses a mismatch. Never logs key material.
fn pair_matches(chain_der: Vec<Vec<u8>>, key: rustls::pki_types::PrivateKeyDer<'static>) -> bool {
    use rustls::pki_types::CertificateDer;
    let chain: Vec<CertificateDer<'static>> =
        chain_der.into_iter().map(CertificateDer::from).collect();
    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    // Any failure means the pair is unusable: a mismatch, a key the
    // provider cannot load or no usable suite. All fail the upload.
    let Ok(staged) =
        rustls::ServerConfig::builder_with_provider(provider).with_safe_default_protocol_versions()
    else {
        return false;
    };
    staged
        .with_no_client_auth()
        .with_single_cert(chain, key)
        .is_ok()
}

/// One validated upload file: the kind plus the raw bytes.
struct UploadFile {
    kind: &'static FileKind,
    bytes: Vec<u8>,
}

/// Validates one upload value by kind. The chain and the CA must hold a
/// certificate; the key and the account key must hold a private key; the
/// key password is opaque text. Fails with a message naming the kind but
/// never the contents.
fn validate_file(api: &str, text: &str) -> Result<UploadFile, String> {
    let Some(kind) = kind_for(api) else {
        return Err(format!("unknown file kind {api:?}"));
    };
    if text.is_empty() {
        return Err(format!("file {api:?} must not be empty"));
    }
    if text.len() > MAX_CERT_FILE_BYTES {
        return Err(format!("file {api:?} passes the 1 MiB cap"));
    }
    let bytes = text.as_bytes();
    match api {
        "chain" | "ca" => {
            parse_certificates(bytes)
                .map_err(|reason| format!("file {api:?} is invalid: {reason}"))?;
        }
        "key" | "acc_key" => {
            parse_private_key(bytes)
                .map_err(|reason| format!("file {api:?} is invalid: {reason}"))?;
        }
        _ => {}
    }
    Ok(UploadFile {
        kind,
        bytes: bytes.to_vec(),
    })
}

/// Validates a whole upload body: a JSON object with at least one known
/// kind, each value a string. Fails with `BAD_REQUEST` text.
fn validate_body(body: &Bytes) -> Result<Vec<UploadFile>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|_| "request body must be JSON".to_string())?;
    let obj = value
        .as_object()
        .ok_or_else(|| "request body must be a JSON object".to_string())?;
    if obj.is_empty() {
        return Err("upload holds no file".to_string());
    }
    let mut out = Vec::with_capacity(obj.len());
    for (api, item) in obj {
        let text = item
            .as_str()
            .ok_or_else(|| format!("file {api:?} must be text"))?;
        out.push(validate_file(api, text)?);
    }
    Ok(out)
}

/// Reads the stored bytes of one kind, if the file exists.
async fn stored_bytes(bundle: &std::path::Path, kind: &FileKind) -> Option<Vec<u8>> {
    let path = bundle.join(kind.filename);
    if !is_file(&path).await {
        return None;
    }
    tokio::fs::read(&path).await.ok()
}

/// Creates the bundle directory. On Unix the new directories get mode
/// 700, because the bundle can hold key material.
async fn create_bundle_dir(bundle: &std::path::Path) -> std::io::Result<()> {
    let mut builder = tokio::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(0o700);
    builder.create(bundle).await
}

/// Writes one new file. The file is created with `mode` on Unix, so
/// key material is never readable by other users, not even for a moment.
/// The mode is never changed after the write. An existing file of that
/// name is an error. A failed write removes the part file.
async fn write_new_file(path: &std::path::Path, bytes: &[u8], mode: u32) -> std::io::Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(mode);
    #[cfg(not(unix))]
    let _ = mode;
    let mut file = options.open(path).await?;
    let written = async {
        file.write_all(bytes).await?;
        file.flush().await?;
        file.sync_all().await
    }
    .await;
    drop(file);
    if written.is_err() {
        tokio::fs::remove_file(path).await.ok();
    }
    written
}

/// Writes all validated files into the bundle directory. Each file goes
/// to a temp name in the same directory, created with its final mode,
/// then renames over the real name, so a reader sees the previous file
/// or the complete new one. Fails with an internal error naming the kind
/// but never the contents.
async fn store_files(bundle: &std::path::Path, files: &[UploadFile]) -> Result<(), Box<Response>> {
    if let Err(error) = create_bundle_dir(bundle).await {
        return Err(Box::new(internal_error(format!(
            "cannot use bundle dir: {error}"
        ))));
    }
    for file in files {
        let slot = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let tmp = bundle.join(format!(".{}.part-{slot}", file.kind.filename));
        let mode = if file.kind.secret {
            KEY_MODE
        } else {
            CERT_MODE
        };
        if let Err(error) = write_new_file(&tmp, &file.bytes, mode).await {
            return Err(Box::new(internal_error(format!(
                "cannot write file {:?}: {error}",
                file.kind.api
            ))));
        }
        if let Err(error) = tokio::fs::rename(&tmp, bundle.join(file.kind.filename)).await {
            tokio::fs::remove_file(&tmp).await.ok();
            return Err(Box::new(internal_error(format!(
                "cannot publish file {:?}: {error}",
                file.kind.api
            ))));
        }
    }
    Ok(())
}

fn internal_error(message: String) -> Response {
    ApiError::InternalError(message).into_response()
}

/// Renders one bundle directory as `{kind: {"path"}}`. Only kinds with
/// a stored file appear. Never renders file contents.
async fn render_bundle(bundle: &std::path::Path) -> serde_json::Value {
    let mut out = serde_json::Map::new();
    for kind in &FILE_KINDS {
        if is_file(&bundle.join(kind.filename)).await {
            let path = bundle.join(kind.filename).to_string_lossy().into_owned();
            out.insert(kind.api.to_string(), serde_json::json!({ "path": path }));
        }
    }
    serde_json::Value::Object(out)
}

/// Shared upload path for both namespaces. Validates names, the body,
/// the account-key rule and the key match, enforces the bounds, then
/// stores. Answers 204 on success.
async fn upload_bundle(
    state: &ApiState,
    namespace: Option<&str>,
    bundle: &str,
    body: Bytes,
) -> Response {
    if bundle.trim().is_empty() || !valid_name(bundle) {
        return ApiError::BadRequest("bundle name is invalid".to_string()).into_response();
    }
    if let Some(ns) = namespace {
        if !valid_name(ns) {
            return ApiError::BadRequest("namespace name is invalid".to_string()).into_response();
        }
    }
    let files = match validate_body(&body) {
        Ok(files) => files,
        Err(reason) => return ApiError::BadRequest(reason).into_response(),
    };
    let dir = bundle_dir(state, namespace, bundle);
    // The account client owns the chain and the key once the account
    // key exists. An upload of either is probably a user error, so it
    // fails instead of fighting the client.
    let wants_chain_or_key = files
        .iter()
        .any(|file| file.kind.api == "chain" || file.kind.api == "key");
    if wants_chain_or_key {
        let acc = kind_for("acc_key").expect("account key kind exists");
        if stored_bytes(&dir, acc).await.is_some() {
            return ApiError::BadRequest(
                "account key exists; the account client manages the chain and the key".to_string(),
            )
            .into_response();
        }
    }
    // Key match: the effective key and chain are the uploaded bytes over
    // the stored files. When both exist, they must belong together.
    let key_kind = kind_for("key").expect("key kind exists");
    let chain_kind = kind_for("chain").expect("chain kind exists");
    // Load stored sides only when the upload lacks them.
    let mut eff_key: Option<Vec<u8>> = files
        .iter()
        .find(|file| file.kind.api == "key")
        .map(|file| file.bytes.clone());
    let mut eff_chain: Option<Vec<u8>> = files
        .iter()
        .find(|file| file.kind.api == "chain")
        .map(|file| file.bytes.clone());
    if eff_key.is_none() {
        eff_key = stored_bytes(&dir, key_kind).await;
    }
    if eff_chain.is_none() {
        eff_chain = stored_bytes(&dir, chain_kind).await;
    }
    // An upload that carries a fresh key with no chain anywhere is
    // checked against nothing yet; the chain upload checks the pair.
    // A bundle may hold a key before its chain arrives.
    // The broker checks the pair when both exist.
    if let (Some(key_pem), Some(chain_pem)) = (eff_key, eff_chain) {
        let fresh_key = files.iter().any(|file| file.kind.api == "key");
        let fresh_chain = files.iter().any(|file| file.kind.api == "chain");
        if fresh_key || fresh_chain {
            let chain_der = match parse_certificates(&chain_pem) {
                Ok(der) => der,
                Err(reason) => {
                    return ApiError::BadRequest(format!("stored chain is invalid: {reason}"))
                        .into_response();
                }
            };
            let key = match parse_private_key(&key_pem) {
                Ok(key) => key,
                Err(reason) => {
                    return ApiError::BadRequest(format!("stored key is invalid: {reason}"))
                        .into_response();
                }
            };
            if !pair_matches(chain_der, key) {
                return ApiError::BadRequest(
                    "private key does not match the certificate".to_string(),
                )
                .into_response();
            }
        }
    }
    // Bounds: a new bundle past the per-namespace cap fails, and a new
    // namespace past the namespace cap fails. Existing bundles update
    // freely within the file cap.
    let is_new = !tokio::fs::metadata(&dir)
        .await
        .is_ok_and(|meta| meta.is_dir());
    if is_new {
        let bundles = count_dirs(&namespace_dir(state, namespace)).await;
        if bundles >= MAX_CERT_BUNDLES {
            return ApiError::BadRequest("bundle store is full".to_string()).into_response();
        }
        if namespace.is_some() {
            let namespaces = count_dirs(&certs_root(state).join("ns")).await;
            let ns_exists = tokio::fs::metadata(&namespace_dir(state, namespace))
                .await
                .is_ok_and(|meta| meta.is_dir());
            if !ns_exists && namespaces >= MAX_CERT_NAMESPACES {
                return ApiError::BadRequest("namespace store is full".to_string()).into_response();
            }
        }
    }
    match store_files(&dir, &files).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(response) => *response,
    }
}

/// Lists the bundles of one namespace as `[{"name"}]`, sorted by name.
/// A missing directory reads as an empty list, never an error.
async fn list_bundles(state: &ApiState, namespace: Option<&str>) -> Response {
    if let Some(ns) = namespace {
        if !valid_name(ns) {
            return ApiError::BadRequest("namespace name is invalid".to_string()).into_response();
        }
    }
    let dir = namespace_dir(state, namespace);
    let Ok(mut entries) = tokio::fs::read_dir(&dir).await else {
        return (StatusCode::OK, Json(Vec::<serde_json::Value>::new())).into_response();
    };
    let mut names: Vec<String> = Vec::new();
    while let Ok(Some(entry)) = entries.next_entry().await {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !valid_name(&name) {
            continue;
        }
        if entry.file_type().await.is_ok_and(|kind| kind.is_dir()) {
            names.push(name);
        }
    }
    names.sort();
    let rows: Vec<serde_json::Value> = names
        .into_iter()
        .map(|name| serde_json::json!({ "name": name }))
        .collect();
    (StatusCode::OK, Json(rows)).into_response()
}

/// Reads one bundle as `{kind: {"path"}}`. A missing bundle is 404 with
/// the documented shape. Never renders file contents.
async fn get_bundle(state: &ApiState, namespace: Option<&str>, bundle: &str) -> Response {
    if bundle.trim().is_empty() || !valid_name(bundle) {
        return ApiError::BadRequest("bundle name is invalid".to_string()).into_response();
    }
    if let Some(ns) = namespace {
        if !valid_name(ns) {
            return ApiError::BadRequest("namespace name is invalid".to_string()).into_response();
        }
    }
    let dir = bundle_dir(state, namespace, bundle);
    if !tokio::fs::metadata(&dir)
        .await
        .is_ok_and(|meta| meta.is_dir())
    {
        return ApiError::NotFound("Bundle not found.".to_string()).into_response();
    }
    (StatusCode::OK, Json(render_bundle(&dir).await)).into_response()
}

/// Deletes one bundle or one file of it. A `kind` query value names the
/// file; without it the whole bundle goes. Missing bundles and files
/// answer 204: deletes are idempotent. An unknown kind is 400.
async fn delete_bundle(
    state: &ApiState,
    namespace: Option<&str>,
    bundle: &str,
    query: HashMap<String, String>,
) -> Response {
    if bundle.trim().is_empty() || !valid_name(bundle) {
        return ApiError::BadRequest("bundle name is invalid".to_string()).into_response();
    }
    if let Some(ns) = namespace {
        if !valid_name(ns) {
            return ApiError::BadRequest("namespace name is invalid".to_string()).into_response();
        }
    }
    let dir = bundle_dir(state, namespace, bundle);
    if let Some(kind_name) = query.get("kind") {
        let Some(kind) = kind_for(kind_name) else {
            return ApiError::BadRequest(format!("unknown file kind {kind_name:?}"))
                .into_response();
        };
        let path = dir.join(kind.filename);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => return StatusCode::NO_CONTENT.into_response(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return StatusCode::NO_CONTENT.into_response();
            }
            Err(error) => {
                return internal_error(format!("cannot delete file {:?}: {error}", kind.api));
            }
        }
    }
    match tokio::fs::remove_dir_all(&dir).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            StatusCode::NO_CONTENT.into_response()
        }
        Err(error) => internal_error(format!("cannot delete bundle: {error}")),
    }
}

/// `GET /certs/global/list`: all bundles of the global namespace.
pub async fn list_global_bundles(State(state): State<ApiState>) -> Response {
    list_bundles(&state, None).await
}

/// `GET /certs/global/name/:name`: all files of one global bundle.
pub async fn get_global_bundle(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Response {
    get_bundle(&state, None, &name).await
}

/// `POST /certs/global/name/:name`: upload files to one global bundle.
pub async fn upload_global_bundle(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    body: Bytes,
) -> Response {
    upload_bundle(&state, None, &name, body).await
}

/// `DELETE /certs/global/name/:name`: delete one global bundle or, with
/// `?kind=`, one file of it.
pub async fn delete_global_bundle(
    State(state): State<ApiState>,
    Path(name): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    delete_bundle(&state, None, &name, query).await
}

/// `GET /certs/ns/:namespace/list`: all bundles of one namespace.
pub async fn list_ns_bundles(
    State(state): State<ApiState>,
    Path(namespace): Path<String>,
) -> Response {
    list_bundles(&state, Some(&namespace)).await
}

/// `GET /certs/ns/:namespace/name/:name`: all files of one bundle.
pub async fn get_ns_bundle(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
) -> Response {
    get_bundle(&state, Some(&namespace), &name).await
}

/// `POST /certs/ns/:namespace/name/:name`: upload files to one bundle.
pub async fn upload_ns_bundle(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    upload_bundle(&state, Some(&namespace), &name, body).await
}

/// `DELETE /certs/ns/:namespace/name/:name`: delete one bundle or, with
/// `?kind=`, one file of it.
pub async fn delete_ns_bundle(
    State(state): State<ApiState>,
    Path((namespace, name)): Path<(String, String)>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    delete_bundle(&state, Some(&namespace), &name, query).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::Router;

    /// Fresh matching certificate chain for tests. Fresh fixture made
    /// with the system certificate tool for this task only.
    const TEST_CHAIN: &str = "-----BEGIN CERTIFICATE-----\nMIIDCTCCAfGgAwIBAgIUAK7BfRCN+hN4266rseYMc9eFu9swDQYJKoZIhvcNAQEL\nBQAwFDESMBAGA1UEAwwJYXAxNS10ZXN0MB4XDTI2MTAwODA5MTU0NVoXDTM2MTAw\nNTA5MTU0NVowFDESMBAGA1UEAwwJYXAxNS10ZXN0MIIBIjANBgkqhkiG9w0BAQEF\nAAOCAQ8AMIIBCgKCAQEAu8n5AKQaYM+UJ9EHr2YxdDqJgM9Oi7YQiq7M29lZWzFH\nDh8NQ/R3146LY2XmedzOftbdl5Sa/LSfqsjuyoNVu+SBBc7Xuh9HSy/pYrXoeydm\ng+L3l9KIEFqXTghHfJlZLuME0clfUckydpuePQ+ZqhyMEDmTJ+MILw5J2SCvSBj0\nI0gbP4Gi3rrvI4btqLboq7vZTZ20vpitO48NPOagUSNCLSA371IlpItspf9hzIfG\nf0kfOYQM4uwPOuuFy3Q5B1S1pTIMEvk6p7qYnN7cGkrI94Zgz415Wqm2cotStkZ3\neHCS4alleNP4ZLAexnZw2JLNq7jiXu5pIIxoCfMvPwIDAQABo1MwUTAdBgNVHQ4E\nFgQU7y2nWmV99SbdCVYG168SiB63DDowHwYDVR0jBBgwFoAU7y2nWmV99SbdCVYG\n168SiB63DDowDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAntAS\nsPHtT5YQFUWM1w0LWSfChMRFMKz05S6yzFegN/tQImQXBd1KpaeeTQ06JGq+OT0G\nZnPxtz91nofEshAjBJBqwRjTAhNGVN8bITFta/Pvj6NsvcjdLE5CeHt0UrXWPOth\n9/bAPNkVm7e7tFAoMY6I86w8im9E8vqrM+LZkAcPpEPtbXbNdH4MF14K4zSjJM8o\nPqy1ahiEyPxV07GUal0daQkLQrXgS8Y/4efSnkGWKIViJcDDXDxVgqbCW0At833Y\nD9DV81UzGdQ1z/UqNpMkz9YcweEv/f1Vl9JwQ2+DsXQv79Ls2AdOvOuJEXc6P2YQ\nuuTu99Wo2cJwNiINWg==\n-----END CERTIFICATE-----";
    /// Fresh private key that matches [`TEST_CHAIN`]. Same source.
    const TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7yfkApBpgz5Qn\n0QevZjF0OomAz06LthCKrszb2VlbMUcOHw1D9HfXjotjZeZ53M5+1t2XlJr8tJ+q\nyO7Kg1W75IEFzte6H0dLL+liteh7J2aD4veX0ogQWpdOCEd8mVku4wTRyV9RyTJ2\nm549D5mqHIwQOZMn4wgvDknZIK9IGPQjSBs/gaLeuu8jhu2otuiru9lNnbS+mK07\njw085qBRI0ItIDfvUiWki2yl/2HMh8Z/SR85hAzi7A8664XLdDkHVLWlMgwS+Tqn\nupic3twaSsj3hmDPjXlaqbZyi1K2Rnd4cJLhqWV40/hksB7GdnDYks2ruOJe7mkg\njGgJ8y8/AgMBAAECggEAHZykDYwoYLBlnhW3JDB1h0IdIVkPULadSyZ0mrezqyP3\nyWvvbTzP57IhnBibIbJGRE1g1KCUBASo0jHXKHX8qfacEIIPwINi0aIKJ7ekTkSQ\nmibLK39WV1ssVXC2x8NtWG28pSbKR3JLk9UcPGgJBNXK5wVn2rf74revwMkHpX2M\noyP0NM3aCzDqv8BHIze5FKUgPsZw1y3sGngm+UyCFSQ86nJ7prtDUyhCf0ACBhcP\n4YdDWsho3J48u3r8vK4PuLPUntvnEhlMIbuv46lcwYRnuZlzRA6r+UpdDmQbfsXp\nFMQVm7V7PO/YE+MEQjW8eLZgHQFonZTCjF7RzGAycQKBgQDk9A/3XWrLugN8GoRE\nXSsVAyVOXWqHIgh7A/cumlhZx5HVGA5Q+JDnkyeuM9DhJdD3m+Q+X8HNC8wolfIB\nE2aZCRwzmseIz/d/6C2BotuV1zWYUX3TcKgBL0UAUX3aP5aqdx36yCoCFqeXgS81\ny/JGnZcWDMpyNOaYwyRVIg32pwKBgQDR+QdbHR51FluyNAezdK3Y+87TtCkuDOSJ\ngNZM1WJBPrEGdea49zHSE8crgwnqg+Qd0kPRsojLvhPvjYGXwH9EnaFNGlCaakLM\nncAYkeQExLWn3eoLgzrZSL1YN5Dt1dmT8n0NTu+4q7GquTt2foZ6BHsZITOcQhuP\n5azivqAtqQKBgC+18HHS5MNpkvpUqEV6yNSLIEYRT3azj+Las7LhdRHYdbpbVfhn\npAKt9rHlmFyivcYrlUeuOruY/sViYdj5iwLPFDJoybXn6KKd5oXCQ1IZU9DXAoqC\nSnBcwltLoBdP8vy47+VnuuFOiUQYJpdmVamDg42TwshD393rpHGdNB23AoGAIW64\naB6L0fLcaNR2AXHrmGgHt5exEt8EqIw2EwfK3D6iKzgso3jgmuWBSgFUW93iPYlU\nORhnRaGfRP5QCxygZwl+PGubN+sBddkMVB0upD+5+N+Ru4GKpjw5wckfL29kdHUg\nyjSuYMYdxOQ3fpeIy/bYrtilghTYlKHqVDQsBvkCgYEAkLjdTFc3osrTJ5y/N6lO\nY49EonoDbAtN1wCdRiisAUOY0opc8cgiuQ+6BZDz61/UrcGxZyIu7Il9ct4FhuRE\nB8M39vzMqmihI+4OrFa18pJ4Zyj2nBfeNsmcVOTwkS8/YS+BuHh5O41ys2Nv5JWC\nuoAse4RhIhFNep9fX6gxDHo=\n-----END PRIVATE KEY-----";
    /// Fresh private key that does NOT match [`TEST_CHAIN`]. Same source.
    const OTHER_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDM2KoZAF/UdRDk\nSGtNk4mHm+kK/dIWOv1DJr+cHOI/py4n2hsb9LDFdUBRzxBkG73PX5Q5L066cfN5\nOEsFKXnlYYa4ynYGQmjZQC75n0h77KKi1AiY7f00ZBm1K9ZTwzNb8UNRgOD2RrTE\n2C0n7Evv9j3qAB9fd57+H+sPxTCtTYPQEkyY4CLztkesuVx3E4BHO3+a79SLR0lC\nP1L1YJqABtg2aCpVUqdjWCgVPoVvRK2/rQ1VdCinLyR5UFo8ACIe35XiUXc4O9wJ\njatA0kYekpFGGE1H0eLiVYvCrGCBniqW/f6JUJ2h0nRBmFWe8xgARpuPkI9DGiz+\nORsC1TtnAgMBAAECggEACexgQRnNiqNCw2xNFmyUd82yIMz7xZ8evO2k0DehL86c\nSjEjD9wIr2gaSsKqW2i+OmXs7kamaU3Mqto/q1GMQCc7UyErtB3sQ/bIaWGxlAWt\nbN0Ze+vh6kA3Jpmn2YlWDmXGCkeHnlnKhAapsvECTlUyQDwe4o11VeY1IuxwitSY\nqtegzzUy6Yt6w7gi+RqKLP0cWV4u5V2WYE+XHg0xFJ5Iv0JV6oz82kstkL2pCm35\njlW2dfLzETLG5QYmMpHExs2chkb2b113SevGScHZeGoSbzWhq3AHr9Nfl8/Jdfja\nRxiYWX0rYMukFCqHV1lE9NMV43Y9jtLgJZ94/kcjYQKBgQDvJ6v8qm+XQPVe/c8c\nm3e1rQZce83oqkBrozFNR7d4joW88wz56JraosCEyplbYio+ND4wi5gle+LQ6sAM\nVr/BL0F7RwrMm29PGccK3JRcxomhe0f75Ha2okTnY+DOxouj26RO/vUaQQmFVdL/\n0j4fn5WL9cNo+UTnOCw1PuRFiQKBgQDbRlsdrUDt2goh/6u/960zwZUtsdAi4E99\ngzjmztM6wxPZTumf87mOsGX6YkkIPeLc/WGCeOrMriIIrdzXXl4zq3dCg4ORr5xw\neTigp7TD6HDSqQnaSN4ZEh1Em/+vlcsxS41WOOwzAQFOqpndPntnjLcI8c/n2wm0\nK89nahItbwKBgAEFkl6J++ARjvrQ6smcgG5uRtCuVrrfv1a2Y8eIvufs8/VStIN8\n0uyMNXZbxzv0t2l8n6dlcUj3tohd+GQVGsygfw8fxpQJNfiIi6koOBoNXxkpha01\n1KwDFXhHnPZFMuOIVLNhs3phPMEtFLGzHEDWVbU7X5JbPkgenVG86wzBAoGAfZNB\nQzGhaZXhD694EKwrCOToJg8a/54uZ9bqk2QHHDPz/hNvQCc4MTtca5Q19v5aGe0S\nFsJg1W80FnxbaQumLzkXR4RBqQFFOjRx9a+VmkxXsI0WE2h/qSPylwQv3cKPZ1+5\nRZO70UnWEdMp4saRra+scvbo9gM+/2vHj5XzawkCgYEA6cVk/0jVbi4vEsyUIPbD\nTmF5C1MXmuhBCMmlGjCk/Xfr4IrDCrZcgTjuo09j87jKJKXX2h51QnHKiD5QDNS+\nY6dd9pEE3Zb1/8lnJm8+Y4EhwsVDE25Eza4ity5+yQ6BB8J9TtHcD0RVBHEz6Ne5\nmEmvdJkz6fdo3UwSrS8ktQc=\n-----END PRIVATE KEY-----";

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    /// Serves the certificate routes over HTTP on loopback. No auth
    /// layer: the test serves the routes directly, like the protected
    /// router behind the real middleware.
    fn test_router(state: ApiState) -> Router {
        Router::new()
            .route("/certs/global/list", get(list_global_bundles))
            .route(
                "/certs/global/name/:name",
                get(get_global_bundle)
                    .post(upload_global_bundle)
                    .delete(delete_global_bundle),
            )
            .route("/certs/ns/:namespace/list", get(list_ns_bundles))
            .route(
                "/certs/ns/:namespace/name/:name",
                get(get_ns_bundle)
                    .post(upload_ns_bundle)
                    .delete(delete_ns_bundle),
            )
            .with_state(state)
    }

    /// One ephemeral loopback server for the certificate routes.
    struct HttpServer {
        port: u16,
        task: tokio::task::JoinHandle<()>,
    }

    impl HttpServer {
        async fn start(state: ApiState) -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind loopback server");
            let port = listener.local_addr().expect("loopback addr").port();
            let task = tokio::spawn(async move {
                axum::serve(listener, test_router(state))
                    .await
                    .expect("serve certificate routes");
            });
            Self { port, task }
        }

        async fn call(
            &self,
            method: &str,
            path: &str,
            body: Option<serde_json::Value>,
        ) -> (u16, String) {
            let mut head = format!("{method} {path} HTTP/1.0\r\nHost: test\r\n");
            if let Some(body) = body {
                let raw = serde_json::to_string(&body).expect("encode body");
                head.push_str(&format!(
                    "Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{raw}",
                    raw.len()
                ));
            } else {
                head.push_str("Connection: close\r\n\r\n");
            }
            let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", self.port))
                .await
                .expect("connect loopback server");
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            stream.write_all(head.as_bytes()).await.expect("write");
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.expect("read");
            let text = String::from_utf8(buf).expect("UTF-8");
            let (head, body) = text.split_once("\r\n\r\n").expect("split");
            let status: u16 = head.lines().next().expect("status")[9..12]
                .parse()
                .expect("code");
            (status, body.to_string())
        }

        async fn call_json(
            &self,
            method: &str,
            path: &str,
            body: Option<serde_json::Value>,
        ) -> (u16, serde_json::Value) {
            let (status, raw) = self.call(method, path, body).await;
            let value = if raw.trim().is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(&raw).expect("JSON")
            };
            (status, value)
        }
    }

    impl Drop for HttpServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    /// Full pair body for uploads: CA, chain and matching key.
    fn full_body() -> serde_json::Value {
        serde_json::json!({
            "ca": TEST_CHAIN,
            "chain": TEST_CHAIN,
            "key": TEST_KEY,
        })
    }

    #[test]
    fn cert_paths_need_system_scope() {
        use crate::v5::api_keys::{access_for_path, PathAccess};
        for path in [
            "/api/v5/certs/global/list",
            "/api/v5/certs/global/name/b1",
            "/api/v5/certs/ns/ns1/list",
            "/api/v5/certs/ns/ns1/name/b1",
        ] {
            assert_eq!(access_for_path(path), PathAccess::Scope("system"));
        }
    }

    #[test]
    fn names_hold_one_safe_segment() {
        assert!(valid_name("bundle1"));
        assert!(valid_name("my-bundle_v2.0"));
        assert!(!valid_name(""));
        assert!(!valid_name("a/b"));
        assert!(!valid_name("a\\b"));
        assert!(!valid_name(".."));
        assert!(!valid_name("."));
        assert!(!valid_name("has space"));
        assert!(!valid_name("semi;colon"));
        assert!(!valid_name(&"n".repeat(MAX_NAME_LEN + 1)));
        assert!(valid_name(&"n".repeat(MAX_NAME_LEN)));
    }

    #[test]
    fn body_checks_shape_before_contents() {
        let empty = Bytes::from_static(b"{}");
        assert!(validate_body(&empty).is_err());
        let list = Bytes::from_static(b"[]");
        assert!(validate_body(&list).is_err());
        let broken = Bytes::from_static(b"{oops");
        assert!(validate_body(&broken).is_err());
        let unknown = Bytes::from_static(br#"{"nope": "x"}"#);
        assert!(validate_body(&unknown).is_err());
        let not_text = Bytes::from_static(br#"{"key": 7}"#);
        assert!(validate_body(&not_text).is_err());
        let blank = Bytes::from_static(br#"{"key": ""}"#);
        assert!(validate_body(&blank).is_err());
        let garbage = Bytes::from_static(br#"{"chain": "not pem"}"#);
        assert!(validate_body(&garbage).is_err());
    }

    #[test]
    fn file_checks_match_fixtures() {
        // The fixtures are a true pair.
        let chain = parse_certificates(TEST_CHAIN.as_bytes()).expect("chain parses");
        assert!(!chain.is_empty());
        let key = parse_private_key(TEST_KEY.as_bytes()).expect("key parses");
        assert!(pair_matches(chain, key));
        // The other key parses but does not match the chain.
        let other = parse_private_key(OTHER_KEY.as_bytes()).expect("other key parses");
        let chain_again = parse_certificates(TEST_CHAIN.as_bytes()).expect("chain parses");
        assert!(!pair_matches(chain_again, other));
        // The key alone is no certificate, and the chain is no key.
        assert!(parse_certificates(TEST_KEY.as_bytes()).is_err());
        assert!(parse_private_key(TEST_CHAIN.as_bytes()).is_err());
    }

    #[tokio::test]
    async fn empty_list_reads_empty() {
        let server = HttpServer::start(standalone_state()).await;
        let (status, body) = server.call_json("GET", "/certs/global/list", None).await;
        assert_eq!(status, 200);
        assert_eq!(body, serde_json::json!([]));
        let (status, body) = server.call_json("GET", "/certs/ns/ns1/list", None).await;
        assert_eq!(status, 200);
        assert_eq!(body, serde_json::json!([]));
    }

    #[tokio::test]
    async fn upload_read_delete_round_trip() {
        let state = standalone_state();
        let server = HttpServer::start(state.clone()).await;
        let (status, _) = server
            .call_json("POST", "/certs/global/name/t-http-1", Some(full_body()))
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.call_json("GET", "/certs/global/list", None).await;
        assert_eq!(status, 200);
        assert_eq!(body, serde_json::json!([{ "name": "t-http-1" }]));
        let (status, raw) = server
            .call("GET", "/certs/global/name/t-http-1", None)
            .await;
        assert_eq!(status, 200);
        // A read renders paths only, never key material.
        assert!(raw.contains("chain.pem"));
        assert!(raw.contains("key.pem"));
        assert!(raw.contains("ca.pem"));
        assert!(!raw.contains("PRIVATE KEY"));
        assert!(!raw.contains(&TEST_KEY[27..60]));
        let body: serde_json::Value = serde_json::from_str(&raw).expect("info is JSON");
        assert!(body["chain"]["path"]
            .as_str()
            .expect("chain path")
            .contains("t-http-1"));
        assert!(body["key"]["path"]
            .as_str()
            .expect("key path")
            .contains("t-http-1"));
        // The key file stores with mode 600.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let key_path = state
                .config
                .dir()
                .join("certs")
                .join("global")
                .join("t-http-1")
                .join("key.pem");
            let mode = std::fs::metadata(&key_path)
                .expect("key file exists")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "key file keeps mode 600");
        }
        let (status, _) = server
            .call("DELETE", "/certs/global/name/t-http-1", None)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .call_json("GET", "/certs/global/name/t-http-1", None)
            .await;
        assert_eq!(status, 404);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
    }

    /// A secret file is never readable by other users, not even for a
    /// moment: the temporary file is created with mode 600. A poller
    /// reads the mode of every temporary file while an upload through
    /// the HTTP routes stores a large secret.
    #[cfg(unix)]
    #[tokio::test]
    async fn key_temp_file_is_private_from_creation() {
        use std::os::unix::fs::PermissionsExt;
        let state = standalone_state();
        let server = HttpServer::start(state.clone()).await;
        let dir = state
            .config
            .dir()
            .join("certs")
            .join("global")
            .join("t-http-mode");
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen_open = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let poller = {
            let (dir, stop, seen_open) = (dir.clone(), stop.clone(), seen_open.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    if let Ok(entries) = std::fs::read_dir(&dir) {
                        for entry in entries.flatten() {
                            let name = entry.file_name().to_string_lossy().to_string();
                            if !name.starts_with(".key-password.part-") {
                                continue;
                            }
                            if let Ok(meta) = entry.metadata() {
                                if meta.permissions().mode() & 0o077 != 0 {
                                    seen_open.store(true, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
            })
        };
        let big = "x".repeat(512 * 1024);
        let (status, _) = server
            .call_json(
                "POST",
                "/certs/global/name/t-http-mode",
                Some(serde_json::json!({ "key_password": big })),
            )
            .await;
        assert_eq!(status, 204);
        stop.store(true, Ordering::Relaxed);
        poller.join().expect("poller ends");
        assert!(
            !seen_open.load(Ordering::Relaxed),
            "the temporary key file was readable by other users"
        );
        let mode = std::fs::metadata(dir.join("key-password"))
            .expect("key file exists")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600, "the stored key keeps mode 600");
        let dir_mode = std::fs::metadata(&dir).expect("dir").permissions().mode();
        assert_eq!(dir_mode & 0o077, 0, "the bundle directory is private");
    }

    #[tokio::test]
    async fn mismatched_key_is_refused() {
        let server = HttpServer::start(standalone_state()).await;
        let bad = serde_json::json!({ "chain": TEST_CHAIN, "key": OTHER_KEY });
        let (status, body) = server
            .call_json("POST", "/certs/global/name/t-mismatch", Some(bad))
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        // Nothing is stored on a refused upload.
        let (status, _) = server
            .call("GET", "/certs/global/name/t-mismatch", None)
            .await;
        assert_eq!(status, 404);
    }

    #[tokio::test]
    async fn lone_key_meets_later_chain() {
        let server = HttpServer::start(standalone_state()).await;
        // A lone key stores; the pair is checked when the chain arrives.
        let (status, _) = server
            .call_json(
                "POST",
                "/certs/global/name/t-lone",
                Some(serde_json::json!({ "key": TEST_KEY })),
            )
            .await;
        assert_eq!(status, 204);
        let (status, _) = server
            .call_json(
                "POST",
                "/certs/global/name/t-lone",
                Some(serde_json::json!({ "chain": TEST_CHAIN })),
            )
            .await;
        assert_eq!(status, 204);
        // A wrong key fails against the stored chain instead.
        let (status, _) = server
            .call_json(
                "POST",
                "/certs/global/name/t-lone-bad",
                Some(serde_json::json!({ "key": OTHER_KEY })),
            )
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .call_json(
                "POST",
                "/certs/global/name/t-lone-bad",
                Some(serde_json::json!({ "chain": TEST_CHAIN })),
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn bad_names_are_refused() {
        let server = HttpServer::start(standalone_state()).await;
        // A backslash stays one path segment and fails the name check.
        let (status, body) = server
            .call_json("POST", "/certs/global/name/bad\\name", Some(full_body()))
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        let (status, _) = server
            .call_json("GET", "/certs/ns/bad\\ns/list", None)
            .await;
        assert_eq!(status, 400);
    }

    #[tokio::test]
    async fn oversize_file_is_refused() {
        let server = HttpServer::start(standalone_state()).await;
        let big = "A".repeat(MAX_CERT_FILE_BYTES + 1);
        let body = serde_json::json!({ "chain": big });
        let (status, response) = server
            .call_json("POST", "/certs/global/name/t-big", Some(body))
            .await;
        assert_eq!(status, 400);
        assert_eq!(response["code"], serde_json::json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn namespace_round_trip_with_kind_delete() {
        let server = HttpServer::start(standalone_state()).await;
        let (status, _) = server
            .call_json("POST", "/certs/ns/ns1/name/b1", Some(full_body()))
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.call_json("GET", "/certs/ns/ns1/list", None).await;
        assert_eq!(status, 200);
        assert_eq!(body, serde_json::json!([{ "name": "b1" }]));
        // One file goes with ?kind=; the bundle keeps the rest.
        let (status, _) = server
            .call("DELETE", "/certs/ns/ns1/name/b1?kind=key", None)
            .await;
        assert_eq!(status, 204);
        let (status, body) = server.call_json("GET", "/certs/ns/ns1/name/b1", None).await;
        assert_eq!(status, 200);
        assert!(body.get("key").is_none());
        assert!(body["chain"]["path"].is_string());
        // An unknown kind fails; a missing bundle still answers 204.
        let (status, body) = server
            .call_json("DELETE", "/certs/ns/ns1/name/b1?kind=nope", None)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        let (status, _) = server.call("DELETE", "/certs/ns/ns1/name/b1", None).await;
        assert_eq!(status, 204);
        let (status, _) = server
            .call("DELETE", "/certs/global/name/no-such", None)
            .await;
        assert_eq!(status, 204);
    }

    #[tokio::test]
    async fn password_file_is_opaque_and_hidden() {
        let server = HttpServer::start(standalone_state()).await;
        let body = serde_json::json!({ "key_password": "s3cret-pw" });
        let (status, _) = server
            .call_json("POST", "/certs/global/name/t-pw", Some(body))
            .await;
        assert_eq!(status, 204);
        let (status, raw) = server.call("GET", "/certs/global/name/t-pw", None).await;
        assert_eq!(status, 200);
        assert!(raw.contains("key-password"));
        assert!(!raw.contains("s3cret-pw"));
    }

    #[tokio::test]
    async fn account_key_blocks_chain_upload() {
        let server = HttpServer::start(standalone_state()).await;
        let body = serde_json::json!({ "acc_key": TEST_KEY });
        let (status, _) = server
            .call_json("POST", "/certs/global/name/t-acme", Some(body))
            .await;
        assert_eq!(status, 204);
        let (status, body) = server
            .call_json(
                "POST",
                "/certs/global/name/t-acme",
                Some(serde_json::json!({ "chain": TEST_CHAIN })),
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
    }

    #[tokio::test]
    async fn bundle_cap_refuses_new_names() {
        let state = standalone_state();
        let server = HttpServer::start(state).await;
        for index in 0..MAX_CERT_BUNDLES {
            let name = format!("cap-{index:03}");
            let (status, _) = server
                .call_json(
                    "POST",
                    &format!("/certs/global/name/{name}"),
                    Some(serde_json::json!({ "ca": TEST_CHAIN })),
                )
                .await;
            assert_eq!(status, 204, "bundle {name} stores below the cap");
        }
        let (status, body) = server
            .call_json(
                "POST",
                "/certs/global/name/cap-past",
                Some(serde_json::json!({ "ca": TEST_CHAIN })),
            )
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
        // An existing bundle still updates past the cap.
        let (status, _) = server
            .call_json(
                "POST",
                "/certs/global/name/cap-000",
                Some(serde_json::json!({ "ca": TEST_CHAIN })),
            )
            .await;
        assert_eq!(status, 204);
    }
}
