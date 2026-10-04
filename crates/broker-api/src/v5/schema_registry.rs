//! Schema registry for the v5 management API.
//!
//! Covers `GET /schema_registry` (paged list, C08-11),
//! `POST /schema_registry` (checked create, C08-12) and
//! `DELETE /schema_registry/:name` (drop one schema, C08-13).
//! Single node, management plane only.
//!
//! Handlers clone at most the capped set per list read under a short lock.
//! No publish path reads this store. Publish and deliver take no lock here.
//!
//! Store bounds (stated here and enforced below):
//! - at most [`MAX_SCHEMAS`] entries (256: caps read clones; past writes fail).
//! - `name` holds at most [`MAX_SCHEMA_NAME_LEN`] chars (256: bounds keys).
//! - one document holds at most [`MAX_SCHEMA_BYTES`] bytes (1 MiB: bounds reads).
//! - `description` holds at most [`MAX_SCHEMA_DESC_LEN`] bytes (1024: bounds rows).
//! - list paging reuses the shared extractor (limit 100, max 1000).
//! - the default store is empty.
//!
//! Persistence: each write exports the checked store to the config core
//! (`SchemasConf`) and saves it. A save failure answers 500. Nothing stays silent.
// TODO(parity): which schema kinds does the spec list beyond json and avro?
// TODO(parity): which fields does each schema row carry beyond name and type?
// TODO(parity): which status and code does delete of a used schema return?

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

/// Most schemas kept in the registry. Past writes fail with `BAD_REQUEST`.
/// Reason: the list clones the full set per read, so the set stays small.
pub const MAX_SCHEMAS: usize = broker_config::MAX_SCHEMAS;
/// Longest accepted schema `name`. Reason: names are keys and clone per read.
const MAX_SCHEMA_NAME_LEN: usize = broker_config::MAX_SCHEMA_NAME_LEN;
/// Most bytes accepted in one schema document. Reason: documents clone per
/// read and persist per entry, so one large document cannot bloat reads.
const MAX_SCHEMA_BYTES: usize = broker_config::MAX_SCHEMA_BYTES;
/// Longest accepted schema `description`. Reason: text clones per read.
const MAX_SCHEMA_DESC_LEN: usize = broker_config::MAX_SCHEMA_DESC_LEN;

/// One stored schema: chosen name, checked kind, short note, parsed document.
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaRecord {
    pub name: String,
    pub schema_type: String,
    pub description: String,
    pub schema: serde_json::Value,
}

impl SchemaRecord {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "name": self.name,
            "type": self.schema_type,
            "description": self.description,
            "schema": self.schema,
        })
    }
}

/// Insert failure: duplicate name or full store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaInsertError {
    Duplicate,
    Full,
}

/// Bounded schema directory, persisted through the config core.
/// One map behind one lock. Reads take a short lock.
/// Publish and deliver never take this lock.
pub struct SchemaRegistryStore {
    inner: RwLock<HashMap<String, SchemaRecord>>,
    registry: RwLock<Option<Arc<broker_config::ConfigRegistry>>>,
    save_lock: Mutex<()>,
}

impl SchemaRegistryStore {
    /// Empty directory (empty reads as an empty page).
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            registry: RwLock::new(None),
            save_lock: Mutex::new(()),
        }
    }

    /// Seed from the registry and persist later writes through it.
    /// Kernel boot path. An empty snapshot is a no-op.
    pub fn from_registry(registry: &Arc<broker_config::ConfigRegistry>) -> Self {
        let store = Self::new();
        store.seed_from_snapshot(&registry.snapshot().schemas);
        *store.registry.write().expect("schema store lock") = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and load its snapshot in place.
    /// Kernel boot path on the shared instance.
    pub fn seed_from_registry(&self, registry: &Arc<broker_config::ConfigRegistry>) {
        self.seed_from_snapshot(&registry.snapshot().schemas);
        *self.registry.write().expect("schema store lock") = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &broker_config::SchemasConf) {
        let mut map = self.inner.write().expect("schema store lock");
        map.clear();
        for entry in &conf.schemas {
            let parsed = match serde_json::from_str(&entry.schema) {
                Ok(value) => value,
                Err(_) => {
                    // Fail closed: skip bad rows at boot.
                    tracing::warn!(
                        schema = entry.name.as_str(),
                        "schema snapshot entry is not valid JSON; skipped"
                    );
                    continue;
                }
            };
            if broker_config::validate_schema_document(entry.schema_type.trim(), &parsed).is_err() {
                tracing::warn!(
                    schema = entry.name.as_str(),
                    "schema snapshot entry fails shape checks; skipped"
                );
                continue;
            }
            map.insert(
                entry.name.clone(),
                SchemaRecord {
                    name: entry.name.clone(),
                    schema_type: entry.schema_type.clone(),
                    description: entry.description.clone(),
                    schema: parsed,
                },
            );
        }
    }

    /// Snapshot of live entries sorted by name for stable reads.
    pub fn snapshot(&self) -> Vec<SchemaRecord> {
        let map = self.inner.read().expect("schema store lock");
        let mut out: Vec<SchemaRecord> = map.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    /// Read one entry by name. Clones at most one small record.
    pub fn get(&self, name: &str) -> Option<SchemaRecord> {
        self.inner
            .read()
            .expect("schema store lock")
            .get(name)
            .cloned()
    }

    /// Insert one entry. Fails on duplicate or full store.
    pub fn insert(&self, entry: SchemaRecord) -> Result<SchemaRecord, SchemaInsertError> {
        let mut map = self.inner.write().expect("schema store lock");
        if map.contains_key(&entry.name) {
            return Err(SchemaInsertError::Duplicate);
        }
        if map.len() >= MAX_SCHEMAS {
            return Err(SchemaInsertError::Full);
        }
        map.insert(entry.name.clone(), entry.clone());
        Ok(entry)
    }

    /// Remove one entry by name. Returns false for unknown names.
    pub fn remove(&self, name: &str) -> bool {
        self.inner
            .write()
            .expect("schema store lock")
            .remove(name)
            .is_some()
    }

    /// Export the store as a checked config root. Entries sort by name
    /// so the saved file stays stable. Only inputs persist.
    fn export_conf(&self) -> broker_config::SchemasConf {
        let map = self.inner.read().expect("schema store lock");
        let mut entries: Vec<broker_config::SchemaEntry> = map
            .values()
            .map(|record| broker_config::SchemaEntry {
                name: record.name.clone(),
                schema_type: record.schema_type.clone(),
                description: record.description.clone(),
                schema: serde_json::to_string(&record.schema).unwrap_or_else(|_| "{}".to_string()),
            })
            .collect();
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        broker_config::SchemasConf { schemas: entries }
    }

    /// Commit the exported root and save it. Failures surface as 500.
    /// Stores without a registry stay memory only.
    fn persist(&self) -> Result<(), broker_config::ConfigError> {
        let registry = self.registry.read().expect("schema store lock").clone();
        let Some(registry) = registry else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().expect("schema store lock");
        let conf = self.export_conf();
        registry.commit_schemas(conf)?;
        registry.save()?;
        Ok(())
    }
}

impl Default for SchemaRegistryStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Map a persist failure to 500. The memory write stays, but the disk
/// save failed, so the loss must not stay silent.
fn schema_persist_error(error: broker_config::ConfigError) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({
            "code": "INTERNAL_ERROR",
            "message": format!("cannot persist schemas: {error}")
        })),
    )
        .into_response()
}

/// Check one create body and build the record.
/// The body must be an object with `name`, `type` and `schema`.
/// `schema` accepts an object or a JSON string. The parser parses it
/// and checks its shape per kind. All limits apply here.
fn parse_schema_request(value: &serde_json::Value) -> Result<SchemaRecord, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "request body must be a JSON object".to_string())?;
    let name = obj
        .get("name")
        .or_else(|| obj.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if name.is_empty() {
        return Err("field `name` is required".to_string());
    }
    if name.len() > MAX_SCHEMA_NAME_LEN {
        return Err(format!(
            "field `name` holds {} chars, at most {MAX_SCHEMA_NAME_LEN}",
            name.len()
        ));
    }
    let kind = obj
        .get("type")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if kind.is_empty() {
        return Err("field `type` is required".to_string());
    }
    if !broker_config::is_known_schema_type(&kind) {
        return Err(format!("unknown schema type: {kind}"));
    }
    let description = obj
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    if description.len() > MAX_SCHEMA_DESC_LEN {
        return Err(format!(
            "field `description` holds {} bytes, at most {MAX_SCHEMA_DESC_LEN}",
            description.len()
        ));
    }
    let raw = obj
        .get("schema")
        .ok_or_else(|| "field `schema` is required".to_string())?;
    let parsed: serde_json::Value = if let Some(text) = raw.as_str() {
        if text.trim().is_empty() {
            return Err("field `schema` must not be empty".to_string());
        }
        if text.len() > MAX_SCHEMA_BYTES {
            return Err(format!(
                "field `schema` holds {} bytes, at most {MAX_SCHEMA_BYTES}",
                text.len()
            ));
        }
        serde_json::from_str(text)
            .map_err(|err| format!("field `schema` is not valid JSON: {err}"))?
    } else if raw.is_object() {
        let text = serde_json::to_string(raw).unwrap_or_default();
        if text.len() > MAX_SCHEMA_BYTES {
            return Err(format!(
                "field `schema` holds {} bytes, at most {MAX_SCHEMA_BYTES}",
                text.len()
            ));
        }
        raw.clone()
    } else {
        return Err("field `schema` must be an object".to_string());
    };
    broker_config::validate_schema_document(&kind, &parsed)
        .map_err(|reason| format!("field `schema` is invalid: {reason}"))?;
    Ok(SchemaRecord {
        name,
        schema_type: kind,
        description,
        schema: parsed,
    })
}

/// `GET /api/v5/schema_registry`: paged list with the standard envelope.
/// An empty registry returns an empty page. Unknown query keys are ignored.
/// Management plane only. Publish and deliver never touch this store.
///
/// Store wiring: the backing store is [`SchemaRegistryStore`], shared with
/// the kernel (`crates/broker-node/src/main.rs`, `serve_api` share).
/// The boot path seeds it from the registry. Only list, create and delete use it.
/// Declared `wired` in `tools/wiring.txt`.
///
/// Fire path cost: none. This route takes one short read lock per request.
/// Publish and deliver take no lock here.
pub async fn list_schemas_registry(State(state): State<ApiState>, params: PageParams) -> Response {
    let records = state.schema_registry.snapshot();
    let rows: Vec<serde_json::Value> = records.iter().map(SchemaRecord::to_json).collect();
    let count = rows.len();
    let page_rows = paginate(&rows, params.page, params.limit);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": page_rows,
            "meta": meta_page(params.page, params.limit, count),
        })),
    )
        .into_response()
}

/// `POST /api/v5/schema_registry`: checked create.
/// Parses the schema before it stores it. Duplicates fail cleanly.
/// Unknown query keys are ignored. Answers 201 with the stored row.
/// Management plane only. Publish and deliver never touch this store.
pub async fn create_schema_registry(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(found) => found,
        Err(err) => {
            return ApiError::BadRequest(format!("invalid schema body: {err}")).into_response();
        }
    };
    let entry = match parse_schema_request(&value) {
        Ok(found) => found,
        Err(reason) => return ApiError::BadRequest(reason).into_response(),
    };
    match state.schema_registry.insert(entry) {
        Ok(stored) => {
            if let Err(error) = state.schema_registry.persist() {
                return schema_persist_error(error);
            }
            (StatusCode::CREATED, Json(stored.to_json())).into_response()
        }
        Err(SchemaInsertError::Duplicate) => {
            ApiError::AlreadyExists("schema already exists".to_string()).into_response()
        }
        Err(SchemaInsertError::Full) => ApiError::BadRequest(format!(
            "schema registry holds {MAX_SCHEMAS} entries, at most {MAX_SCHEMAS}"
        ))
        .into_response(),
    }
}

/// `DELETE /api/v5/schema_registry/:name`: drop one schema.
/// Unknown names return 404 with the documented shape.
/// Delete removes the entry without a use check. Answers 204 on success.
/// Management plane only. Publish and deliver never touch this store.
///
/// Store wiring: same store as list and create, shared with the kernel.
/// The boot path seeds it. Only list, create and delete use it.
/// Declared `wired` in `tools/wiring.txt`.
///
/// Fire path cost: none. This route takes one short write lock.
/// Publish and deliver take no lock here.
// TODO(parity): which status and code does delete of a used schema return?
pub async fn delete_schema_registry(
    State(state): State<ApiState>,
    Path(name): Path<String>,
) -> Response {
    if state.schema_registry.get(&name).is_none() {
        return ApiError::NotFound(format!("schema {name:?} not found")).into_response();
    }
    if !state.schema_registry.remove(&name) {
        return ApiError::NotFound(format!("schema {name:?} not found")).into_response();
    }
    if let Err(error) = state.schema_registry.persist() {
        return schema_persist_error(error);
    }
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn avro_schema() -> serde_json::Value {
        serde_json::json!({
            "type": "record",
            "name": "Reading",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "temp", "type": "double"}
            ]
        })
    }

    fn json_schema() -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "temp": {"type": "number"}
            }
        })
    }

    #[test]
    fn insert_rejects_duplicate_and_reports_full() {
        let store = SchemaRegistryStore::new();
        let entry = SchemaRecord {
            name: "a".to_string(),
            schema_type: "json".to_string(),
            description: String::new(),
            schema: json_schema(),
        };
        assert!(store.insert(entry.clone()).is_ok());
        assert_eq!(
            store.insert(entry.clone()),
            Err(SchemaInsertError::Duplicate)
        );
        for i in 1..MAX_SCHEMAS {
            let numbered = SchemaRecord {
                name: format!("s-{i}"),
                schema_type: "json".to_string(),
                description: String::new(),
                schema: json_schema(),
            };
            assert!(store.insert(numbered).is_ok());
        }
        assert_eq!(store.snapshot().len(), MAX_SCHEMAS);
        let extra = SchemaRecord {
            name: "over-cap".to_string(),
            schema_type: "json".to_string(),
            description: String::new(),
            schema: json_schema(),
        };
        assert_eq!(store.insert(extra), Err(SchemaInsertError::Full));
        assert!(store.remove("a"));
        assert!(!store.remove("a"));
    }

    #[test]
    fn snapshot_sorts_by_name_for_stable_reads() {
        let store = SchemaRegistryStore::new();
        for name in ["b", "a"] {
            assert!(store
                .insert(SchemaRecord {
                    name: name.to_string(),
                    schema_type: "json".to_string(),
                    description: String::new(),
                    schema: json_schema(),
                })
                .is_ok());
        }
        let names: Vec<String> = store.snapshot().iter().map(|r| r.name.clone()).collect();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);
    }

    #[test]
    fn parse_accepts_object_and_string_shapes() {
        let from_object = parse_schema_request(&serde_json::json!({
            "name": "s1",
            "type": "avro",
            "schema": avro_schema(),
        }))
        .expect("object schema parses");
        assert_eq!(from_object.name, "s1");
        let text = serde_json::to_string(&avro_schema()).expect("schema text");
        let from_text = parse_schema_request(&serde_json::json!({
            "name": "s2",
            "type": "avro",
            "schema": text,
        }))
        .expect("string schema parses");
        assert_eq!(from_text.schema, avro_schema());
    }

    #[test]
    fn parse_rejects_bad_type_and_bad_shapes() {
        assert!(parse_schema_request(&serde_json::json!({
            "name": "s",
            "type": "nope",
            "schema": json_schema(),
        }))
        .is_err());
        assert!(parse_schema_request(&serde_json::json!({
            "name": "s",
            "type": "json",
            "schema": {"foo": 1},
        }))
        .is_err());
        assert!(parse_schema_request(&serde_json::json!({
            "name": "s",
            "type": "avro",
            "schema": {"type": "record", "name": "", "fields": []},
        }))
        .is_err());
    }

    #[test]
    fn export_round_trips_through_checked_config() {
        let store = SchemaRegistryStore::new();
        assert!(store
            .insert(SchemaRecord {
                name: "s1".to_string(),
                schema_type: "json".to_string(),
                description: "note".to_string(),
                schema: json_schema(),
            })
            .is_ok());
        let conf = store.export_conf();
        conf.validate().expect("exported schemas must validate");
        assert_eq!(conf.schemas.len(), 1);
        assert_eq!(conf.schemas[0].name, "s1");
    }
}
