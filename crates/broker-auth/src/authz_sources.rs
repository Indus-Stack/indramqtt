//! Ordered authorization source registry for the management API.
//!
//! Covers the registry behind `GET /authorization/sources` (ordered list),
//! `POST /authorization/sources` (register one source),
//! `GET /authorization/sources/:type` (one source's config and health),
//! `PUT /authorization/sources/:type` (replace one source) and
//! `POST /authorization/sources/:type/metrics/reset` (zero one source's
//! live counters), plus the built-in
//! catch-all clear (`DELETE
//! /authorization/sources/built_in_database/rules`, which empties the
//! built-in rule table). Single-node, management-plane only: handlers
//! clone at most the capped list once per request under a short lock;
//! the publish and subscribe authorization consults read the built-in
//! rule table (`MemoryAuth` in `crate::lib`, driven by the publish event
//! in `crates/broker-node/src/main.rs`), never this registry's write
//! lock, and publish/deliver never touch this store.
//!
//! Store bounds (both stated here and enforced below):
//! - at most [`broker_config::MAX_AUTHZ_SOURCES`] entries (8: the
//!   management plane snapshots the whole registry per request and the
//!   decision path scans at most the capped entries, so the registry must
//!   stay small and bounded; 8 covers the documented backend families
//!   with headroom while keeping per-request clones cheap); creates past
//!   the cap are rejected instead of growing without limit;
//! - `type` values name a known source type and are unique within the
//!   registry (a duplicate type is rejected, never silently merged);
//! - the stored backend config is at most 8 KiB of JSON, so one entry
//!   cannot balloon memory;
//! - reads clone at most the capped list once per request under a short
//!   lock; decisions read a lock-free snapshot.
//!
//! Only the built-in database executes in the parity waves; every other
//! source type's config is stored and reported, never claimed as live.
//! An empty registry lists as an empty collection, never an error.
// TODO(parity): is 8 the documented source cap, and which source types
// plus per-source fields (status, order, health) does the spec require?
// The rulebook does not decide the exact set; the current choice mirrors
// the authenticator backend families and reports type/enable only,
// omitting status fields the broker cannot supply.

use broker_config::{
    is_known_authz_source_type, AuthzSourceEntry as ConfEntry, AuthzSourcesConf, ConfigRegistry,
    MAX_AUTHZ_SOURCES,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Longest accepted backend-config JSON document (8 KiB).
/// Reason (measured cost): configs ride the per-request registry clone,
/// so one entry cannot balloon memory; 8 KiB caps the worst-case snapshot
/// at 8 entries x 8 KiB = 64 KiB, keeping per-request clones cheap, and
/// larger documents are rejected instead of growing without limit.
const MAX_CONFIG_LEN: usize = 8192;

/// One ordered registry slot: the source key plus its enable flag and
/// opaque backend config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthzSourceEntry {
    pub source_type: String,
    pub enable: bool,
    pub config: serde_json::Value,
}

/// Why [`AuthzSourceRegistry::insert`] refused an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceInsertError {
    /// The `type` is already stored.
    Duplicate,
    /// The store holds [`MAX_AUTHZ_SOURCES`] entries.
    Full,
    /// The entry failed validation (message names the field).
    Invalid(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Ordered authorization source registry behind one short lock plus a
/// lock-free read snapshot for the decision path.
///
/// Management writes take the write lock, append the stored entry and
/// publish a fresh snapshot; readers load the snapshot without taking
/// the write lock and never block on a management write beyond one `Arc`
/// swap. Publish and deliver never touch either.
///
/// Counter bound (stated here and enforced below): at most
/// [`MAX_AUTHZ_SOURCES`] counter records (8: one per source `type`, so
/// the map can never outgrow the registry); each record holds exactly
/// three `AtomicU64` values and no list, queue or history ever
/// accumulates. Counters are memory-only and reset to zero on seeding
/// (health is computed live and never persists).
pub struct AuthzSourceRegistry {
    inner: parking_lot::RwLock<Vec<AuthzSourceEntry>>,
    snapshot: arc_swap::ArcSwap<Vec<AuthzSourceEntry>>,
    registry: parking_lot::RwLock<Option<Arc<ConfigRegistry>>>,
    save_lock: parking_lot::Mutex<()>,
    counters: parking_lot::RwLock<HashMap<String, Arc<AuthzSourceCounters>>>,
}

/// Why [`AuthzSourceRegistry::reorder`] refused a new order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceReorderError {
    /// The body failed structural validation (message names the problem).
    Invalid(String),
    /// One or more supplied types are not stored (lists the unknown types).
    Unknown(Vec<String>),
    /// The order omits stored types, so applying it would drop entries
    /// (lists the missing types).
    Incomplete(Vec<String>),
    /// The order names the same type twice.
    Duplicate(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Why [`AuthzSourceRegistry::relocate`] refused a move.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceMoveError {
    /// No stored source carries the requested `type`.
    NotFound,
    /// The target position is outside the stored order.
    OutOfRange(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Why [`AuthzSourceRegistry::remove`] refused a delete.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceRemoveError {
    /// No stored source carries the requested `type`.
    NotFound,
    /// Deleting the last remaining source would leave authorization open
    /// (an empty registry authorizes nothing explicitly), so the delete
    /// is refused instead of opening the broker.
    Last(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Why [`AuthzSourceRegistry::update`] refused a replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SourceUpdateError {
    /// No stored source carries the requested `type`.
    NotFound,
    /// The replacement failed validation (message names the field).
    Invalid(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}

/// Per-source live counters behind fixed-width atomics.
///
/// One record per source `type`: `total` counts every authorization
/// verdict attributed to that type, split into `allow` (the source
/// authorized) and `deny` (the source refused). All reads are lock-free
/// atomic loads and all increments are lock-free atomic adds; the map
/// itself is only looked up under one short read lock to clone the
/// `Arc`. Counters are memory-only and restart at zero (configs persist
/// across a restart through the config registry, counters do not).
/// Management-plane only: publish and deliver never touch these
/// counters, and no publish/subscribe consult increments them today.
// TODO(parity): which counter keys plus which broker event attributes
// them does the spec require (allow/deny split, per-decision versus
// per-request)? The rulebook does not decide the exact set; the current
// choice counts explicit record calls only, until the checker pins it.
#[derive(Debug, Default)]
pub struct AuthzSourceCounters {
    allow: AtomicU64,
    deny: AtomicU64,
    total: AtomicU64,
}

/// Lock-free snapshot of one source's counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuthzSourceCounterSnapshot {
    pub allow: u64,
    pub deny: u64,
    pub total: u64,
}

impl AuthzSourceCounters {
    fn snapshot(&self) -> AuthzSourceCounterSnapshot {
        AuthzSourceCounterSnapshot {
            allow: self.allow.load(Ordering::Relaxed),
            deny: self.deny.load(Ordering::Relaxed),
            total: self.total.load(Ordering::Relaxed),
        }
    }

    fn record_allow(&self) {
        self.allow.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    fn record_deny(&self) {
        self.deny.fetch_add(1, Ordering::Relaxed);
        self.total.fetch_add(1, Ordering::Relaxed);
    }

    /// Zero every counter. Lock-free atomic stores; recorders keep
    /// incrementing with lock-free adds and publish/deliver never touch
    /// these counters.
    fn reset(&self) {
        self.allow.store(0, Ordering::Relaxed);
        self.deny.store(0, Ordering::Relaxed);
        self.total.store(0, Ordering::Relaxed);
    }
}

impl AuthzSourceRegistry {
    /// Default registry: only the built-in database source (the one
    /// backend that executes, so a fresh broker authorizes against its
    /// local rule table without operator setup).
    pub fn new() -> Self {
        Self::from_snapshot(&AuthzSourcesConf::default())
    }

    /// Seed from a validated snapshot root. Memory-only. An empty root
    /// yields an empty registry, which lists as an empty collection.
    pub fn from_snapshot(conf: &AuthzSourcesConf) -> Self {
        let registry = Self {
            inner: parking_lot::RwLock::new(Vec::new()),
            snapshot: arc_swap::ArcSwap::from(Arc::new(Vec::new())),
            registry: parking_lot::RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
            counters: parking_lot::RwLock::new(HashMap::new()),
        };
        registry.seed_from_snapshot(conf);
        registry
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. Kernel boot path; defaults yield the
    /// built-in database source.
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let store = Self::from_snapshot(&registry.snapshot().authz_sources);
        *store.registry.write() = Some(Arc::clone(registry));
        store
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    pub fn seed_from_registry(&self, registry: &Arc<ConfigRegistry>) {
        let entries = registry
            .snapshot()
            .authz_sources
            .sources
            .iter()
            .filter_map(conf_to_entry)
            .collect::<Vec<_>>();
        *self.inner.write() = entries.clone();
        self.snapshot.store(Arc::new(entries.clone()));
        // Counters never persist: a re-seed restarts every source at zero.
        let mut counters = self.counters.write();
        counters.clear();
        for entry in &entries {
            counters
                .entry(entry.source_type.clone())
                .or_insert_with(|| Arc::new(AuthzSourceCounters::default()));
        }
        *self.registry.write() = Some(Arc::clone(registry));
    }

    fn seed_from_snapshot(&self, conf: &AuthzSourcesConf) {
        let entries = conf
            .sources
            .iter()
            .filter_map(conf_to_entry)
            .collect::<Vec<_>>();
        *self.inner.write() = entries.clone();
        self.snapshot.store(Arc::new(entries.clone()));
        // Counters never persist: a restart (or re-seed) starts every
        // source at zero. Bounded by the source cap: one record per
        // seeded type, never more than MAX_AUTHZ_SOURCES entries.
        let mut counters = self.counters.write();
        counters.clear();
        for entry in &entries {
            counters
                .entry(entry.source_type.clone())
                .or_insert_with(|| Arc::new(AuthzSourceCounters::default()));
        }
    }

    /// Ordered snapshot of the registry in stored order. Management-plane
    /// read only; clones at most the capped list under one short lock.
    pub fn list(&self) -> Vec<AuthzSourceEntry> {
        self.inner.read().clone()
    }

    /// Lock-free snapshot for readers: one `Arc` load, no write lock, at
    /// most the capped entries to scan. Publish and deliver never call
    /// this on the hot path; decisions read snapshots.
    pub fn snapshot(&self) -> Arc<Vec<AuthzSourceEntry>> {
        self.snapshot.load_full()
    }

    /// One entry by source `type` (`None` when unknown). Management-plane
    /// read only; clones at most one entry under a short lock.
    pub fn get(&self, source_type: &str) -> Option<AuthzSourceEntry> {
        self.inner
            .read()
            .iter()
            .find(|e| e.source_type == source_type)
            .cloned()
    }

    /// Whether the built-in database source may execute: true when the
    /// registry holds an enabled `built_in_database` slot. Consulted by
    /// the rule-table tasks once they land; management-plane only today.
    pub fn built_in_available(&self) -> bool {
        let snap = self.snapshot.load();
        snap.iter()
            .any(|e| e.source_type == "built_in_database" && e.enable)
    }

    /// Register one source. Fails cleanly on duplicate `type` (caller
    /// maps to `ALREADY_EXISTS`), a full registry (caller maps to
    /// `BAD_REQUEST`) or an invalid entry (caller maps to `BAD_REQUEST`
    /// or `NOT_FOUND` for an unknown type). Validated all-before-apply:
    /// the candidate root is validated before it can become visible, so
    /// invalid state is never observable. Persists through the registry
    /// when one is attached; a commit or save failure surfaces as
    /// [`SourceInsertError::Persist`] and the in-memory update stays
    /// (commit precedes the atomic save, mirroring the credential
    /// store).
    pub fn insert(&self, entry: AuthzSourceEntry) -> Result<AuthzSourceEntry, SourceInsertError> {
        validate_entry(&entry)?;
        let _guard = self.save_lock.lock();
        let conf = {
            let mut current = self.inner.write();
            if current.iter().any(|e| e.source_type == entry.source_type) {
                return Err(SourceInsertError::Duplicate);
            }
            if current.len() >= MAX_AUTHZ_SOURCES {
                return Err(SourceInsertError::Full);
            }
            let mut next = current.clone();
            next.push(entry.clone());
            let conf = export_conf(&next);
            conf.validate()
                .map_err(|e| SourceInsertError::Invalid(e.to_string()))?;
            current.push(entry.clone());
            self.snapshot.store(Arc::new(current.clone()));
            // One bounded counter record per source type; a re-created
            // type after a delete restarts at zero.
            self.counters
                .write()
                .entry(entry.source_type.clone())
                .or_insert_with(|| Arc::new(AuthzSourceCounters::default()));
            conf
        };
        if let Some(registry) = self.registry.read().clone() {
            registry
                .commit_authz_sources(conf)
                .map_err(|e| SourceInsertError::Persist(e.to_string()))?;
            registry
                .save()
                .map_err(|e| SourceInsertError::Persist(e.to_string()))?;
        }
        Ok(entry)
    }

    /// Replace the whole source evaluation order with the supplied
    /// ordered type list.
    ///
    /// Validate-all-before-apply: the full list is checked (non-empty
    /// known types, no duplicates, every type stored, no stored type
    /// omitted) before anything becomes visible, so a partial or
    /// unknown-type list applies nothing. On success the stored order,
    /// the lock-free snapshot and the persisted registry all move
    /// together. Management-plane only: takes the write lock once per
    /// request; the publish and subscribe consults read the built-in
    /// rule table, never this registry, and publish/deliver never touch
    /// this store.
    // TODO(parity): is the documented order body a bare array of
    // `{"type": ...}` objects (as implemented here, mirroring the
    // sibling authenticator order), an array of bare type strings, or an
    // object carrying the list? The rulebook does not decide the exact
    // envelope; the current choice mirrors the sibling order until the
    // checker pins it.
    pub fn reorder(&self, types: Vec<String>) -> Result<(), SourceReorderError> {
        for source_type in &types {
            if source_type.trim().is_empty() {
                return Err(SourceReorderError::Invalid(
                    "field `type` must not be empty".to_string(),
                ));
            }
            if !is_known_authz_source_type(source_type) {
                return Err(SourceReorderError::Unknown(vec![source_type.clone()]));
            }
        }
        let mut seen = std::collections::HashSet::with_capacity(types.len());
        for source_type in &types {
            if !seen.insert(source_type.clone()) {
                return Err(SourceReorderError::Duplicate(format!(
                    "order names type {source_type:?} more than once"
                )));
            }
        }
        let _guard = self.save_lock.lock();
        let reordered = {
            let current = self.inner.read();
            let unknown: Vec<String> = types
                .iter()
                .filter(|t| !current.iter().any(|e| &e.source_type == *t))
                .cloned()
                .collect();
            let missing: Vec<String> = current
                .iter()
                .filter(|e| !types.contains(&e.source_type))
                .map(|e| e.source_type.clone())
                .collect();
            if !unknown.is_empty() && !missing.is_empty() {
                return Err(SourceReorderError::Invalid(format!(
                    "unknown authorization source type(s): {}; order omits stored type(s): {}",
                    unknown.join(", "),
                    missing.join(", "),
                )));
            }
            if !unknown.is_empty() {
                return Err(SourceReorderError::Unknown(unknown));
            }
            if !missing.is_empty() {
                return Err(SourceReorderError::Incomplete(missing));
            }
            let mut next = Vec::with_capacity(current.len());
            for source_type in &types {
                let entry = current
                    .iter()
                    .find(|e| &e.source_type == source_type)
                    .cloned()
                    .expect("types validated against current registry");
                next.push(entry);
            }
            let conf = export_conf(&next);
            conf.validate()
                .map_err(|e| SourceReorderError::Invalid(e.to_string()))?;
            next
        };
        let conf = export_conf(&reordered);
        {
            let mut current = self.inner.write();
            *current = reordered.clone();
            self.snapshot.store(Arc::new(reordered));
        }
        if let Some(registry) = self.registry.read().clone() {
            registry
                .commit_authz_sources(conf)
                .map_err(|e| SourceReorderError::Persist(e.to_string()))?;
            registry
                .save()
                .map_err(|e| SourceReorderError::Persist(e.to_string()))?;
        }
        Ok(())
    }

    /// Move one stored source to a zero-based position and report the new
    /// order.
    ///
    /// Unknown types fail with `NotFound` and apply nothing. A target at
    /// or past the registry length fails with `OutOfRange` and moves
    /// nothing. Moving to the entry's current index is a no-op success.
    /// On success the stored order, the lock-free snapshot and the
    /// persisted registry all move together, so the order persists across
    /// a restart. Management-plane only: one short write lock per request
    /// (plus the registry save lock on persist); the publish and
    /// subscribe consults read the built-in rule table, never this
    /// registry, and publish/deliver never touch this store. A move never
    /// grows the list, it only reorders at most the capped entries.
    pub fn relocate(
        &self,
        source_type: &str,
        position: usize,
    ) -> Result<Vec<AuthzSourceEntry>, SourceMoveError> {
        let _guard = self.save_lock.lock();
        let (reordered, conf, moved) = {
            let mut current = self.inner.write();
            let Some(from) = current.iter().position(|e| e.source_type == source_type) else {
                return Err(SourceMoveError::NotFound);
            };
            if position >= current.len() {
                return Err(SourceMoveError::OutOfRange(format!(
                    "position {position} is out of range for {} entries (valid 0..{})",
                    current.len(),
                    current.len().saturating_sub(1),
                )));
            }
            if from == position {
                return Ok(current.clone());
            }
            let entry = current.remove(from);
            current.insert(position, entry);
            let next = current.clone();
            let conf = export_conf(&next);
            conf.validate()
                .map_err(|e| SourceMoveError::Persist(e.to_string()))?;
            self.snapshot.store(Arc::new(next.clone()));
            (next, conf, true)
        };
        if moved {
            if let Some(registry) = self.registry.read().clone() {
                registry
                    .commit_authz_sources(conf)
                    .map_err(|e| SourceMoveError::Persist(e.to_string()))?;
                registry
                    .save()
                    .map_err(|e| SourceMoveError::Persist(e.to_string()))?;
            }
        }
        Ok(reordered)
    }

    /// Remove the source stored under `type`, returning it.
    ///
    /// Validate-all-before-apply: unknown types fail with `NotFound` and
    /// apply nothing. Deleting the last remaining source is refused with
    /// `Last` instead of leaving authorization open. On success the
    /// stored order, the lock-free snapshot and the persisted registry
    /// all move together, and the freed `type` may be re-created.
    /// Management-plane only: one short write lock per request (plus the
    /// registry save lock on persist); the publish and subscribe
    /// consults read the built-in rule table, never this registry.
    // TODO(parity): is "last" the last entry of any kind, or the last
    // enabled entry / last live backend? The rulebook does not decide
    // the exact refusal set; the current choice refuses deleting the
    // sole remaining entry (any kind) as the conservative fail-closed
    // answer until the checker pins it.
    pub fn remove(&self, source_type: &str) -> Result<AuthzSourceEntry, SourceRemoveError> {
        let _guard = self.save_lock.lock();
        let (removed, conf) = {
            let mut current = self.inner.write();
            let Some(pos) = current.iter().position(|e| e.source_type == source_type) else {
                return Err(SourceRemoveError::NotFound);
            };
            if current.len() == 1 {
                return Err(SourceRemoveError::Last(
                    "cannot delete the last authorization source: authorization must not be left open"
                        .to_string(),
                ));
            }
            let removed = current.remove(pos);
            let next = current.clone();
            let conf = export_conf(&next);
            conf.validate()
                .map_err(|e| SourceRemoveError::Persist(e.to_string()))?;
            self.snapshot.store(Arc::new(next));
            self.counters.write().remove(source_type);
            (removed, conf)
        };
        if let Some(registry) = self.registry.read().clone() {
            registry
                .commit_authz_sources(conf)
                .map_err(|e| SourceRemoveError::Persist(e.to_string()))?;
            registry
                .save()
                .map_err(|e| SourceRemoveError::Persist(e.to_string()))?;
        }
        Ok(removed)
    }

    /// Replace the entry stored under `type` with `entry` (whose
    /// `source_type` must equal `type`).
    ///
    /// Validate-all-before-apply: the candidate is validated before
    /// anything becomes visible, so a bad replacement applies nothing.
    /// The stored position is preserved. On success the entry, the
    /// lock-free snapshot and the persisted registry all move together.
    /// Counters survive config/enable replaces: they are live, not
    /// config. Management-plane only: one short write lock per request
    /// (plus the registry save lock on persist); the publish and
    /// subscribe authorization consults read the built-in rule table,
    /// never this registry, and publish/deliver never touch this store.
    // TODO(parity): may the update rename the entry (change its `type`)
    // or only replace the config/enable pair in place? The rulebook does
    // not decide the exact PUT field set; the current choice replaces the
    // whole entry in place (same `type`) after full validation until the
    // checker pins the shape.
    pub fn update(
        &self,
        source_type: &str,
        entry: AuthzSourceEntry,
    ) -> Result<AuthzSourceEntry, SourceUpdateError> {
        if entry.source_type != source_type {
            return Err(SourceUpdateError::Invalid(
                "field `type` must match the path type".to_string(),
            ));
        }
        validate_entry(&entry).map_err(|e| match e {
            SourceInsertError::Invalid(msg) => SourceUpdateError::Invalid(msg),
            SourceInsertError::Duplicate => {
                SourceUpdateError::Invalid("field `type` is duplicated".to_string())
            }
            SourceInsertError::Full => {
                SourceUpdateError::Invalid("authorization source registry is full".to_string())
            }
            SourceInsertError::Persist(msg) => SourceUpdateError::Persist(msg),
        })?;
        let _guard = self.save_lock.lock();
        let updated = {
            let mut current = self.inner.write();
            let Some(pos) = current.iter().position(|e| e.source_type == source_type) else {
                return Err(SourceUpdateError::NotFound);
            };
            let mut next = current.clone();
            next[pos] = entry.clone();
            let conf = export_conf(&next);
            conf.validate()
                .map_err(|e| SourceUpdateError::Invalid(e.to_string()))?;
            *current = next.clone();
            self.snapshot.store(Arc::new(next));
            // Counters survive config/enable replaces: they are live, not
            // config. Ensure the record exists for registries seeded
            // before counters landed.
            self.counters
                .write()
                .entry(source_type.to_string())
                .or_insert_with(|| Arc::new(AuthzSourceCounters::default()));
            conf
        };
        if let Some(registry) = self.registry.read().clone() {
            registry
                .commit_authz_sources(updated)
                .map_err(|e| SourceUpdateError::Persist(e.to_string()))?;
            registry
                .save()
                .map_err(|e| SourceUpdateError::Persist(e.to_string()))?;
        }
        Ok(entry)
    }

    /// Live counters for one source `type` (`None` when unknown).
    ///
    /// Management-plane read only: lock-free atomic loads, no write lock,
    /// no allocation beyond the returned copy. Publish and deliver never
    /// call this.
    pub fn counter_snapshot(&self, source_type: &str) -> Option<AuthzSourceCounterSnapshot> {
        self.counters
            .read()
            .get(source_type)
            .map(|counters| counters.snapshot())
    }

    /// Record one allow verdict for `type`.
    ///
    /// Management-plane only: one short read lock to clone the counter
    /// `Arc` plus two lock-free atomic adds; unknown types apply nothing
    /// (no accumulation). Publish and deliver never call this.
    pub fn record_allow(&self, source_type: &str) {
        if let Some(counters) = self.counters.read().get(source_type).cloned() {
            counters.record_allow();
        }
    }

    /// Record one deny verdict for `type`.
    ///
    /// Management-plane only: same cost and bounds as
    /// [`AuthzSourceRegistry::record_allow`]. Publish and deliver never
    /// call this.
    pub fn record_deny(&self, source_type: &str) {
        if let Some(counters) = self.counters.read().get(source_type).cloned() {
            counters.record_deny();
        }
    }

    /// Zero the live counters for one source `type`.
    ///
    /// Returns `false` when `type` names no stored source, so the caller
    /// can answer 404 instead of resetting nothing; unknown types never
    /// create a counter record (no accumulation). A stored entry whose
    /// counter record is missing (seeded before counters landed) is
    /// ensured at zero. Management-plane only: one short read lock to
    /// clone the counter `Arc` plus lock-free atomic stores; recorders
    /// keep incrementing concurrently and publish/deliver never call
    /// this. The reset itself does not persist: only the zeroed counter
    /// values remain, and the next verdict counts up from zero.
    pub fn reset_counters(&self, source_type: &str) -> bool {
        if self.get(source_type).is_none() {
            return false;
        }
        if let Some(counters) = self.counters.read().get(source_type).cloned() {
            counters.reset();
        } else {
            self.counters
                .write()
                .entry(source_type.to_string())
                .or_insert_with(|| Arc::new(AuthzSourceCounters::default()));
        }
        true
    }

    /// Number of stored entries.
    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    /// Whether the registry holds no entries.
    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

impl Default for AuthzSourceRegistry {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_entry(entry: &AuthzSourceEntry) -> Result<(), SourceInsertError> {
    if entry.source_type.trim().is_empty() {
        return Err(SourceInsertError::Invalid(
            "field `type` must not be empty".to_string(),
        ));
    }
    if !is_known_authz_source_type(&entry.source_type) {
        return Err(SourceInsertError::Invalid(format!(
            "field `type` {:?} is unknown",
            entry.source_type
        )));
    }
    let rendered = entry.config.to_string();
    if rendered.len() > MAX_CONFIG_LEN {
        return Err(SourceInsertError::Invalid(
            "field `config` is too large".to_string(),
        ));
    }
    Ok(())
}

fn export_conf(entries: &[AuthzSourceEntry]) -> AuthzSourcesConf {
    AuthzSourcesConf {
        sources: entries
            .iter()
            .map(|e| ConfEntry {
                source_type: e.source_type.clone(),
                enable: e.enable,
                config: if e.config.is_null() {
                    String::new()
                } else {
                    e.config.to_string()
                },
            })
            .collect(),
    }
}

fn conf_to_entry(conf: &ConfEntry) -> Option<AuthzSourceEntry> {
    if conf.source_type.trim().is_empty() || !is_known_authz_source_type(&conf.source_type) {
        return None;
    }
    let config = if conf.config.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_str(&conf.config).ok()?
    };
    Some(AuthzSourceEntry {
        source_type: conf.source_type.clone(),
        enable: conf.enable,
        config,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(source_type: &str) -> AuthzSourceEntry {
        AuthzSourceEntry {
            source_type: source_type.to_string(),
            enable: true,
            config: serde_json::Value::Null,
        }
    }

    #[test]
    fn default_registry_holds_only_the_built_in_source() {
        let registry = AuthzSourceRegistry::new();
        assert_eq!(registry.len(), 1);
        let stored = registry.get("built_in_database").expect("built-in present");
        assert!(stored.enable);
        assert!(registry.built_in_available());
        let snap = registry.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].source_type, "built_in_database");
    }

    #[test]
    fn empty_snapshot_lists_empty_without_error() {
        let registry = AuthzSourceRegistry::from_snapshot(&AuthzSourcesConf { sources: vec![] });
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(registry.list().is_empty());
        assert!(registry.snapshot().is_empty());
        assert!(!registry.built_in_available());
    }

    #[test]
    fn insert_appends_in_order_and_rejects_duplicates() {
        let registry = AuthzSourceRegistry::new();
        registry.insert(entry("file")).expect("file insert");
        let ids: Vec<_> = registry
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(
            ids,
            vec!["built_in_database".to_string(), "file".to_string()]
        );
        // Snapshot moves with the store.
        let snap: Vec<_> = registry
            .snapshot()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(ids, snap);
        assert_eq!(
            registry.insert(entry("file")),
            Err(SourceInsertError::Duplicate)
        );
        // Stored order is unchanged by the rejected duplicate.
        let ids: Vec<_> = registry
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(
            ids,
            vec!["built_in_database".to_string(), "file".to_string()]
        );
    }

    #[test]
    fn insert_rejects_unknown_type_and_oversized_config() {
        let registry = AuthzSourceRegistry::new();
        assert!(matches!(
            registry.insert(entry("nowhere")),
            Err(SourceInsertError::Invalid(_))
        ));
        let mut bad = entry("file");
        bad.source_type = "  ".to_string();
        assert!(matches!(
            registry.insert(bad),
            Err(SourceInsertError::Invalid(_))
        ));
        let mut big = entry("file");
        big.config = serde_json::Value::String("x".repeat(MAX_CONFIG_LEN + 1));
        assert!(matches!(
            registry.insert(big),
            Err(SourceInsertError::Invalid(_))
        ));
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn insert_enforces_source_cap() {
        let registry = AuthzSourceRegistry::from_snapshot(&AuthzSourcesConf { sources: vec![] });
        for source_type in broker_config::KNOWN_AUTHZ_SOURCE_TYPES
            .iter()
            .take(MAX_AUTHZ_SOURCES)
        {
            registry.insert(entry(source_type)).expect("fill to cap");
        }
        assert_eq!(registry.len(), MAX_AUTHZ_SOURCES);
        // The next documented type is not stored yet, so the cap (not a
        // duplicate) refuses it.
        assert_eq!(
            registry.insert(entry(
                broker_config::KNOWN_AUTHZ_SOURCE_TYPES[MAX_AUTHZ_SOURCES]
            )),
            Err(SourceInsertError::Full)
        );
        assert_eq!(registry.len(), MAX_AUTHZ_SOURCES);
    }

    #[test]
    fn registry_survives_registry_reload() {
        let dir = std::env::temp_dir().join(format!(
            "authz-sources-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        // Fresh defaults already persist the built-in source.
        let restarted = AuthzSourceRegistry::from_registry(&registry);
        assert!(restarted.get("built_in_database").is_some());
        restarted.insert(entry("file")).expect("persist file");
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        drop(restarted);
        drop(registry);
        let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = AuthzSourceRegistry::from_registry(&reloaded);
        let ids: Vec<_> = restarted
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(
            ids,
            vec!["built_in_database".to_string(), "file".to_string()]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    fn order(types: &[&str]) -> Vec<String> {
        types.iter().map(|t| t.to_string()).collect()
    }

    fn stored_order(registry: &AuthzSourceRegistry) -> Vec<String> {
        registry
            .list()
            .iter()
            .map(|e| e.source_type.clone())
            .collect()
    }

    #[test]
    fn reorder_swaps_evaluation_order_and_rejects_bad_without_change() {
        let registry = AuthzSourceRegistry::new();
        registry.insert(entry("file")).expect("file insert");
        assert_eq!(
            stored_order(&registry),
            vec!["built_in_database".to_string(), "file".to_string()]
        );

        registry
            .reorder(order(&["file", "built_in_database"]))
            .expect("reorder applies");
        assert_eq!(
            stored_order(&registry),
            vec!["file".to_string(), "built_in_database".to_string()]
        );
        // The lock-free snapshot moves with the store.
        let snap: Vec<_> = registry
            .snapshot()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(snap, stored_order(&registry));

        // Unknown types, partial lists, duplicates and malformed bodies
        // apply nothing.
        assert!(matches!(
            registry.reorder(order(&["file", "nowhere"])),
            Err(SourceReorderError::Unknown(_))
        ));
        assert!(matches!(
            registry.reorder(order(&["nowhere"])),
            Err(SourceReorderError::Unknown(_))
        ));
        assert!(matches!(
            registry.reorder(order(&["file"])),
            Err(SourceReorderError::Incomplete(_))
        ));
        assert!(matches!(
            registry.reorder(order(&["file", "file", "built_in_database"])),
            Err(SourceReorderError::Duplicate(_))
        ));
        assert!(matches!(
            registry.reorder(order(&["", "file"])),
            Err(SourceReorderError::Invalid(_))
        ));
        assert_eq!(
            stored_order(&registry),
            vec!["file".to_string(), "built_in_database".to_string()]
        );
    }

    #[test]
    fn remove_deletes_one_source_and_refuses_the_last() {
        let registry = AuthzSourceRegistry::new();
        registry.insert(entry("file")).expect("file insert");

        let removed = registry.remove("file").expect("delete applies");
        assert_eq!(removed.source_type, "file");
        assert_eq!(
            stored_order(&registry),
            vec!["built_in_database".to_string()]
        );
        // The lock-free snapshot moves with the store.
        assert_eq!(registry.snapshot().len(), 1);

        // Unknown types are a miss and apply nothing.
        assert_eq!(registry.remove("nowhere"), Err(SourceRemoveError::NotFound));
        assert_eq!(
            stored_order(&registry),
            vec!["built_in_database".to_string()]
        );

        // Deleting the last live source is refused instead of leaving
        // authorization open.
        assert!(matches!(
            registry.remove("built_in_database"),
            Err(SourceRemoveError::Last(_))
        ));
        assert_eq!(
            stored_order(&registry),
            vec!["built_in_database".to_string()]
        );
        assert!(registry.built_in_available());
    }

    #[test]
    fn update_replaces_in_place_and_counters_survive_until_reset() {
        use super::SourceUpdateError;

        let registry = AuthzSourceRegistry::new();
        registry.insert(entry("file")).expect("file insert");
        assert_eq!(
            stored_order(&registry),
            vec!["built_in_database".to_string(), "file".to_string()]
        );

        // Unknown types miss and a mismatched entry type is invalid;
        // both apply nothing.
        assert_eq!(
            registry.update("http", entry("http")),
            Err(SourceUpdateError::NotFound)
        );
        let mut mismatched = entry("http");
        assert_eq!(
            registry.update("file", mismatched.clone()),
            Err(SourceUpdateError::Invalid(
                "field `type` must match the path type".to_string()
            ))
        );
        mismatched.source_type = "  ".to_string();
        assert!(matches!(
            registry.update("file", mismatched),
            Err(SourceUpdateError::Invalid(_))
        ));
        assert_eq!(
            stored_order(&registry),
            vec!["built_in_database".to_string(), "file".to_string()]
        );

        // A valid replacement applies in place: the stored position is
        // preserved and the snapshot moves with the store.
        let updated = AuthzSourceEntry {
            source_type: "file".to_string(),
            enable: false,
            config: serde_json::json!({"path": "/etc/authz/after.conf"}),
        };
        let stored = registry
            .update("file", updated.clone())
            .expect("update applies");
        assert_eq!(stored, updated);
        assert_eq!(
            stored_order(&registry),
            vec!["built_in_database".to_string(), "file".to_string()]
        );
        let snap: Vec<_> = registry
            .snapshot()
            .iter()
            .map(|e| e.source_type.clone())
            .collect();
        assert_eq!(snap, stored_order(&registry));
        let reread = registry.get("file").expect("updated entry reads back");
        assert!(!reread.enable);
        assert_eq!(
            reread.config["path"],
            serde_json::json!("/etc/authz/after.conf")
        );

        // Counters move on explicit records, survive a config replace,
        // and zero on reset; unknown types move and reset nothing.
        registry.record_allow("file");
        registry.record_deny("file");
        let moved = registry.counter_snapshot("file").expect("counters move");
        assert_eq!(moved.allow, 1);
        assert_eq!(moved.deny, 1);
        assert_eq!(moved.total, 2);
        registry
            .update("file", updated.clone())
            .expect("replace keeps counters");
        let kept = registry.counter_snapshot("file").expect("counters survive");
        assert_eq!(kept.total, 2);
        registry.record_allow("nowhere");
        assert!(registry.counter_snapshot("nowhere").is_none());
        assert!(!registry.reset_counters("nowhere"));
        assert!(registry.reset_counters("file"));
        let zeroed = registry.counter_snapshot("file").expect("counters zero");
        assert_eq!(zeroed.allow, 0);
        assert_eq!(zeroed.deny, 0);
        assert_eq!(zeroed.total, 0);
        // Removing the type drops its counter record.
        registry.remove("file").expect("delete applies");
        assert!(registry.counter_snapshot("file").is_none());
    }
}
