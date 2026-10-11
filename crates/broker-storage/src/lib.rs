pub mod offline;
pub mod stream;
pub use offline::{OfflineConfig, OfflineQueueStore, OfflineRecord};
pub use stream::{DurableStreamStore, FsyncPolicy, StreamConfig, StreamRecord};

use async_trait::async_trait;
use broker_protocol::{QoS, Topic, TopicFilter};
use bytes::Bytes;
use parking_lot::RwLock;
use std::collections::HashMap;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Message not found at offset: {0}")]
    NotFound(u64),

    #[error("Storage engine failure: {0}")]
    Engine(String),
}

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredMessage {
    pub offset: u64,
    pub topic: Topic,
    pub qos: QoS,
    pub retain: bool,
    pub payload: Bytes,
    /// Publish time recorded where the message is already being written
    /// (retained store / append), as millis since the Unix epoch.
    /// `None` means the message predates timestamp recording; API reads
    /// must omit the field rather than substituting anything.
    pub publish_at_ms: Option<u64>,
}

/// Current wall-clock time as millis since the Unix epoch. Read once per
/// stored message on the write path (retained/appends are rare), never
/// on the delivery hot path.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[async_trait]
pub trait MessageStore: Send + Sync {
    async fn append(&self, topic: Topic, qos: QoS, retain: bool, payload: Bytes) -> Result<u64>;
    async fn get(&self, offset: u64) -> Result<StoredMessage>;
}

/// Result of one tenant retained write.
///
/// The store decides inside its single write lock. Callers count
/// a per-tenant drop without extra reads. Global drops carry no
/// tenant count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetainedWriteOutcome {
    /// The topic is stored.
    Stored,
    /// A new topic drops at the per-tenant cap.
    DroppedPerTenantLimit,
    /// A new topic drops at the global cap.
    DroppedGlobalLimit,
}

#[async_trait]
pub trait RetainedStore: Send + Sync {
    async fn set_retained(&self, topic: Topic, qos: QoS, payload: Bytes) -> Result<()>;
    async fn get_retained(&self, topic: &Topic) -> Result<Option<StoredMessage>>;
    async fn clear_retained(&self, topic: &Topic) -> Result<()>;
    /// All retained messages whose topic matches `filter`, ordered by
    /// topic for deterministic delivery to new subscribers.
    async fn find_matching(&self, filter: &TopicFilter) -> Result<Vec<StoredMessage>>;
    /// Tenant-scoped retained (MT-02): the same topic string in two
    /// tenants holds two independent retained messages; matching, reads
    /// and clears in one tenant never observe another tenant's messages.
    /// The default tenant follows the unscoped path with identical
    /// semantics. Subscribe-time and publish-time callers pass the
    /// subscriber's/publisher's tenant (MT-01 id).
    async fn set_retained_in_tenant(
        &self,
        topic: Topic,
        qos: QoS,
        payload: Bytes,
        tenant: &str,
    ) -> Result<()>;
    /// Store one tenant retained topic and report the outcome.
    ///
    /// The check and the write share one write lock. The publish
    /// path uses this call and takes no extra lock. The default
    /// impl stores through the plain call and reports stored.
    async fn set_retained_in_tenant_checked(
        &self,
        topic: Topic,
        qos: QoS,
        payload: Bytes,
        tenant: &str,
    ) -> Result<RetainedWriteOutcome> {
        self.set_retained_in_tenant(topic, qos, payload, tenant)
            .await
            .map(|()| RetainedWriteOutcome::Stored)
    }
    async fn get_retained_in_tenant(
        &self,
        topic: &Topic,
        tenant: &str,
    ) -> Result<Option<StoredMessage>>;
    async fn clear_retained_in_tenant(&self, topic: &Topic, tenant: &str) -> Result<()>;
    async fn find_matching_in_tenant(
        &self,
        filter: &TopicFilter,
        tenant: &str,
    ) -> Result<Vec<StoredMessage>>;
    /// Total retained entries across all tenants (for the global
    /// `stats.retained` counter). O(1) map length, no scan, no allocation.
    async fn count_retained(&self) -> Result<usize>;
    /// Retained entries in one tenant. O(1) counter read, never a scan.
    async fn count_retained_in_tenant(&self, tenant: &str) -> Result<usize> {
        let _ = tenant;
        Ok(0)
    }
    /// Set the default per-tenant retained cap (0 = unlimited).
    fn set_default_retained_per_tenant_cap(&self, _cap: usize) {}
    /// Set one tenant retained cap (0 = unlimited).
    fn set_retained_per_tenant_cap(&self, _tenant: &str, _cap: usize) {}
    /// Effective retained cap for one tenant (0 = unlimited).
    fn retained_cap_for_tenant(&self, _tenant: &str) -> usize {
        DEFAULT_MAX_RETAINED_PER_TENANT
    }
    /// Removes every retained entry of one tenant together with its cap
    /// override and its count, and returns the number of entries removed.
    /// Used when the operator deletes a tenant, so a later tenant with the
    /// same name starts empty.
    async fn clear_tenant(&self, _tenant: &str) -> Result<usize> {
        Ok(0)
    }
}

#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn update_cursor(&self, session_id: u64, offset: u64) -> Result<()>;
    async fn get_cursor(&self, session_id: u64) -> Result<u64>;
}

/// Tenant id for retained entries with no tenant attribute (MT-01
/// default). Single-tenant installs only ever carry this value, so their
/// store/clear/match path is identical to the pre-tenancy one.
pub const DEFAULT_TENANT_ID: &str = "default";

/// Longest tenant tag stored on a retained key. Mirrors the MT-01 render
/// bound so one key's tenant part can never outgrow the assignment it came
/// from: at most ~128 bytes of shared string per retained topic. Empty or
/// overlong ids fall back to the default tenant (fail closed, never another
/// tenant's).
pub const MAX_TENANT_ID_LEN: usize = 128;

/// Clamp a tenant id to what a retained key may store: empty or overlong
/// ids become the default tenant (fail closed, never another tenant's).
fn normalize_tenant_key(tenant: &str) -> std::sync::Arc<str> {
    if tenant.is_empty() || tenant.len() > MAX_TENANT_ID_LEN {
        std::sync::Arc::from(DEFAULT_TENANT_ID)
    } else {
        std::sync::Arc::from(tenant)
    }
}

/// Maximum retained topics held by [`MemoryStore`].
///
/// The map holds at most this many `(tenant, topic)` pairs across all
/// tenants; inserts past the cap keep delivery working but drop the new
/// topic (existing topics may still be replaced), so one client cannot
/// balloon the node. The cap stays global so per-tenant memory is bounded
/// by the same limit with no second unlimited default (per-tenant
/// accounting arrives in MT-04). Management-plane
/// reads clone at most one entry (single lookup) or a bounded page (list),
/// never scanning without a cap, and take only short read locks so they
/// never block delivery.
///
/// Each entry holds one shared topic string plus payload.
/// 100_000 entries bound total retained state.
/// Inserts past the cap drop the new topic.
pub const MAX_RETAINED_MESSAGES: usize = 100_000;

/// Default max retained topics per tenant (0 = unlimited).
///
/// The broker sets no per-tenant bound unless the operator sets one. The
/// global bound [`MAX_RETAINED_MESSAGES`] always applies.
pub const DEFAULT_MAX_RETAINED_PER_TENANT: usize = 0;

/// One retained table plus its per-tenant counts and caps.
///
/// Counts and caps live beside the map under the same lock, so a
/// retained write pays one lock and two integer compares, never a
/// scan and never a second lock. The deliver path takes no lock here.
struct RetainedInner {
    map: HashMap<(std::sync::Arc<str>, Topic), StoredMessage>,
    per_tenant: HashMap<std::sync::Arc<str>, usize>,
    caps: HashMap<std::sync::Arc<str>, usize>,
    default_cap: usize,
}

impl Default for RetainedInner {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
            per_tenant: HashMap::new(),
            caps: HashMap::new(),
            default_cap: DEFAULT_MAX_RETAINED_PER_TENANT,
        }
    }
}

/// In-memory storage engine for local fast tests and zero-disk development.
#[derive(Default)]
pub struct MemoryStore {
    messages: RwLock<Vec<StoredMessage>>,
    retained: RwLock<RetainedInner>,
    cursors: RwLock<HashMap<u64, u64>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Retained topics held by one tenant.
    ///
    /// Reads one integer under the same short read lock, never a scan.
    pub fn count_retained_in_tenant_sync(&self, tenant: &str) -> usize {
        let scope = normalize_tenant_key(tenant);
        self.retained
            .read()
            .per_tenant
            .get(scope.as_ref())
            .copied()
            .unwrap_or(0)
    }

    /// Set the default per-tenant retained cap (0 = unlimited).
    ///
    /// Applies to tenants with no override. Boot and API paths only.
    pub fn set_default_retained_per_tenant_cap(&self, cap: usize) {
        self.retained.write().default_cap = cap;
    }

    /// Set one tenant retained cap (0 = unlimited).
    ///
    /// Applies on the next retained write in that tenant. Boot and
    /// API paths only; the publish path never calls this.
    pub fn set_retained_per_tenant_cap(&self, tenant: &str, cap: usize) {
        let scope = normalize_tenant_key(tenant);
        self.retained.write().caps.insert(scope, cap);
    }

    /// Effective retained cap for one tenant (0 = unlimited).
    pub fn retained_cap_for_tenant_sync(&self, tenant: &str) -> usize {
        let scope = normalize_tenant_key(tenant);
        let inner = self.retained.read();
        inner
            .caps
            .get(scope.as_ref())
            .copied()
            .unwrap_or(inner.default_cap)
    }
}

#[async_trait]
impl MessageStore for MemoryStore {
    async fn append(&self, topic: Topic, qos: QoS, retain: bool, payload: Bytes) -> Result<u64> {
        let mut msgs = self.messages.write();
        let offset = msgs.len() as u64;
        msgs.push(StoredMessage {
            offset,
            topic,
            qos,
            retain,
            payload,
            publish_at_ms: Some(now_ms()),
        });
        Ok(offset)
    }

    async fn get(&self, offset: u64) -> Result<StoredMessage> {
        let msgs = self.messages.read();
        msgs.get(offset as usize)
            .cloned()
            .ok_or(StorageError::NotFound(offset))
    }
}

#[async_trait]
impl RetainedStore for MemoryStore {
    async fn set_retained(&self, topic: Topic, qos: QoS, payload: Bytes) -> Result<()> {
        self.set_retained_in_tenant(topic, qos, payload, DEFAULT_TENANT_ID)
            .await
    }

    async fn get_retained(&self, topic: &Topic) -> Result<Option<StoredMessage>> {
        self.get_retained_in_tenant(topic, DEFAULT_TENANT_ID).await
    }

    async fn clear_retained(&self, topic: &Topic) -> Result<()> {
        self.clear_retained_in_tenant(topic, DEFAULT_TENANT_ID)
            .await
    }

    async fn find_matching(&self, filter: &TopicFilter) -> Result<Vec<StoredMessage>> {
        self.find_matching_in_tenant(filter, DEFAULT_TENANT_ID)
            .await
    }

    async fn set_retained_in_tenant(
        &self,
        topic: Topic,
        qos: QoS,
        payload: Bytes,
        tenant: &str,
    ) -> Result<()> {
        self.set_retained_in_tenant_checked(topic, qos, payload, tenant)
            .await
            .map(|_| ())
    }

    async fn set_retained_in_tenant_checked(
        &self,
        topic: Topic,
        qos: QoS,
        payload: Bytes,
        tenant: &str,
    ) -> Result<RetainedWriteOutcome> {
        let key = (normalize_tenant_key(tenant), topic.clone());
        let mut inner = self.retained.write();
        if inner.map.len() >= MAX_RETAINED_MESSAGES && !inner.map.contains_key(&key) {
            return Ok(RetainedWriteOutcome::DroppedGlobalLimit);
        }
        let is_new = !inner.map.contains_key(&key);
        if is_new {
            let cap = inner
                .caps
                .get(key.0.as_ref())
                .copied()
                .unwrap_or(inner.default_cap);
            if cap > 0 {
                let used = inner.per_tenant.get(key.0.as_ref()).copied().unwrap_or(0);
                if used >= cap {
                    return Ok(RetainedWriteOutcome::DroppedPerTenantLimit);
                }
            }
        }
        inner.map.insert(
            key.clone(),
            StoredMessage {
                offset: 0,
                topic,
                qos,
                retain: true,
                payload,
                publish_at_ms: Some(now_ms()),
            },
        );
        if is_new {
            *inner.per_tenant.entry(key.0).or_insert(0) += 1;
        }
        Ok(RetainedWriteOutcome::Stored)
    }

    async fn get_retained_in_tenant(
        &self,
        topic: &Topic,
        tenant: &str,
    ) -> Result<Option<StoredMessage>> {
        let inner = self.retained.read();
        Ok(inner
            .map
            .get(&(normalize_tenant_key(tenant), topic.clone()))
            .cloned())
    }

    async fn clear_retained_in_tenant(&self, topic: &Topic, tenant: &str) -> Result<()> {
        let mut inner = self.retained.write();
        let key = (normalize_tenant_key(tenant), topic.clone());
        if inner.map.remove(&key).is_some() {
            if let Some(count) = inner.per_tenant.get_mut(key.0.as_ref()) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    inner.per_tenant.remove(key.0.as_ref());
                }
            }
        }
        Ok(())
    }

    async fn clear_tenant(&self, tenant: &str) -> Result<usize> {
        let scope = normalize_tenant_key(tenant);
        // The default tenant never leaves.
        if scope.as_ref() == DEFAULT_TENANT_ID {
            return Ok(0);
        }
        let mut inner = self.retained.write();
        let before = inner.map.len();
        inner.map.retain(|(key_tenant, _), _| *key_tenant != scope);
        let removed = before - inner.map.len();
        inner.per_tenant.remove(scope.as_ref());
        inner.caps.remove(scope.as_ref());
        Ok(removed)
    }

    async fn find_matching_in_tenant(
        &self,
        filter: &TopicFilter,
        tenant: &str,
    ) -> Result<Vec<StoredMessage>> {
        let scope = normalize_tenant_key(tenant);
        let inner = self.retained.read();
        let mut matched: Vec<StoredMessage> = inner
            .map
            .iter()
            .filter(|((entry_tenant, _), msg)| {
                entry_tenant.as_ref() == scope.as_ref() && filter.matches(&msg.topic)
            })
            .map(|(_, msg)| msg.clone())
            .collect();
        matched.sort_by(|a, b| a.topic.as_str().cmp(b.topic.as_str()));
        Ok(matched)
    }

    async fn count_retained(&self) -> Result<usize> {
        Ok(self.retained.read().map.len())
    }

    async fn count_retained_in_tenant(&self, tenant: &str) -> Result<usize> {
        Ok(self.count_retained_in_tenant_sync(tenant))
    }

    fn set_default_retained_per_tenant_cap(&self, cap: usize) {
        self.set_default_retained_per_tenant_cap(cap);
    }

    fn set_retained_per_tenant_cap(&self, tenant: &str, cap: usize) {
        self.set_retained_per_tenant_cap(tenant, cap);
    }

    fn retained_cap_for_tenant(&self, tenant: &str) -> usize {
        self.retained_cap_for_tenant_sync(tenant)
    }
}

#[async_trait]
impl SessionStore for MemoryStore {
    async fn update_cursor(&self, session_id: u64, offset: u64) -> Result<()> {
        let mut cursors = self.cursors.write();
        cursors.insert(session_id, offset);
        Ok(())
    }

    async fn get_cursor(&self, session_id: u64) -> Result<u64> {
        let cursors = self.cursors.read();
        Ok(cursors.get(&session_id).copied().unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_store() {
        let store = MemoryStore::new();
        let topic = Topic::new("telemetry/temp").unwrap();
        let offset = store
            .append(
                topic.clone(),
                QoS::AtLeastOnce,
                false,
                Bytes::from_static(b"25.4"),
            )
            .await
            .unwrap();

        assert_eq!(offset, 0);
        let msg = store.get(offset).await.unwrap();
        assert_eq!(msg.payload, Bytes::from_static(b"25.4"));

        store.update_cursor(101, offset).await.unwrap();
        assert_eq!(store.get_cursor(101).await.unwrap(), offset);
    }

    #[tokio::test]
    async fn clear_tenant_removes_entries_cap_and_count() {
        let store = MemoryStore::new();
        let topic = Topic::new("device/state").unwrap();
        store.set_retained_per_tenant_cap("t-gone", 5);
        for tenant in ["t-gone", "t-kept", DEFAULT_TENANT_ID] {
            store
                .set_retained_in_tenant(
                    topic.clone(),
                    QoS::AtMostOnce,
                    Bytes::from_static(b"v"),
                    tenant,
                )
                .await
                .unwrap();
        }
        assert_eq!(store.clear_tenant("t-gone").await.unwrap(), 1);
        assert!(store
            .get_retained_in_tenant(&topic, "t-gone")
            .await
            .unwrap()
            .is_none());
        assert_eq!(store.count_retained_in_tenant_sync("t-gone"), 0);
        assert_eq!(
            store.retained_cap_for_tenant_sync("t-gone"),
            DEFAULT_MAX_RETAINED_PER_TENANT,
            "the cap override is gone"
        );
        for tenant in ["t-kept", DEFAULT_TENANT_ID] {
            assert!(store
                .get_retained_in_tenant(&topic, tenant)
                .await
                .unwrap()
                .is_some());
        }
        // The default tenant is never cleared.
        assert_eq!(store.clear_tenant(DEFAULT_TENANT_ID).await.unwrap(), 0);
        assert_eq!(store.count_retained().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_retained_set_get_clear() {
        let store = MemoryStore::new();
        let topic = Topic::new("device/state").unwrap();

        assert!(store.get_retained(&topic).await.unwrap().is_none());

        store
            .set_retained(
                topic.clone(),
                QoS::AtMostOnce,
                Bytes::from_static(b"online"),
            )
            .await
            .unwrap();
        let msg = store.get_retained(&topic).await.unwrap().expect("stored");
        assert_eq!(msg.payload, Bytes::from_static(b"online"));
        assert!(msg.retain);

        // Replace keeps only the latest value.
        store
            .set_retained(topic.clone(), QoS::AtLeastOnce, Bytes::from_static(b"away"))
            .await
            .unwrap();
        let msg = store.get_retained(&topic).await.unwrap().expect("replaced");
        assert_eq!(msg.payload, Bytes::from_static(b"away"));
        assert_eq!(msg.qos, QoS::AtLeastOnce);

        store.clear_retained(&topic).await.unwrap();
        assert!(store.get_retained(&topic).await.unwrap().is_none());

        // Clearing an absent topic is a no-op.
        store.clear_retained(&topic).await.unwrap();
    }

    #[tokio::test]
    async fn test_find_matching_exact_and_wildcards() {
        let store = MemoryStore::new();
        for (topic, payload) in [
            ("device/state", "online"),
            ("device/battery", "87"),
            ("sensors/temperature", "21.5"),
        ] {
            store
                .set_retained(
                    Topic::new(topic).unwrap(),
                    QoS::AtMostOnce,
                    Bytes::from(payload),
                )
                .await
                .unwrap();
        }

        // Exact filter matches one.
        let matched = store
            .find_matching(&TopicFilter::new("device/state").unwrap())
            .await
            .unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].payload, Bytes::from_static(b"online"));

        // Single-level wildcard.
        let matched = store
            .find_matching(&TopicFilter::new("device/+").unwrap())
            .await
            .unwrap();
        assert_eq!(matched.len(), 2);
        // Deterministic topic order.
        assert_eq!(matched[0].topic.as_str(), "device/battery");
        assert_eq!(matched[1].topic.as_str(), "device/state");

        // Multi-level wildcard.
        let matched = store
            .find_matching(&TopicFilter::new("#").unwrap())
            .await
            .unwrap();
        assert_eq!(matched.len(), 3);

        // No match.
        let matched = store
            .find_matching(&TopicFilter::new("unknown/#").unwrap())
            .await
            .unwrap();
        assert!(matched.is_empty());

        // Cleared topics disappear from matching.
        store
            .clear_retained(&Topic::new("device/state").unwrap())
            .await
            .unwrap();
        let matched = store
            .find_matching(&TopicFilter::new("device/+").unwrap())
            .await
            .unwrap();
        assert_eq!(matched.len(), 1);
    }
}
