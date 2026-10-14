//! Temporary message collects behind the `subscribe_collect` tool.
//!
//! The tool registers one slot here, subscribes the same filter on the
//! live router under a synthetic client id, drains matching publishes
//! until its limit or its time bound, then removes both rows. The
//! publish paths feed this registry: the management publish path
//! (`crate::v5::clients::do_mgmt_publish`) and the kernel ingress
//! pipeline (`broker-node`, ingress hook). Idle publishes pay one
//! relaxed atomic load and nothing else.

use broker_protocol::{QoS, Topic, TopicFilter};
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

/// Largest payload kept for one collected message, in bytes.
/// Larger payloads are cut and flagged.
pub const MAX_COLLECT_BYTES: usize = 64 * 1024;

// Reason: the largest collect asks for 100 messages, and the queue
// holds one burst past that while the handler drains.
const COLLECT_QUEUE: usize = 128;

// Reason: operator debugging is interactive. Eight concurrent collects
// bound the registry near 64 MiB in the worst case (8 slots times 128
// messages times 64 KiB). The ninth collect fails closed.
const MAX_COLLECTORS: usize = 8;

/// Prefix for synthetic client ids of temporary subscriptions.
/// The suffix is a process-unique counter, so two collects never share
/// an id.
const COLLECT_CLIENT_PREFIX: &str = "mcp-collect-";

/// One message handed to a running collect.
#[derive(Debug, Clone)]
pub(crate) struct CollectedMessage {
    pub(crate) topic: String,
    pub(crate) qos: u8,
    pub(crate) retain: bool,
    pub(crate) payload: Bytes,
    pub(crate) truncated: bool,
}

/// One running collect: its filter, its tenant and its queue.
struct CollectSlot {
    filter: TopicFilter,
    max_qos: u8,
    tenant: String,
    tx: tokio::sync::mpsc::Sender<CollectedMessage>,
}

/// Registry of running collects, shared by the tool and the publish
/// paths. Idle publishes pay one atomic load and nothing else.
pub struct CollectRegistry {
    active: AtomicUsize,
    next_id: AtomicU64,
    inner: std::sync::Mutex<HashMap<u64, CollectSlot>>,
}

/// Collect fails when the registry is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CollectError {
    /// Too many collects run.
    Full,
}

impl std::fmt::Display for CollectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Full => write!(f, "too many concurrent collects"),
        }
    }
}

impl std::error::Error for CollectError {}

impl CollectRegistry {
    /// Empty registry. No collect runs.
    pub fn new() -> Self {
        Self {
            active: AtomicUsize::new(0),
            next_id: AtomicU64::new(1),
            inner: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Number of running collects. Tests use this; the publish path
    /// uses only the atomic fast path.
    #[cfg(test)]
    pub fn count(&self) -> usize {
        self.inner.lock().expect("registry lock").len()
    }

    /// Register one collect. Fails closed past [`MAX_COLLECTORS`].
    pub fn register(
        &self,
        filter: TopicFilter,
        max_qos: u8,
        tenant: &str,
    ) -> Result<CollectHandle, CollectError> {
        let (tx, rx) = tokio::sync::mpsc::channel(COLLECT_QUEUE);
        let mut slots = self.inner.lock().expect("registry lock");
        if slots.len() >= MAX_COLLECTORS {
            return Err(CollectError::Full);
        }
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        slots.insert(
            id,
            CollectSlot {
                filter: filter.clone(),
                max_qos,
                tenant: tenant.to_string(),
                tx,
            },
        );
        self.active.fetch_add(1, Ordering::Relaxed);
        Ok(CollectHandle {
            id,
            client_id: format!("{COLLECT_CLIENT_PREFIX}{id}"),
            filter,
            tenant: tenant.to_string(),
            rx,
        })
    }

    /// Remove one collect. Runs on every exit path through the guard.
    pub fn unregister(&self, id: u64) {
        let removed = self.inner.lock().expect("registry lock").remove(&id);
        if removed.is_some() {
            self.active.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// Hand one published message to each matching collect.
    ///
    /// Fast path (no collect runs, the default): one relaxed atomic
    /// load and a return. No lock, no allocation, no clone. Slow path
    /// (a collect runs): one short lock, one filter test per slot, and
    /// one clone per matching slot. The queue send never blocks; a full
    /// queue drops the message.
    pub fn feed(&self, tenant: &str, topic: &Topic, qos: QoS, retain: bool, payload: &Bytes) {
        if self.active.load(Ordering::Relaxed) == 0 {
            return;
        }
        let qos_raw = u8::from(qos);
        let slots = self.inner.lock().expect("registry lock");
        for slot in slots.values() {
            if slot.tenant != tenant {
                continue;
            }
            if !slot.filter.matches(topic) {
                continue;
            }
            let (bytes, truncated) = truncate_payload(payload);
            let _ = slot.tx.try_send(CollectedMessage {
                topic: topic.as_str().to_string(),
                qos: std::cmp::min(qos_raw, slot.max_qos),
                retain,
                payload: bytes,
                truncated,
            });
        }
    }
}

/// Cut one payload to [`MAX_COLLECT_BYTES`]. Reports the cut.
pub(crate) fn truncate_payload(payload: &Bytes) -> (Bytes, bool) {
    if payload.len() > MAX_COLLECT_BYTES {
        (payload.slice(..MAX_COLLECT_BYTES), true)
    } else {
        (payload.clone(), false)
    }
}

impl Default for CollectRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// One registered collect: its id, its synthetic client id, its filter
/// and its queue. The guard below owns this and unregisters it.
pub struct CollectHandle {
    pub(crate) id: u64,
    pub(crate) client_id: String,
    pub(crate) filter: TopicFilter,
    pub(crate) tenant: String,
    pub(crate) rx: tokio::sync::mpsc::Receiver<CollectedMessage>,
}

/// Removes the temporary subscription on drop: the registry slot and
/// the router row. Covers every exit path, including a dropped client
/// connection mid-collect.
pub(crate) struct CollectGuard {
    pub(crate) registry: Arc<CollectRegistry>,
    pub(crate) router: Arc<broker_router::Router>,
    pub(crate) id: u64,
    pub(crate) client_id: String,
    pub(crate) filter: TopicFilter,
    pub(crate) tenant: String,
}

impl Drop for CollectGuard {
    fn drop(&mut self) {
        self.registry.unregister(self.id);
        self.router
            .unsubscribe_in_tenant(&self.filter, &self.client_id, &self.tenant);
    }
}

/// Render one collected message. Binary payloads ride base64.
pub(crate) fn render_collected(next: &CollectedMessage) -> serde_json::Value {
    let (encoding, payload) = match std::str::from_utf8(&next.payload) {
        Ok(text) => ("plain", text.to_string()),
        Err(_) => ("base64", {
            use base64::engine::general_purpose::STANDARD as Base64;
            use base64::Engine as _;
            Base64.encode(next.payload.as_ref())
        }),
    };
    serde_json::json!({
        "topic": next.topic,
        "qos": next.qos,
        "retain": next.retain,
        "payload": payload,
        "payload_encoding": encoding,
        "truncated": next.truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_starts_empty_and_feeds_nothing() {
        let registry = CollectRegistry::new();
        assert_eq!(registry.active.load(Ordering::Relaxed), 0);
        let topic = Topic::new("quiet/topic").expect("test topic is valid");
        registry.feed(
            broker_session::tenant::DEFAULT_TENANT_ID,
            &topic,
            QoS::AtMostOnce,
            false,
            &Bytes::from_static(b"hello"),
        );
        assert_eq!(registry.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn feed_matches_filter_and_tenant_only() {
        let registry = CollectRegistry::new();
        let filter = TopicFilter::new("sensors/+").expect("test filter is valid");
        let mut handle = registry
            .register(filter, 1, broker_session::tenant::DEFAULT_TENANT_ID)
            .expect("first collect registers");
        let topic = Topic::new("sensors/temp").expect("test topic is valid");
        registry.feed(
            broker_session::tenant::DEFAULT_TENANT_ID,
            &topic,
            QoS::AtLeastOnce,
            true,
            &Bytes::from_static(b"21"),
        );
        let next = handle.rx.try_recv().expect("matching publish arrives");
        assert_eq!(next.topic, "sensors/temp");
        assert_eq!(next.qos, 1);
        assert!(next.retain);
        assert_eq!(next.payload, Bytes::from_static(b"21"));
        assert!(!next.truncated);
        // Another tenant never feeds this collect.
        registry.feed(
            "tenant-b",
            &topic,
            QoS::AtLeastOnce,
            false,
            &Bytes::from_static(b"no"),
        );
        assert!(handle.rx.try_recv().is_err());
        // Another topic never feeds this collect.
        let other = Topic::new("other/topic").expect("test topic is valid");
        registry.feed(
            broker_session::tenant::DEFAULT_TENANT_ID,
            &other,
            QoS::AtMostOnce,
            false,
            &Bytes::from_static(b"no"),
        );
        assert!(handle.rx.try_recv().is_err());
        // QoS downgrades to the subscription maximum.
        let hot = Topic::new("sensors/hot").expect("test topic is valid");
        registry.feed(
            broker_session::tenant::DEFAULT_TENANT_ID,
            &hot,
            QoS::AtLeastOnce,
            false,
            &Bytes::from_static(b"x"),
        );
        assert_eq!(handle.rx.try_recv().expect("hot arrives").qos, 1);
        registry.unregister(handle.id);
        assert_eq!(registry.count(), 0);
        assert_eq!(registry.active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn feed_cuts_payloads_past_64kib() {
        let registry = CollectRegistry::new();
        let filter = TopicFilter::new("big/#").expect("test filter is valid");
        let mut handle = registry
            .register(filter, 0, broker_session::tenant::DEFAULT_TENANT_ID)
            .expect("collect registers");
        let big = vec![b'A'; MAX_COLLECT_BYTES + 16];
        let topic = Topic::new("big/blob").expect("test topic is valid");
        registry.feed(
            broker_session::tenant::DEFAULT_TENANT_ID,
            &topic,
            QoS::AtMostOnce,
            false,
            &Bytes::from(big),
        );
        let next = handle.rx.try_recv().expect("big payload arrives");
        assert_eq!(next.payload.len(), MAX_COLLECT_BYTES);
        assert!(next.truncated);
        registry.unregister(handle.id);
    }

    #[test]
    fn registry_refuses_past_eight_collects() {
        let registry = CollectRegistry::new();
        let mut ids = Vec::new();
        for _ in 0..MAX_COLLECTORS {
            let filter = TopicFilter::new("busy/#").expect("test filter is valid");
            let handle = registry
                .register(filter, 0, broker_session::tenant::DEFAULT_TENANT_ID)
                .expect("collect registers");
            ids.push(handle.id);
        }
        let filter = TopicFilter::new("busy/#").expect("test filter is valid");
        assert!(registry
            .register(filter, 0, broker_session::tenant::DEFAULT_TENANT_ID)
            .is_err());
        for id in ids {
            registry.unregister(id);
        }
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn collect_renders_binary_as_base64() {
        let payload = Bytes::from_static(&[0xff, 0xfe, 0x00]);
        let rendered = render_collected(&CollectedMessage {
            topic: "mcp/bin".to_string(),
            qos: 0,
            retain: false,
            payload: payload.clone(),
            truncated: false,
        });
        assert_eq!(rendered["payload_encoding"], serde_json::json!("base64"));
        let text = rendered["payload"].as_str().expect("payload text");
        use base64::engine::general_purpose::STANDARD as Base64;
        use base64::Engine as _;
        assert_eq!(Base64.decode(text).expect("payload decodes"), payload);
    }
}
