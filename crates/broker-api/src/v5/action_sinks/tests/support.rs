use super::*;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

pub(crate) fn record(id: &str, action_type: &str, connector: &str) -> ActionRecord {
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

pub(crate) fn mv02b_record(
    id: &str,
    action_type: &str,
    connector: &str,
    parameters: serde_json::Value,
) -> ActionRecord {
    ActionRecord {
        parameters,
        ..record(id, action_type, connector)
    }
}

#[derive(Default)]
pub(crate) struct BatchSink {
    pub(crate) batches: Mutex<Vec<usize>>,
    pub(crate) delay: Duration,
    pub(crate) active: AtomicUsize,
    pub(crate) max_active: AtomicUsize,
    pub(crate) fail: bool,
}

#[async_trait::async_trait]
impl broker_connectors::Sink for BatchSink {
    async fn send(
        &self,
        topic: &Topic,
        payload: &Bytes,
        qos: QoS,
    ) -> broker_connectors::Result<()> {
        self.send_batch(&[SinkBatchItem {
            topic: topic.clone(),
            payload: payload.clone(),
            qos,
        }])
        .await
    }

    async fn send_batch(&self, items: &[SinkBatchItem]) -> broker_connectors::Result<()> {
        let active = self.active.fetch_add(1, AtomicOrdering::SeqCst) + 1;
        self.max_active.fetch_max(active, AtomicOrdering::SeqCst);
        self.batches
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(items.len());
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.active.fetch_sub(1, AtomicOrdering::SeqCst);
        if self.fail {
            return Err(broker_connectors::ConnectorError::Dispatch(
                "planned failure".to_string(),
            ));
        }
        Ok(())
    }

    fn kind(&self) -> &'static str {
        "test-batch"
    }
}

pub(crate) fn runtime(
    inner: Arc<BatchSink>,
    opts: ActionResourceOpts,
) -> (ActionRuntimeSink, Arc<ActionMetrics>) {
    let metrics = Arc::new(ActionMetrics::new());
    (
        ActionRuntimeSink::new(inner, opts, Arc::clone(&metrics), None),
        metrics,
    )
}

pub(crate) fn mv02d_record(
    id: &str,
    action_type: &str,
    connector: &str,
    parameters: serde_json::Value,
) -> ActionRecord {
    ActionRecord {
        parameters,
        ..record(id, action_type, connector)
    }
}
