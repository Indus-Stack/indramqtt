use super::*;

pub(crate) async fn ingress(
    engine: &RuleEngine,
    sink: &Arc<dyn BrokerSink>,
    payload: &'static [u8],
) -> usize {
    engine
        .dispatch_ingress(
            &Topic::new("sensors/kitchen").unwrap(),
            &Bytes::from_static(payload),
            QoS::AtMostOnce,
            sink,
        )
        .await
}

pub(crate) use std::sync::Mutex as StdMutex;

#[derive(Debug, Default)]
pub(crate) struct RecordingSink {
    pub(crate) published: StdMutex<Vec<(Topic, Bytes, QoS, bool)>>,
}

#[async_trait]
impl BrokerSink for RecordingSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<(), RuleEngineError> {
        self.published
            .lock()
            .unwrap()
            .push((topic, payload, qos, retain));
        Ok(())
    }
}
