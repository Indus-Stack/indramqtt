//! MQTT ingress source consumer.
//!
//! One task holds one remote MQTT connection. It subscribes to the
//! configured remote topic and forwards every received PUBLISH matching
//! the active filter to the broker ingress. The source runtime stops a
//! task on update and delete, and retires any previous subscription when a
//! persistent session changes its topic filter.
//!
//! Bounds: the driver request channel holds at most
//! [`MQTT_SOURCE_CHANNEL_CAP`] outgoing requests per source. The driver
//! reads at most ten packets per poll. This task enables manual
//! acknowledgements and sends `PUBACK` only after local ingress finishes
//! handling each packet. A stop request drains the packets that the
//! driver already read before the task ends. A failed connection retries
//! after [`MQTT_SOURCE_RETRY`], which keeps a downed remote broker from
//! hot-spinning the task.

use crate::{ConnectorError, Result};
use async_trait::async_trait;
use broker_protocol::{Topic, TopicFilter};
use bytes::Bytes;
use rumqttc::{
    AsyncClient, Event, EventLoop, MqttOptions, NetworkOptions, Packet, QoS, SubscribeReasonCode,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Outgoing request-channel capacity for one source. Reason: the driver
/// sends SUBSCRIBE, UNSUBSCRIBE, acknowledgements and keepalive requests
/// through this channel; 64 bounds that queue and never blocks delivery.
pub const MQTT_SOURCE_CHANNEL_CAP: usize = 64;

/// Longest time to drain received packets after a stop request. Reason:
/// an update waits for this drain. 250 ms is short beside the 200 ms
/// reconcile interval and covers the driver's pending batch.
pub const MQTT_SOURCE_DRAIN: Duration = Duration::from_millis(250);

/// Bound for one drain read with no packet waiting. Reason: keeps the
/// whole stop wait inside [`MQTT_SOURCE_DRAIN`].
const MQTT_SOURCE_DRAIN_READ: Duration = Duration::from_millis(20);

/// Delay before a failed connection retries. Reason: a downed remote
/// broker must not spin the task, and one second is short beside the
/// source health interval.
pub const MQTT_SOURCE_RETRY: Duration = Duration::from_secs(1);

/// One remote MQTT connection for a source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSourceConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive_secs: u16,
    pub clean_session: bool,
    pub topic: String,
    /// Previous subscription filter to unsubscribe when a persistent
    /// session (`clean_session = false`) replaces its topic filter.
    pub previous_topic: Option<String>,
    pub qos: u8,
    /// Delay before a failed connection retries.
    pub retry_delay: Duration,
    /// Longest time for one connection attempt. `None` uses the driver
    /// default.
    pub connection_timeout: Option<Duration>,
}

impl MqttSourceConfig {
    /// Returns true if the persistent configuration fields match,
    /// ignoring one-shot `previous_topic` migration state.
    pub fn same_config(&self, other: &Self) -> bool {
        self.host == other.host
            && self.port == other.port
            && self.client_id == other.client_id
            && self.username == other.username
            && self.password == other.password
            && self.keep_alive_secs == other.keep_alive_secs
            && self.clean_session == other.clean_session
            && self.topic == other.topic
            && self.qos == other.qos
            && self.retry_delay == other.retry_delay
            && self.connection_timeout == other.connection_timeout
    }
}

/// One message received from the remote broker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MqttSourceMessage {
    pub topic: String,
    pub payload: Bytes,
    pub qos: u8,
    pub retain: bool,
}

/// State change reported to the source runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttSourceEvent {
    /// The remote broker accepted the subscription.
    Connected,
    /// One remote publish.
    Message(MqttSourceMessage),
    /// The connection failed. The message is the reason.
    Disconnected(String),
}

/// Receiver of the source event stream. The runtime writes the status
/// and publishes the message into the broker.
#[async_trait]
pub trait MqttSourceHandler: Send + Sync {
    async fn on_event(&self, event: MqttSourceEvent);
}

/// Run one source until the task is stopped. A failed connection reports
/// the reason and retries after a bounded pause. A stop request drains
/// the packets already read, then ends the task.
pub async fn run_mqtt_source(
    config: MqttSourceConfig,
    handler: Arc<dyn MqttSourceHandler>,
    mut stop: watch::Receiver<bool>,
) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        let result = run_once(&config, handler.as_ref(), stop.clone()).await;
        match result {
            Ok(()) => return,
            Err(error) => {
                handler
                    .on_event(MqttSourceEvent::Disconnected(error.to_string()))
                    .await;
            }
        }
        tokio::select! {
            _ = wait_for_stop(&mut stop) => return,
            _ = tokio::time::sleep(config.retry_delay) => {}
        }
    }
}

/// Wait until the runtime asks the consumer to stop.
async fn wait_for_stop(stop: &mut watch::Receiver<bool>) {
    loop {
        if *stop.borrow_and_update() {
            return;
        }
        if stop.changed().await.is_err() {
            return;
        }
    }
}

async fn run_once(
    config: &MqttSourceConfig,
    handler: &dyn MqttSourceHandler,
    mut stop: watch::Receiver<bool>,
) -> Result<()> {
    let filter = TopicFilter::new(&config.topic).map_err(|error| {
        ConnectorError::Dispatch(format!(
            "invalid source topic filter {:?}: {error}",
            config.topic
        ))
    })?;
    let mut options = MqttOptions::new(config.client_id.clone(), config.host.clone(), config.port);
    options.set_keep_alive(Duration::from_secs(u64::from(config.keep_alive_secs)));
    options.set_clean_session(config.clean_session);
    options.set_manual_acks(true);
    if let Some(username) = &config.username {
        options.set_credentials(
            username.clone(),
            config.password.clone().unwrap_or_default(),
        );
    }
    let (client, mut eventloop) = AsyncClient::new(options, MQTT_SOURCE_CHANNEL_CAP);
    if let Some(timeout) = config.connection_timeout {
        // The driver takes whole seconds; round a shorter value up to one.
        let mut network_options = NetworkOptions::new();
        network_options.set_connection_timeout(timeout.as_secs().max(1));
        eventloop.set_network_options(network_options);
    }
    if let Some(old_topic) = &config.previous_topic {
        if old_topic != &config.topic {
            client
                .unsubscribe(old_topic.clone())
                .await
                .map_err(|error| {
                    ConnectorError::Connection(format!(
                        "mqtt source unsubscribe request failed: {error}"
                    ))
                })?;
        }
    }
    client
        .subscribe(config.topic.clone(), source_qos(config.qos))
        .await
        .map_err(|error| {
            ConnectorError::Connection(format!("mqtt source subscribe request failed: {error}"))
        })?;
    loop {
        let polled = tokio::select! {
            _ = wait_for_stop(&mut stop) => {
                drain(&mut eventloop, &client, &filter, &config.topic, handler).await;
                return Ok(());
            }
            event = eventloop.poll() => event,
        };
        match polled {
            Ok(event) => handle_event(event, &client, &filter, &config.topic, handler).await?,
            Err(error) => {
                return Err(ConnectorError::Connection(format!(
                    "mqtt source connection failed: {error}"
                )));
            }
        }
    }
}

/// Deliver every packet that the driver already read. The bound keeps
/// one update from waiting on a busy remote broker.
async fn drain(
    eventloop: &mut EventLoop,
    client: &AsyncClient,
    filter: &TopicFilter,
    topic_pattern: &str,
    handler: &dyn MqttSourceHandler,
) {
    let deadline = tokio::time::Instant::now() + MQTT_SOURCE_DRAIN;
    loop {
        while let Some(event) = eventloop.state.events.pop_front() {
            if handle_event(event, client, filter, topic_pattern, handler)
                .await
                .is_err()
            {
                return;
            }
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let wait = MQTT_SOURCE_DRAIN_READ.min(deadline - now);
        match tokio::time::timeout(wait, eventloop.poll()).await {
            Ok(Ok(event)) => {
                if handle_event(event, client, filter, topic_pattern, handler)
                    .await
                    .is_err()
                {
                    return;
                }
            }
            Ok(Err(_)) => break,
            Err(_) => {}
        }
    }
    while let Some(event) = eventloop.state.events.pop_front() {
        if handle_event(event, client, filter, topic_pattern, handler)
            .await
            .is_err()
        {
            return;
        }
    }
}

async fn handle_event(
    event: Event,
    client: &AsyncClient,
    filter: &TopicFilter,
    topic_pattern: &str,
    handler: &dyn MqttSourceHandler,
) -> Result<()> {
    match event {
        Event::Incoming(Packet::SubAck(suback)) => {
            if suback.return_codes.is_empty()
                || suback
                    .return_codes
                    .iter()
                    .any(|code| !matches!(code, SubscribeReasonCode::Success(_)))
            {
                return Err(ConnectorError::Connection(format!(
                    "mqtt source subscription to {topic_pattern:?} rejected by broker"
                )));
            }
            handler.on_event(MqttSourceEvent::Connected).await;
        }
        Event::Incoming(Packet::Publish(publish)) => {
            let matched = Topic::new(publish.topic.as_str())
                .map(|parsed| filter.matches(&parsed))
                .unwrap_or(false);
            if matched {
                let qos = match publish.qos {
                    QoS::AtMostOnce => 0,
                    QoS::AtLeastOnce => 1,
                    QoS::ExactlyOnce => 2,
                };
                handler
                    .on_event(MqttSourceEvent::Message(MqttSourceMessage {
                        topic: publish.topic.clone(),
                        payload: publish.payload.clone(),
                        qos,
                        retain: publish.retain,
                    }))
                    .await;
            }
            if publish.qos != QoS::AtMostOnce {
                client.ack(&publish).await.map_err(|error| {
                    ConnectorError::Connection(format!("mqtt source ack failed: {error}"))
                })?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn source_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingHandler {
        events: Mutex<Vec<MqttSourceEvent>>,
    }

    #[async_trait]
    impl MqttSourceHandler for RecordingHandler {
        async fn on_event(&self, event: MqttSourceEvent) {
            self.events.lock().await.push(event);
        }
    }

    fn base_config(port: u16, topic: &str) -> MqttSourceConfig {
        MqttSourceConfig {
            host: "127.0.0.1".to_string(),
            port,
            client_id: "test-source".to_string(),
            username: None,
            password: None,
            keep_alive_secs: 60,
            clean_session: true,
            topic: topic.to_string(),
            previous_topic: None,
            qos: 1,
            retry_delay: Duration::from_millis(50),
            connection_timeout: Some(Duration::from_secs(2)),
        }
    }

    async fn read_mqtt_packet(stream: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
        let mut header = [0u8; 1];
        stream.read_exact(&mut header).await.unwrap();
        let mut len = 0usize;
        let mut shift = 0usize;
        loop {
            let mut b = [0u8; 1];
            stream.read_exact(&mut b).await.unwrap();
            len |= usize::from(b[0] & 0x7F) << shift;
            if b[0] & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        let mut body = vec![0u8; len];
        if len > 0 {
            stream.read_exact(&mut body).await.unwrap();
        }
        (header[0] >> 4, body)
    }

    fn encode_publish_qos1(topic: &str, pkid: u16, payload: &[u8]) -> Vec<u8> {
        let t = topic.as_bytes();
        let rem = 2 + t.len() + 2 + payload.len();
        let mut out = Vec::with_capacity(2 + rem);
        out.push(0x32); // PUBLISH QoS 1
        assert!(rem < 128);
        out.push(rem as u8);
        out.extend_from_slice(&(t.len() as u16).to_be_bytes());
        out.extend_from_slice(t);
        out.extend_from_slice(&pkid.to_be_bytes());
        out.extend_from_slice(payload);
        out
    }

    #[tokio::test]
    async fn suback_failure_reports_disconnected_not_connected() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let (pkt_type, _) = read_mqtt_packet(&mut sock).await;
            assert_eq!(pkt_type, 1); // CONNECT
            sock.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap(); // CONNACK
            let (pkt_type, body) = read_mqtt_packet(&mut sock).await;
            assert_eq!(pkt_type, 8); // SUBSCRIBE
                                     // SUBACK with 0x80 Failure return code
            sock.write_all(&[0x90, 0x03, body[0], body[1], 0x80])
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let handler = Arc::new(RecordingHandler::default());
        let (stop_tx, stop_rx) = watch::channel(false);
        let cfg = base_config(port, "denied/topic");
        let h = Arc::clone(&handler);
        let task = tokio::spawn(async move {
            run_mqtt_source(cfg, h, stop_rx).await;
        });
        tokio::time::sleep(Duration::from_millis(120)).await;
        let _ = stop_tx.send(true);
        let _ = task.await;

        let events = handler.events.lock().await;
        assert!(
            !events
                .iter()
                .any(|e| matches!(e, MqttSourceEvent::Connected)),
            "rejected SUBACK must not emit Connected: {events:?}"
        );
        assert!(
            events.iter().any(
                |e| matches!(e, MqttSourceEvent::Disconnected(msg) if msg.contains("rejected"))
            ),
            "rejected SUBACK must emit Disconnected: {events:?}"
        );
    }

    #[tokio::test]
    async fn persistent_topic_change_unsubscribes_and_filters_stale_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (unsub_tx, unsub_rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let (puback_tx, puback_rx) = tokio::sync::oneshot::channel::<u16>();

        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let (pkt_type, _) = read_mqtt_packet(&mut sock).await;
            assert_eq!(pkt_type, 1);
            sock.write_all(&[0x20, 0x02, 0x00, 0x00]).await.unwrap();

            // Expect UNSUBSCRIBE + SUBSCRIBE in any order
            let mut got_unsub = None;
            let mut got_sub = false;
            while got_unsub.is_none() || !got_sub {
                let (pkt_type, body) = read_mqtt_packet(&mut sock).await;
                if pkt_type == 10 {
                    // UNSUBSCRIBE
                    sock.write_all(&[0xB0, 0x02, body[0], body[1]])
                        .await
                        .unwrap();
                    got_unsub = Some(body);
                } else if pkt_type == 8 {
                    // SUBSCRIBE
                    sock.write_all(&[0x90, 0x03, body[0], body[1], 0x01])
                        .await
                        .unwrap();
                    got_sub = true;
                }
            }
            let _ = unsub_tx.send(got_unsub.unwrap());

            // Send one stale publish on old topic and one valid publish on new topic
            sock.write_all(&encode_publish_qos1("old/topic", 1, b"stale"))
                .await
                .unwrap();
            let (ack1_type, ack1_body) = read_mqtt_packet(&mut sock).await;
            assert_eq!(ack1_type, 4); // PUBACK for packet 1
            assert_eq!(u16::from_be_bytes([ack1_body[0], ack1_body[1]]), 1);

            sock.write_all(&encode_publish_qos1("new/topic", 2, b"fresh"))
                .await
                .unwrap();
            let (ack2_type, ack2_body) = read_mqtt_packet(&mut sock).await;
            assert_eq!(ack2_type, 4);
            let _ = puback_tx.send(u16::from_be_bytes([ack2_body[0], ack2_body[1]]));
            tokio::time::sleep(Duration::from_millis(100)).await;
        });

        let handler = Arc::new(RecordingHandler::default());
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut cfg = base_config(port, "new/topic");
        cfg.clean_session = false;
        cfg.previous_topic = Some("old/topic".to_string());
        let h = Arc::clone(&handler);
        let task = tokio::spawn(async move {
            run_mqtt_source(cfg, h, stop_rx).await;
        });

        let unsub_body = tokio::time::timeout(Duration::from_secs(3), unsub_rx)
            .await
            .unwrap()
            .unwrap();
        let unsub_str = String::from_utf8_lossy(&unsub_body[4..]);
        assert_eq!(unsub_str, "old/topic");
        let acked_pkid = tokio::time::timeout(Duration::from_secs(3), puback_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(acked_pkid, 2);

        let _ = stop_tx.send(true);
        let _ = task.await;

        let events = handler.events.lock().await;
        let delivered_topics: Vec<&str> = events
            .iter()
            .filter_map(|e| match e {
                MqttSourceEvent::Message(m) => Some(m.topic.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(delivered_topics, vec!["new/topic"]);
    }
}
