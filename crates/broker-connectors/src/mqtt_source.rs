//! MQTT ingress source consumer.
//!
//! One task holds one remote MQTT connection. It subscribes to the
//! configured remote topic and forwards every received PUBLISH to the
//! broker ingress. The source runtime stops a task on update and delete,
//! so an old consumer never keeps its subscription beside the new one.
//!
//! Bounds: the driver request channel holds at most
//! [`MQTT_SOURCE_CHANNEL_CAP`] outgoing requests per source. The driver
//! reads at most ten packets per poll. This task delivers each packet
//! before the next poll, so it keeps no ingress queue. A stop request
//! drains the packets that the driver already read before the task ends.
//! A failed connection retries after [`MQTT_SOURCE_RETRY`], which keeps
//! a downed remote broker from hot-spinning the task.

use crate::{ConnectorError, Result};
use async_trait::async_trait;
use bytes::Bytes;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, NetworkOptions, Packet, QoS};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

/// Outgoing request-channel capacity for one source. Reason: the driver
/// sends SUBSCRIBE, acknowledgements and keepalive requests through this
/// channel; 64 bounds that queue and never blocks the delivery path.
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
    pub qos: u8,
    /// Delay before a failed connection retries.
    pub retry_delay: Duration,
    /// Longest time for one connection attempt. `None` uses the driver
    /// default.
    pub connection_timeout: Option<Duration>,
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
    let mut options = MqttOptions::new(config.client_id.clone(), config.host.clone(), config.port);
    options.set_keep_alive(Duration::from_secs(u64::from(config.keep_alive_secs)));
    options.set_clean_session(config.clean_session);
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
    client
        .subscribe(config.topic.clone(), source_qos(config.qos))
        .await
        .map_err(|error| {
            ConnectorError::Connection(format!("mqtt source subscribe request failed: {error}"))
        })?;
    loop {
        let polled = tokio::select! {
            _ = wait_for_stop(&mut stop) => {
                drain(&mut eventloop, handler).await;
                return Ok(());
            }
            event = eventloop.poll() => event,
        };
        match polled {
            Ok(event) => handle_event(event, handler).await,
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
async fn drain(eventloop: &mut EventLoop, handler: &dyn MqttSourceHandler) {
    let deadline = tokio::time::Instant::now() + MQTT_SOURCE_DRAIN;
    loop {
        while let Some(event) = eventloop.state.events.pop_front() {
            handle_event(event, handler).await;
        }
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let wait = MQTT_SOURCE_DRAIN_READ.min(deadline - now);
        match tokio::time::timeout(wait, eventloop.poll()).await {
            Ok(Ok(event)) => handle_event(event, handler).await,
            Ok(Err(_)) => break,
            Err(_) => {}
        }
    }
    while let Some(event) = eventloop.state.events.pop_front() {
        handle_event(event, handler).await;
    }
}

async fn handle_event(event: Event, handler: &dyn MqttSourceHandler) {
    match event {
        Event::Incoming(Packet::SubAck(_)) => {
            handler.on_event(MqttSourceEvent::Connected).await;
        }
        Event::Incoming(Packet::Publish(publish)) => {
            let qos = match publish.qos {
                QoS::AtMostOnce => 0,
                QoS::AtLeastOnce => 1,
                QoS::ExactlyOnce => 2,
            };
            handler
                .on_event(MqttSourceEvent::Message(MqttSourceMessage {
                    topic: publish.topic,
                    payload: publish.payload,
                    qos,
                    retain: publish.retain,
                }))
                .await;
        }
        _ => {}
    }
}

fn source_qos(qos: u8) -> QoS {
    match qos {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}
