use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn rule_republish_reaches_alert_subscriber() {
    use broker_rules::RuleAction;

    let mut shared = Shared::new();
    shared.allow_anonymous = true;
    // Ingress rule: anything under sensors/+ is republished to
    // alerts/critical. No MQTT loopback involved: delivery flows
    // engine -> in-memory sink -> subscriber mailbox.
    shared
        .engine
        .create_rule(
            "republish-temp".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            None,
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("alerts/critical").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let shared = shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, shared).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });

    // Client A subscribes to alerts/critical as conn 301.
    let sub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect sub");
    let sub = FramedTransport::new(sub_io);
    sub.send(bind_frame(
        301,
        1,
        encode_bind_meta("alert-watcher", true, 60),
    ))
    .await
    .expect("bind");
    let _ = sub.recv().await.expect("recv binding");
    sub.send(subscribe_frame(
        301,
        2,
        encode_subscribe_meta(5, "alert-watcher", &[("alerts/critical", 1)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);

    // Client B publishes to sensors/temperature as conn 302.
    let pub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect pub");
    let publ = FramedTransport::new(pub_io);
    publ.send(bind_frame(
        302,
        1,
        encode_bind_meta("thermometer", true, 60),
    ))
    .await
    .expect("bind");
    let _ = publ.recv().await.expect("recv binding");
    let (meta, payload) = encode_publish_meta("sensors/temperature", 0, 0, false, b"21.5C");
    publ.send(publish_frame(302, 2, meta, payload))
        .await
        .expect("publish");

    // Client A receives the rule-republished message with the
    // identical payload on its own transport.
    let routed = sub.recv().await.expect("recv republished");
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    assert_eq!(routed.header.conn_id, 301);
    assert_eq!(routed.payload, Bytes::from_static(b"21.5C"));
    let (topic, _, qos, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(topic, "alerts/critical");
    assert_eq!(qos, 0);

    server.abort();
}

#[tokio::test]
async fn rule_sql_transforms_payload_end_to_end() {
    use broker_rules::RuleAction;

    let shared = test_shared();
    // Streaming SQL rule: project only `temperature` out of raw
    // payloads. The secret field must never reach subscribers.
    shared
        .engine
        .create_rule(
            "project-temp".to_string(),
            TopicFilter::new("raw/+").unwrap(),
            Some(r#"SELECT temperature FROM "raw/temp" WHERE temperature > 0"#.to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("transformed/temp").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("SQL rule creates");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            let (stream, _) = listener.accept().await.expect("accept");
            let shared = shared.clone();
            tokio::spawn(async move {
                handle_connection(stream, shared).await.expect("handle");
            });
        }
        std::future::pending::<()>().await;
    });

    // Client A subscribes to transformed/temp as conn 401.
    let sub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect sub");
    let sub = FramedTransport::new(sub_io);
    sub.send(bind_frame(401, 1, encode_bind_meta("dashboard", true, 60)))
        .await
        .expect("bind");
    let _ = sub.recv().await.expect("recv binding");
    sub.send(subscribe_frame(
        401,
        2,
        encode_subscribe_meta(5, "dashboard", &[("transformed/temp", 1)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);

    // Client B publishes a wide payload to raw/temp as conn 402.
    let pub_io = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect pub");
    let publ = FramedTransport::new(pub_io);
    publ.send(bind_frame(402, 1, encode_bind_meta("sensor-9", true, 60)))
        .await
        .expect("bind");
    let _ = publ.recv().await.expect("recv binding");
    let raw = br#"{ "temperature": 85.0, "secret": "hide_me" }"#;
    let (meta, payload) = encode_publish_meta("raw/temp", 0, 0, false, raw);
    publ.send(publish_frame(402, 2, meta, payload))
        .await
        .expect("publish");

    // Client A receives only the projected field over its transport.
    let routed = sub.recv().await.expect("recv transformed");
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    assert_eq!(routed.header.conn_id, 401);
    let (topic, _, _, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(topic, "transformed/temp");
    let body: serde_json::Value = serde_json::from_slice(&routed.payload).expect("payload is JSON");
    assert_eq!(body, serde_json::json!({ "temperature": 85.0 }));

    server.abort();
}

#[tokio::test]
async fn rule_sql_forwards_to_http_webhook() {
    use broker_connectors::HttpWebhookSink;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    // Minimal HTTP capture endpoint over raw TCP (keeps broker-node
    // free of HTTP server deps): records one POST body, answers 200.
    let hook_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind hook");
    let hook_port = hook_listener.local_addr().expect("addr").port();
    let captured: Arc<parking_lot::Mutex<Option<Vec<u8>>>> =
        Arc::new(parking_lot::Mutex::new(None));
    let captured_rx = captured.clone();
    let hook_task = tokio::spawn(async move {
        let (mut stream, _) = hook_listener.accept().await.expect("accept hook");
        let mut buf = Vec::new();
        loop {
            let mut chunk = [0u8; 1024];
            let n = stream.read(&mut chunk).await.expect("read head");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
            if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                break;
            }
        }
        let head_end = buf
            .windows(4)
            .position(|w| w == b"\r\n\r\n")
            .expect("request head")
            + 4;
        let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
        let mut content_length = 0usize;
        for line in head.lines() {
            if let Some((key, value)) = line.split_once(':') {
                if key.trim().eq_ignore_ascii_case("content-length") {
                    content_length = value.trim().parse().unwrap_or(0);
                }
            }
        }
        while buf.len() < head_end + content_length {
            let mut chunk = [0u8; 1024];
            let n = stream.read(&mut chunk).await.expect("read body");
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&chunk[..n]);
        }
        *captured_rx.lock() = Some(buf[head_end..head_end + content_length].to_vec());
        stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .expect("write hook response");
    });

    let shared = test_shared();
    // Rule projects telemetry and forwards to the webhook sink: no
    // MQTT subscribers are involved at all.
    let sink = Arc::new(HttpWebhookSink::new(
        format!("http://127.0.0.1:{hook_port}/hook"),
        reqwest::header::HeaderMap::new(),
        reqwest::Client::new(),
    ));
    shared.engine.connectors().register("webhook-1", sink);
    shared
        .engine
        .create_rule(
            "telemetry-webhook".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(r#"SELECT temperature FROM "sensors/+" WHERE temperature > 0"#.to_string()),
            true,
            vec![broker_rules::RuleAction::ForwardConnector {
                connector_id: "webhook-1".to_string(),
            }],
        )
        .expect("rule creates");

    // Edge client publishes wide telemetry as conn 901.
    let bl_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let bl_addr = bl_listener.local_addr().expect("local addr");
    let edge_task = tokio::spawn(async move {
        let (stream, _) = bl_listener.accept().await.expect("accept");
        handle_connection(stream, shared).await.expect("handle");
    });
    let publ = bind_client(bl_addr, 901, "sensor-x", true).await;
    let raw = br#"{ "temperature": 85.0, "secret": "hide_me" }"#;
    let (meta, payload) = encode_publish_meta("sensors/kitchen", 0, 0, false, raw);
    publ.send(publish_frame(901, 2, meta, payload))
        .await
        .expect("publish");

    // The webhook receives exactly the projected JSON.
    tokio::time::timeout(std::time::Duration::from_secs(5), hook_task)
        .await
        .expect("webhook fired")
        .expect("hook task");
    let body = captured.lock().clone().expect("captured body");
    let json: serde_json::Value = serde_json::from_slice(&body).expect("webhook body is JSON");
    assert_eq!(json, serde_json::json!({ "temperature": 85.0 }));

    edge_task.abort();
}

#[tokio::test]
async fn shared_wires_broker_sink_into_engine() {
    // INDRA-213: window-flush republish actions resolve through the
    // same in-memory sink as inline dispatch (no loopback).
    let shared = Shared::new();
    assert!(
        shared.engine.broker_sink().is_some(),
        "Shared::new must install the flush sink"
    );
}

/// B4-07 Done-when-1: fill the in-memory rule queue through the broker
/// path, spill to disk rather than erroring, replay in order.
///
/// Publishes via `ingress_pipeline` (the same connect/publish/deliver
/// path MQTT takes, never a direct store call). Each ingress mirrors
/// a durability copy through `RuleEngine::dispatch_ingress` into the
/// bounded input queue behind `push`/`try_push` (writes
/// `crates/broker-rules/src/spill.rs:580` `SpillLog::spill`, replays
/// `crates/broker-rules/src/spill.rs:637` `SpillLog::replay_one`,
/// driven by the rule-ingress event). Capacity 2 plus four publishes
/// yields two memory enqueues and two disk spills; draining via
/// `next_event` (the pressure-ease consumer, also driven by the
/// broker spill task in prod) returns memory first, then disk, oldest
/// first. Live rule matching still runs inline during the same
/// publishes.
#[tokio::test]
async fn rule_spill_broker_path_spills_and_replays_in_order() {
    let dir = {
        static SLOT: AtomicU64 = AtomicU64::new(0);
        let slot = SLOT.fetch_add(1, Ordering::SeqCst);
        std::env::temp_dir().join(format!(
            "indramqtt-rule-spill-broker-{}-{slot}",
            std::process::id()
        ))
    };
    std::fs::remove_dir_all(&dir).ok();
    let mut shared = Shared::new();
    let spill_engine = RuleEngine::new_with_spill(2, dir.clone()).expect("spill engine opens");
    spill_engine.set_metrics(&shared.metrics);
    spill_engine.set_broker_sink(shared.sink.clone());
    shared.engine = Arc::new(spill_engine);
    shared
        .engine
        .create_rule(
            "spill-broker-probe".to_string(),
            TopicFilter::new("spill/+").unwrap(),
            None,
            true,
            vec![broker_rules::RuleAction::Log],
        )
        .expect("rule creates");
    let topic = Topic::new("spill/probe").unwrap();
    for payload in [
        Bytes::from_static(b"one"),
        Bytes::from_static(b"two"),
        Bytes::from_static(b"three"),
        Bytes::from_static(b"four"),
    ] {
        // Real broker ingress, not `engine.input().push`.
        let _ = ingress_pipeline(
            &shared,
            &topic,
            QoS::AtMostOnce,
            false,
            &payload,
            broker_session::tenant::DEFAULT_TENANT_ID,
        )
        .await;
    }
    let stats = shared.engine.spill_stats();
    assert_eq!(stats.spilled, 2, "overflow spills, never errors: {stats:?}");
    assert_eq!(stats.dropped, 0);
    assert_eq!(shared.engine.input().spill_backlog_len(), 2);
    for want in [
        Bytes::from_static(b"one"),
        Bytes::from_static(b"two"),
        Bytes::from_static(b"three"),
        Bytes::from_static(b"four"),
    ] {
        let event = shared
            .engine
            .input()
            .next_event()
            .await
            .expect("replay delivers in order");
        assert_eq!(event.payload, want);
    }
    let stats = shared.engine.spill_stats();
    assert_eq!(stats.spilled, 2);
    assert_eq!(stats.replayed, 2);
    assert_eq!(stats.dropped, 0);
    assert_eq!(shared.engine.input().spill_backlog_len(), 0);
    assert_eq!(shared.metrics.rule_spill_spilled(), 2);
    assert_eq!(shared.metrics.rule_spill_replayed(), 2);
    assert_eq!(shared.metrics.rule_spill_dropped(), 0);
    shared.engine.input().sync_spill().expect("sync");
    std::fs::remove_dir_all(&dir).ok();
}

/// Enterprise window split (X1-06): the production engine built by
/// `Shared::new` carries the enterprise window executor, so a
/// windowed rule created through the management path evaluates end
/// to end through the rule engine exactly as before the split.
#[tokio::test]
async fn production_shared_engine_executes_windowed_rules() {
    #[derive(Debug, Default)]
    struct ProbeSink {
        published: std::sync::Mutex<Vec<(Topic, Bytes, QoS, bool)>>,
    }

    #[async_trait::async_trait]
    impl BrokerSink for ProbeSink {
        async fn publish(
            &self,
            topic: Topic,
            payload: Bytes,
            qos: QoS,
            retain: bool,
        ) -> Result<(), broker_rules::RuleEngineError> {
            self.published
                .lock()
                .unwrap()
                .push((topic, payload, qos, retain));
            Ok(())
        }
    }

    let shared = Shared::new();
    let rule = shared
        .engine
        .create_rule(
            "x1-06-window-probe".to_string(),
            TopicFilter::new("x106/+").unwrap(),
            Some(
                r#"SELECT avg(temperature) AS avg_temp FROM "x106/+" GROUP BY COUNTWINDOW(2)"#
                    .to_string(),
            ),
            true,
            vec![broker_rules::RuleAction::Republish {
                topic: Topic::new("x106/agg").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("windowed rule creates");
    assert_eq!(shared.engine.rule_task_count(), 1);
    let sink = Arc::new(ProbeSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    // Window flushes dispatch back through the engine's stored sink
    // (see `RuleEngine::dispatch_window_row`), not the per-call
    // ingress sink: install the probe so the COUNTWINDOW aggregate
    // below lands where this test reads it.
    shared.engine.set_broker_sink(sink_obj.clone());
    let topic = Topic::new("x106/kitchen").unwrap();
    for payload in [
        Bytes::from_static(br#"{ "temperature": 10.0 }"#),
        Bytes::from_static(br#"{ "temperature": 30.0 }"#),
    ] {
        shared
            .engine
            .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &sink_obj)
            .await;
    }
    for _ in 0..60 {
        if !sink.published.lock().unwrap().is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "one row per closed window");
    assert_eq!(published[0].0.as_str(), "x106/agg");
    let row: serde_json::Value = serde_json::from_slice(&published[0].1).expect("row is JSON");
    assert_eq!(row["avg_temp"], serde_json::json!(20.0));
    assert!(shared
        .engine
        .remove_rule(&rule.id)
        .expect("remove cannot fail"));
    assert_eq!(shared.engine.window_worker_count(), 0);
}
/// RQ-04 helpers: drive events through broker entries, never `fire_*`.
/// Each test below creates one event rule, causes the event with real
/// broker frames, drains with one probe publish, and reads the rule
/// output from a live mailbox. Short sentences only.
#[cfg(test)]
fn rq04_mailbox(
    shared: &Shared,
    conn_id: u64,
) -> tokio::sync::mpsc::UnboundedReceiver<BrokerFrame> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    shared.conns.register(conn_id, tx);
    rx
}

/// Create one republish rule over an `$events/...` source.
/// Production entry: the same `create_rule` the API calls.
#[cfg(test)]
fn rq04_rule(shared: &Shared, id: &str, source: &str, select: &str, out: &str) {
    shared
        .engine
        .create_rule(
            id.to_string(),
            TopicFilter::new("#").expect("filter parses"),
            Some(format!("SELECT {select} FROM \"{source}\"")),
            true,
            vec![broker_rules::RuleAction::Republish {
                topic: Topic::new(out).expect("out topic parses"),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("rule creates");
}

/// Bind one client through the broker bind entry. Fails on refusal.
#[cfg(test)]
async fn rq04_bind(shared: &Shared, conn_id: u64, seq: u64, client_id: &str) {
    let frame = bind_frame(conn_id, seq, encode_bind_meta(client_id, true, 60));
    let reply = apply_bind(&frame, shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0, "bind must accept {client_id}");
}

/// Subscribe one client through the broker subscribe entry.
#[cfg(test)]
async fn rq04_subscribe(shared: &Shared, conn_id: u64, seq: u64, client_id: &str, filter: &str) {
    let frame = subscribe_frame(
        conn_id,
        seq,
        encode_subscribe_meta(1, client_id, &[(filter, 0)]),
    );
    apply_subscribe(&frame, shared)
        .await
        .0
        .expect("subscribe replies");
}

/// Publish one message through the broker publish entry. Then route
/// its fan-out like the live path. Data reaches the mailbox and the
/// backlog. Dead drops fire the drop event through the same hook.
#[cfg(test)]
async fn rq04_publish(shared: &Shared, conn_id: u64, seq: u64, topic: &str, payload: &[u8]) {
    let (meta, bytes) = encode_publish_meta(topic, seq as u16, 0, false, payload);
    let frame = publish_frame(conn_id, seq, meta, bytes);
    let (_, deliveries) = apply_publish(&frame, shared).await;
    route_publish_out_deliveries(shared, &frame, deliveries);
}

/// Encode one `UnsubscribeIn` frame. Mirrors the edge contract.
#[cfg(test)]
fn rq04_unsubscribe_frame(
    conn_id: u64,
    seq: u64,
    packet_id: u16,
    client_id: &str,
    filters: &[&str],
) -> BrokerFrame {
    let id = client_id.as_bytes();
    let mut meta = Vec::new();
    meta.extend_from_slice(&packet_id.to_be_bytes());
    meta.extend_from_slice(&(id.len() as u16).to_be_bytes());
    meta.extend_from_slice(id);
    meta.extend_from_slice(&(filters.len() as u16).to_be_bytes());
    for filter in filters {
        meta.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        meta.extend_from_slice(filter.as_bytes());
    }
    BrokerFrame::new(
        OpCode::UnsubscribeIn,
        conn_id,
        seq,
        Bytes::from(meta),
        Bytes::new(),
    )
    .expect("valid unsubscribe frame")
}

/// Read every republished JSON body for one connection. Drain the
/// mailbox and the QoS 0 backlog. Rule outputs ride QoS 0. Skip
/// other frames.
#[cfg(test)]
fn rq04_collect_outputs(
    shared: &Shared,
    conn_id: u64,
    rx: &mut tokio::sync::mpsc::UnboundedReceiver<BrokerFrame>,
    out_topic: &str,
) -> Vec<serde_json::Value> {
    fn push_frame(frame: &BrokerFrame, out_topic: &str, out: &mut Vec<serde_json::Value>) {
        if frame.header.opcode != OpCode::PublishOut {
            return;
        }
        let (topic, _, _, _) = decode_publish_meta_parts(&frame.metadata);
        if topic != out_topic {
            return;
        }
        if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&frame.payload) {
            out.push(value);
        }
    }
    let mut out = Vec::new();
    while let Ok(frame) = rx.try_recv() {
        push_frame(&frame, out_topic, &mut out);
    }
    while let Some(frame) = shared.conns.pop_qos0(conn_id) {
        push_frame(&frame, out_topic, &mut out);
    }
    out
}

#[tokio::test]
async fn rq04_client_connected_through_broker() {
    let shared = test_shared();
    // Watcher setup first so its own bind fires with no rule.
    let mut rx = rq04_mailbox(&shared, 910);
    rq04_bind(&shared, 910, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 910, 2, "rq04-watcher", "out/connected").await;
    rq04_rule(
        &shared,
        "rq04-connected",
        "$events/client_connected",
        "clientid, keepalive, proto_ver, clean_start, conn_ack_code",
        "out/connected",
    );
    rq04_bind(&shared, 911, 1, "rq04-joiner").await;
    // Probe on the joiner conn drains without a new connect event.
    rq04_publish(&shared, 911, 2, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 910, &mut rx, "out/connected");
    assert_eq!(got.len(), 1, "one connect fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-joiner");
    assert_eq!(got[0]["keepalive"], 60);
    assert_eq!(got[0]["proto_ver"], 4);
    assert_eq!(got[0]["clean_start"], true);
    assert_eq!(got[0]["conn_ack_code"], 0);
}

#[tokio::test]
async fn rq04_client_connack_through_broker() {
    let shared = test_shared();
    let mut rx = rq04_mailbox(&shared, 920);
    rq04_bind(&shared, 920, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 920, 2, "rq04-watcher", "out/connack").await;
    rq04_rule(
        &shared,
        "rq04-connack",
        "$events/client_connack",
        "clientid, return_code, session_present",
        "out/connack",
    );
    rq04_bind(&shared, 921, 1, "rq04-joiner").await;
    rq04_publish(&shared, 921, 2, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 920, &mut rx, "out/connack");
    assert_eq!(got.len(), 1, "one bind fires one connack: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-joiner");
    assert_eq!(got[0]["return_code"], 0);
    assert!(got[0].get("session_present").is_some());
}

#[tokio::test]
async fn rq04_client_disconnected_clean_through_broker() {
    let shared = test_shared();
    let mut rx = rq04_mailbox(&shared, 930);
    rq04_bind(&shared, 930, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 930, 2, "rq04-watcher", "out/disconnected").await;
    rq04_rule(
        &shared,
        "rq04-disconnected",
        "$events/client_disconnected",
        "clientid, reason",
        "out/disconnected",
    );
    rq04_bind(&shared, 931, 1, "rq04-leaver").await;
    apply_unbind(&unbind_frame(931, 2, "rq04-leaver"), &shared);
    // Probe on the watcher conn drains without a new connect event.
    rq04_publish(&shared, 930, 3, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 930, &mut rx, "out/disconnected");
    assert_eq!(got.len(), 1, "one clean close fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-leaver");
    assert_eq!(got[0]["reason"], "normal");
    assert!(got[0].get("disconnect_code").is_none());
}

#[tokio::test]
async fn rq04_client_disconnected_tcp_close_through_broker() {
    let shared = test_shared();
    let mut rx = rq04_mailbox(&shared, 940);
    rq04_bind(&shared, 940, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 940, 2, "rq04-watcher", "out/tcpclose").await;
    rq04_rule(
        &shared,
        "rq04-tcpclose",
        "$events/client_disconnected",
        "clientid, reason",
        "out/tcpclose",
    );
    rq04_bind(&shared, 941, 1, "rq04-dropped").await;
    apply_disconnect(&disconnect_frame(941, 2, "rq04-dropped"), &shared).await;
    rq04_publish(&shared, 940, 3, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 940, &mut rx, "out/tcpclose");
    assert_eq!(got.len(), 1, "one TCP close fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-dropped");
    assert_eq!(got[0]["reason"], "tcp_close");
    assert!(got[0].get("disconnect_code").is_none());
}

#[tokio::test]
async fn rq04_authz_subscribe_through_broker() {
    let shared = test_shared();
    let mut rx = rq04_mailbox(&shared, 950);
    rq04_bind(&shared, 950, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 950, 2, "rq04-watcher", "out/authzsub").await;
    rq04_rule(
        &shared,
        "rq04-authzsub",
        "$events/client_check_authz_complete",
        "clientid, action, topic, allow, source",
        "out/authzsub",
    );
    rq04_bind(&shared, 951, 1, "rq04-subber").await;
    rq04_subscribe(&shared, 951, 2, "rq04-subber", "data/authz").await;
    // Probe on the subber conn drains. It adds one publish check.
    rq04_publish(&shared, 951, 3, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 950, &mut rx, "out/authzsub");
    let sub_events: Vec<_> = got
        .iter()
        .filter(|v| v["action"] == "subscribe" && v["topic"] == "data/authz")
        .collect();
    assert_eq!(sub_events.len(), 1, "one subscribe check: {got:?}");
    assert_eq!(sub_events[0]["clientid"], "rq04-subber");
    assert_eq!(sub_events[0]["allow"], true);
    assert!(sub_events[0]["topic"].is_string());
    assert!(sub_events[0].get("retain").is_none());
}

#[tokio::test]
async fn rq04_authz_publish_through_broker() {
    let shared = test_shared();
    let mut rx = rq04_mailbox(&shared, 960);
    rq04_bind(&shared, 960, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 960, 2, "rq04-watcher", "out/authzpub").await;
    rq04_rule(
        &shared,
        "rq04-authzpub",
        "$events/client_check_authz_complete",
        "clientid, action, topic, allow, source, retain",
        "out/authzpub",
    );
    rq04_bind(&shared, 961, 1, "rq04-pubber").await;
    rq04_publish(&shared, 961, 2, "data/authzpub", b"{}").await;
    // Probe on the pubber conn drains. It adds one more publish check.
    rq04_publish(&shared, 961, 3, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 960, &mut rx, "out/authzpub");
    let pub_events: Vec<_> = got
        .iter()
        .filter(|v| v["action"] == "publish" && v["topic"] == "data/authzpub")
        .collect();
    assert_eq!(pub_events.len(), 1, "one publish check: {got:?}");
    assert_eq!(pub_events[0]["clientid"], "rq04-pubber");
    assert_eq!(pub_events[0]["allow"], true);
    assert_eq!(pub_events[0]["retain"], false);
    assert_eq!(pub_events[0]["source"], "local");
}

#[tokio::test]
async fn rq04_session_subscribed_through_broker() {
    let shared = test_shared();
    let mut rx = rq04_mailbox(&shared, 970);
    rq04_bind(&shared, 970, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 970, 2, "rq04-watcher", "out/subbed").await;
    rq04_rule(
        &shared,
        "rq04-subbed",
        "$events/session_subscribed",
        "clientid, topic, qos",
        "out/subbed",
    );
    rq04_bind(&shared, 971, 1, "rq04-subber").await;
    rq04_subscribe(&shared, 971, 2, "rq04-subber", "data/subbed").await;
    rq04_publish(&shared, 971, 3, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 970, &mut rx, "out/subbed");
    let ours: Vec<_> = got.iter().filter(|v| v["topic"] == "data/subbed").collect();
    assert_eq!(ours.len(), 1, "one subscribe fires once: {got:?}");
    assert_eq!(ours[0]["clientid"], "rq04-subber");
    assert_eq!(ours[0]["qos"], 0);
}

#[tokio::test]
async fn rq04_session_unsubscribed_through_broker() {
    let shared = test_shared();
    let mut rx = rq04_mailbox(&shared, 980);
    rq04_bind(&shared, 980, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 980, 2, "rq04-watcher", "out/unsubbed").await;
    rq04_rule(
        &shared,
        "rq04-unsubbed",
        "$events/session_unsubscribed",
        "clientid, topic",
        "out/unsubbed",
    );
    rq04_bind(&shared, 981, 1, "rq04-subber").await;
    rq04_subscribe(&shared, 981, 2, "rq04-subber", "data/unsub").await;
    apply_unsubscribe(
        &rq04_unsubscribe_frame(981, 3, 7, "rq04-subber", &["data/unsub"]),
        &shared,
    )
    .await;
    rq04_publish(&shared, 981, 4, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 980, &mut rx, "out/unsubbed");
    assert_eq!(got.len(), 1, "one unsubscribe fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-subber");
    assert_eq!(got[0]["topic"], "data/unsub");
}

#[tokio::test]
async fn rq04_message_delivered_through_broker() {
    let shared = test_shared();
    let mut rx_watcher = rq04_mailbox(&shared, 990);
    let mut rx_sub = rq04_mailbox(&shared, 991);
    rq04_bind(&shared, 990, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 990, 2, "rq04-watcher", "out/delivered").await;
    rq04_bind(&shared, 991, 1, "rq04-sub").await;
    rq04_subscribe(&shared, 991, 2, "rq04-sub", "data/delivered").await;
    rq04_rule(
        &shared,
        "rq04-delivered",
        "$events/message_delivered",
        "clientid, topic, qos",
        "out/delivered",
    );
    rq04_bind(&shared, 992, 1, "rq04-pub").await;
    rq04_publish(&shared, 992, 2, "data/delivered", b"{\"v\": 1}").await;
    // Probe on the pub conn drains without new deliveries.
    rq04_publish(&shared, 992, 3, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 990, &mut rx_watcher, "out/delivered");
    assert_eq!(got.len(), 1, "one delivery fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-sub");
    assert_eq!(got[0]["topic"], "data/delivered");
    assert_eq!(got[0]["qos"], 0);
    // The subscriber still gets the data itself through the broker.
    let data = rq04_collect_outputs(&shared, 991, &mut rx_sub, "data/delivered");
    assert_eq!(data.len(), 1, "data must still deliver");
}

#[tokio::test]
async fn rq04_message_acked_through_broker() {
    let shared = test_shared();
    let mut rx_watcher = rq04_mailbox(&shared, 1000);
    let mut rx_sub = rq04_mailbox(&shared, 1001);
    rq04_bind(&shared, 1000, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 1000, 2, "rq04-watcher", "out/acked").await;
    rq04_bind(&shared, 1001, 1, "rq04-sub").await;
    let sub_frame = subscribe_frame(
        1001,
        2,
        encode_subscribe_meta(11, "rq04-sub", &[("data/acked", 1)]),
    );
    apply_subscribe(&sub_frame, &shared)
        .await
        .0
        .expect("subscribe replies");
    rq04_rule(
        &shared,
        "rq04-acked",
        "$events/message_acked",
        "clientid, topic, msg_id",
        "out/acked",
    );
    rq04_bind(&shared, 1002, 1, "rq04-pub").await;
    let (meta, bytes) = encode_publish_meta("data/acked", 5, 1, false, b"hi");
    let pub_frame = publish_frame(1002, 2, meta, bytes);
    let (_, deliveries) = apply_publish(&pub_frame, &shared).await;
    route_publish_out_deliveries(&shared, &pub_frame, deliveries);
    // Read the downlink packet id from the subscriber mailbox.
    let mut packet_id: Option<u16> = None;
    while let Ok(frame) = rx_sub.try_recv() {
        if frame.header.opcode != OpCode::PublishOut {
            continue;
        }
        let (topic, pid, _, _) = decode_publish_meta_parts(&frame.metadata);
        if topic == "data/acked" {
            packet_id = Some(pid);
        }
    }
    let packet_id = packet_id.expect("subscriber must get QoS 1 delivery");
    apply_puback(&puback_frame(1001, 3, packet_id), &shared);
    rq04_bind(&shared, 1003, 1, "rq04-probe").await;
    rq04_publish(&shared, 1003, 2, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 1000, &mut rx_watcher, "out/acked");
    assert_eq!(got.len(), 1, "one ack fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-sub");
    assert_eq!(got[0]["topic"], "data/acked");
    assert_eq!(got[0]["msg_id"], packet_id);
}

#[tokio::test]
async fn rq04_message_dropped_quota_through_broker() {
    use broker_auth::UserQuotas;
    let shared = test_shared();
    // Watcher binds first while the user store is still empty.
    let mut rx = rq04_mailbox(&shared, 1010);
    rq04_bind(&shared, 1010, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 1010, 2, "rq04-watcher", "out/dropped").await;
    rq04_rule(
        &shared,
        "rq04-dropped",
        "$events/message_dropped",
        "clientid, topic, reason",
        "out/dropped",
    );
    shared.auth.add_user("rq04-fast", b"pw").expect("user adds");
    shared
        .auth
        .set_quotas(
            "rq04-fast",
            UserQuotas {
                max_connections: None,
                max_publish_rate: Some(1),
                max_publish_burst: Some(1),
            },
        )
        .expect("quota sets");
    // Capped publisher binds with credentials through the broker.
    let frame = bind_frame(
        1011,
        1,
        encode_bind_meta_creds("rq04-throttled", true, 60, "rq04-fast", b"pw"),
    );
    let reply = apply_bind(&frame, &shared).await.expect("bind replies");
    let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
    assert_eq!(rc, 0);
    // First publish routes. Second exceeds the burst and drops.
    let (meta, bytes) = encode_publish_meta("data/drop", 1, 0, false, b"one");
    apply_publish(&publish_frame(1011, 2, meta, bytes), &shared).await;
    let (meta, bytes) = encode_publish_meta("data/drop", 2, 0, false, b"two");
    apply_publish(&publish_frame(1011, 3, meta, bytes), &shared).await;
    // Probe on the unlimited watcher conn drains the queued event.
    rq04_publish(&shared, 1010, 9, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 1010, &mut rx, "out/dropped");
    assert_eq!(got.len(), 1, "one quota drop fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-throttled");
    assert_eq!(got[0]["topic"], "data/drop");
    assert_eq!(got[0]["reason"], "quota_exceeded");
}

#[tokio::test]
async fn rq04_delivery_dropped_dead_mailbox_through_broker() {
    let shared = test_shared();
    rq04_rule(
        &shared,
        "rq04-ddropped",
        "$events/delivery_dropped",
        "clientid, topic, reason",
        "out/ddropped",
    );
    let mut rx_watcher = rq04_mailbox(&shared, 1020);
    rq04_bind(&shared, 1020, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 1020, 2, "rq04-watcher", "out/ddropped").await;
    let _ = rq04_collect_outputs(&shared, 1020, &mut rx_watcher, "out/ddropped");
    // Subscriber joins, then its mailbox dies while routing state stays.
    let _rx_dead = rq04_mailbox(&shared, 1021);
    rq04_bind(&shared, 1021, 1, "rq04-dead").await;
    rq04_subscribe(&shared, 1021, 2, "rq04-dead", "data/dead").await;
    shared.conns.unregister(1021);
    rq04_bind(&shared, 1022, 1, "rq04-pub").await;
    rq04_publish(&shared, 1022, 2, "data/dead", b"lost").await;
    rq04_bind(&shared, 1023, 1, "rq04-probe").await;
    rq04_publish(&shared, 1023, 2, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 1020, &mut rx_watcher, "out/ddropped");
    assert_eq!(got.len(), 1, "one dead mailbox fires once: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-dead");
    assert_eq!(got[0]["topic"], "data/dead");
    assert_eq!(got[0]["reason"], "dead_mailbox");
}

#[tokio::test]
async fn rq04_other_tenant_event_reaches_no_rule() {
    let shared = test_shared();
    let mut rx_watcher = rq04_mailbox(&shared, 1030);
    let mut rx_sub = rq04_mailbox(&shared, 1031);
    rq04_bind(&shared, 1030, 1, "rq04-watcher").await;
    rq04_subscribe(&shared, 1030, 2, "rq04-watcher", "out/tenant").await;
    rq04_bind(&shared, 1031, 1, "rq04-sub").await;
    rq04_subscribe(&shared, 1031, 2, "rq04-sub", "data/tenant").await;
    rq04_rule(
        &shared,
        "rq04-tenant",
        "$events/message_delivered",
        "clientid, topic",
        "out/tenant",
    );
    // Foreign publisher moves to another tenant before it publishes.
    // Its publish must neither deliver nor fire an event to the rule.
    rq04_bind(&shared, 1032, 1, "rq04-foreign-pub").await;
    if let Some(session) = shared.sessions.get("rq04-foreign-pub") {
        session.set_tenant_id("acme".to_string());
    }
    rq04_publish(&shared, 1032, 2, "data/tenant", b"{\"v\": 0}").await;
    // Default publisher delivers and fires through the broker.
    rq04_bind(&shared, 1033, 1, "rq04-default-pub").await;
    rq04_publish(&shared, 1033, 2, "data/tenant", b"{\"v\": 1}").await;
    rq04_publish(&shared, 1033, 3, "probe/drain", b"{}").await;
    let got = rq04_collect_outputs(&shared, 1030, &mut rx_watcher, "out/tenant");
    assert_eq!(got.len(), 1, "only the default tenant fires: {got:?}");
    assert_eq!(got[0]["clientid"], "rq04-sub");
    assert_eq!(got[0]["topic"], "data/tenant");
    // The foreign publish never reaches the default subscriber.
    // Only the default data frame arrives on the data topic.
    let data = rq04_collect_outputs(&shared, 1031, &mut rx_sub, "data/tenant");
    assert_eq!(data.len(), 1, "only default data delivers: {data:?}");
}
