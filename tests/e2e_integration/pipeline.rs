use super::*;

#[tokio::test]
async fn test_e2e_router_and_rules_pipeline() {
    let router = Router::new();
    let engine = RuleEngine::new(1024, BackpressurePolicy::Block);
    let sink = Arc::new(TestBrokerSink::default());
    let broker_sink: Arc<dyn BrokerSink> = sink.clone();

    // 1. Install subscriptions
    let filter = TopicFilter::new("industrial/+/telemetry").unwrap();
    router.subscribe(
        &filter,
        Subscription {
            client_id: "scada-gateway-1".into(),
            conn_id: 101,
            qos: QoS::AtLeastOnce,
            group: None,
            tenant: broker_router::DEFAULT_TENANT_ID.into(),
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
            subscription_id: 0,
        },
    );

    // 2. Install streaming rule with projection
    engine
        .create_rule(
            "temp_alarm".to_string(),
            TopicFilter::new("industrial/+/telemetry").unwrap(),
            Some(
                r#"SELECT machine_id, temperature FROM "industrial/+/telemetry" WHERE temperature > 85.0"#
                    .to_string(),
            ),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("alarms/high_temperature").unwrap(),
                qos: QoS::AtLeastOnce,
            }],
        )
        .expect("rule created");

    // 3. Ingest matching event below threshold (router matches, but rule filter suppresses action)
    let normal_topic = Topic::new("industrial/line1/telemetry").unwrap();
    let normal_payload = Bytes::from_static(br#"{"machine_id": "M1", "temperature": 72.0}"#);

    assert_eq!(router.matches(&normal_topic).len(), 1);
    engine
        .dispatch_ingress(
            &normal_topic,
            &normal_payload,
            QoS::AtLeastOnce,
            &broker_sink,
        )
        .await;
    assert_eq!(sink.published.lock().unwrap().len(), 0);

    // 4. Ingest matching event above threshold (triggers rule republish action)
    let alarm_topic = Topic::new("industrial/line2/telemetry").unwrap();
    let alarm_payload = Bytes::from_static(br#"{"machine_id": "M2", "temperature": 94.5}"#);

    assert_eq!(router.matches(&alarm_topic).len(), 1);
    engine
        .dispatch_ingress(&alarm_topic, &alarm_payload, QoS::AtLeastOnce, &broker_sink)
        .await;

    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(published[0].0.as_str(), "alarms/high_temperature");
    let projected_json: serde_json::Value =
        serde_json::from_slice(&published[0].1).expect("valid projected JSON");
    assert_eq!(projected_json["machine_id"], "M2");
    assert_eq!(projected_json["temperature"], 94.5);
}

#[test]
fn test_e2e_session_lifecycle_and_offline_queue() {
    let session_mgr = SessionManager::new_with_limits(Some(50));
    let client_id = "edge-station-42";

    // Create durable session
    let (session, present) = session_mgr.get_or_create(client_id, false);
    assert!(!present);
    assert_eq!(session.client_id, client_id);

    // Queue offline messages
    for i in 1..=20 {
        let topic = Topic::new(format!("downstream/job-{i}")).unwrap();
        let payload = Bytes::from(format!("payload-{i}"));
        session.push_offline(QueuedMessage {
            topic,
            qos: QoS::AtLeastOnce,
            retain: false,
            payload,
            publish_at_ms: None,
        });
    }
    assert_eq!(session.offline_len(), 20);

    // Reconnecting retrieves the same session with all 20 queued messages
    let (resumed, present2) = session_mgr.get_or_create(client_id, false);
    assert!(present2);
    assert_eq!(resumed.id, session.id);
    assert_eq!(resumed.offline_len(), 20);

    // Drain offline messages on reconnection
    let drained = resumed.drain_offline();
    assert_eq!(drained.len(), 20);
    assert_eq!(resumed.offline_len(), 0);
}

/// Kick closes the socket of a live client through the kernel listener.
///
/// A real 3.1.1 client connects to the booted kernel. The test kicks it
/// with `DELETE /api/v5/clients/{id}` and bearer auth. The client then
/// observes TCP close. The kick sends `ConnClose` with empty metadata,
/// so a version-5 client gets no DISCONNECT either. A regressed
/// unregister-first order drops the close and the final read times out
/// instead of passing.
#[tokio::test]
async fn kicked_mqtt_client_sees_disconnect() {
    use tokio::io::AsyncReadExt;

    const CLIENT_ID: &str = "kicked-client";

    let kernel = spawn_kernel("").await;
    let mut sock = dial_mqtt(kernel.mqtt_port).await;
    send_mqtt(&mut sock, &mqtt_connect_packet(CLIENT_ID)).await;
    let (head, body) = recv_mqtt(&mut sock).await;
    assert_eq!(head, 0x20, "kernel answers with CONNACK");
    assert_eq!(body, vec![0x00, 0x00], "CONNECT is accepted");

    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{}", kernel.api_port);
    let kick = tokio::time::timeout(
        STEP,
        http.delete(format!("{base}/api/v5/clients/{CLIENT_ID}"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("kick in time")
    .expect("kick send");
    assert_eq!(kick.status(), 204, "kick removes the live client");

    let mut probe = [0u8; 1];
    let closed = tokio::time::timeout(STEP, sock.read(&mut probe))
        .await
        .expect("close in time")
        .expect("close read");
    assert_eq!(closed, 0, "kicked socket is closed");

    let again = tokio::time::timeout(
        STEP,
        http.delete(format!("{base}/api/v5/clients/{CLIENT_ID}"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("second kick in time")
    .expect("second kick send");
    assert_eq!(again.status(), 404, "kicked client is gone");

    kernel.finish().await;
}
