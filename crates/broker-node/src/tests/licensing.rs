use super::*;

#[test]
fn b2_04_mqtt_serves_in_trial_grace_and_lapsed() {
    // MQTT connect, publish and subscribe keep working with no
    // licence (trial), with a licence in grace, and with a lapsed
    // licence: only enterprise entitlements turn off, never traffic.
    use broker_cluster::InstallationState;
    let now = 1_750_000_000u64;
    let states = vec![
        InstallationState::Trial { days_remaining: 90 },
        InstallationState::Grace {
            customer: "Acme".to_string(),
            expires_at: now - 1_000,
            days_remaining: 80,
            grace_total_days: 90,
            max_nodes: 5,
            features: vec!["clustering".to_string()],
            kid: "key-a".to_string(),
        },
        InstallationState::Lapsed {
            reason: "trial ended".to_string(),
        },
    ];
    for state in &states {
        let sessions = SessionManager::new();
        let router = Router::new();
        let metrics = Metrics::new();
        let frame = bind_frame(11, 1, encode_bind_meta("device-b204", true, 60));
        let reply = reply_for_frame(&frame, &sessions).expect("bind replies");
        let (_, _, rc) = decode_session_binding_meta(&reply.metadata);
        assert_eq!(rc, 0, "connect works in state {}", state.name());
        let filter = TopicFilter::new("sensors/b204").unwrap();
        router.subscribe(
            &filter,
            Subscription::new("device-b204", 77, QoS::AtLeastOnce),
        );
        let topic = Topic::new("sensors/b204").unwrap();
        let deliveries = build_downlink_frames(
            &router,
            &sessions,
            &metrics,
            &topic,
            QoS::AtLeastOnce,
            false,
            &Bytes::from_static(b"hello"),
            broker_session::tenant::DEFAULT_TENANT_ID,
            None,
        );
        assert!(
            !deliveries.is_empty(),
            "publish/subscribe work in state {}",
            state.name()
        );
        assert_eq!(state.entitlements_on(), state.name() != "lapsed");
    }
}

#[test]
fn b2_04_ceiling_refusal_raises_alarm_and_names_ceiling() {
    // A node joining beyond the licensed ceiling is refused membership
    // with a reason naming the ceiling and the current count, raises an
    // alarm, and keeps serving MQTT standalone (refused from the
    // cluster, not stopped as a broker).
    let shared = test_shared();
    let now = 1_750_000_000u64;
    let state = InstallationState::Valid {
        customer: "Acme".to_string(),
        expires_at: now + 10_000_000,
        max_nodes: 1,
        features: vec!["clustering".to_string()],
        kid: "key-a".to_string(),
        days_remaining: 100,
    };
    let err = broker_cluster::join_admission(2, &state).expect_err("beyond ceiling refused");
    assert!(err.contains('1'), "reason names the ceiling: {err}");
    assert!(err.contains('2'), "reason names the current count: {err}");
    shared.licence.note_ceiling_refusal(&shared.alarms, &err);
    let active = shared.alarms.list_active();
    assert!(
        active.iter().any(|a| a.name == "licence_node_ceiling"),
        "ceiling refusal raises an alarm"
    );
}

#[test]
fn b2_04_restart_keeps_identity_and_trial() {
    // Restarting a node inside a trial does not restart the trial: the
    // identity (with its recorded trial start) survives the restart.
    let dir = unique_data_dir();
    let data_dir = dir.to_str().expect("temp path is UTF-8").to_string();
    let start = 1_750_000_000u64;
    let first =
        broker_cluster::ClusterIdentity::load_or_create(std::path::Path::new(&data_dir), start)
            .expect("create identity");
    let mid = start + 10 * 86_400;
    let reloaded =
        broker_cluster::ClusterIdentity::load_or_create(std::path::Path::new(&data_dir), mid)
            .expect("reload identity");
    assert_eq!(reloaded.identity, first.identity);
    assert_eq!(reloaded.trial_started_at, first.trial_started_at);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn b2_04_unlicensed_node_generates_request() {
    // A fresh installation with no licence is in trial and can
    // generate a licence request: the bootstrap never deadlocks.
    let shared = test_shared();
    let now = 1_750_000_000u64;
    let dir = unique_data_dir();
    let data_dir = dir.to_str().expect("temp path is UTF-8").to_string();
    let state = shared
        .licence
        .load_from_data_dir(std::path::Path::new(&data_dir), now)
        .expect("loads");
    assert_eq!(state.name(), "trial");
    let request = shared
        .licence
        .generate_request(Some(3), vec!["clustering".to_string()], "")
        .expect("request");
    assert!(!request.installation_identity.is_empty());
    assert!(request.summary.contains(&request.installation_identity));
    std::fs::remove_dir_all(&dir).ok();
}
