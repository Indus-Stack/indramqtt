use super::*;

#[test]
fn boot_runs_on_file_settings_and_a_typed_flag_wins() {
    let dir = unique_data_dir();
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(
        dir.join("indra.toml"),
        "[node]\nid = \"from-file\"\n\
         [listeners.api]\nbind = \"127.0.0.1:28099\"\n\
         [session]\nmax_qos0_backlog = 7\n\
         [auth]\nallow_anonymous = true\n\
         [webhook]\nbreaker_threshold = 9\n",
    )
    .expect("write indra.toml");

    // No flags. Each file setting goes to the field that the startup reads.
    let args = resolve_boot(&dir, &[]);
    assert_eq!(args.node_id, "from-file");
    assert_eq!(args.api_bind, "127.0.0.1:28099");
    assert_eq!(args.qos0_backlog, 7);
    assert!(args.allow_anonymous);
    assert_eq!(args.webhook_breaker_threshold, 9);

    // A typed flag overrides the file. The file sets the other values.
    let args = resolve_boot(&dir, &["--node-id", "from-flag", "--qos0-backlog", "3"]);
    assert_eq!(args.node_id, "from-flag");
    assert_eq!(args.qos0_backlog, 3);
    assert_eq!(args.api_bind, "127.0.0.1:28099");

    // The operator typed the default value. The flag overrides the file.
    let args = resolve_boot(&dir, &["--node-id", "indra-node-1"]);
    assert_eq!(args.node_id, "indra-node-1");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Orderly stop closes sockets, tells v5 clients why, keeps wills.
///
/// Goes through the broker: three sockets on a kernel listener, then
/// `shutdown`. v5 sees DISCONNECT 0x8B. v4 sees a plain close. The
/// will watcher sees no publish. New accepts are refused.
#[tokio::test]
async fn orderly_stop_closes_native_connections_without_will() {
    // Encode one remaining length as MQTT varint bytes.
    fn remaining_len(mut len: usize) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut byte = (len % 128) as u8;
            len /= 128;
            if len > 0 {
                byte |= 0x80;
            }
            out.push(byte);
            if len == 0 {
                break;
            }
        }
        out
    }
    // Build one 3.1.1 CONNECT packet.
    fn connect_v4(client_id: &str) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0x00, 0x3C];
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        let mut packet = vec![0x10];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    // Build one v5 CONNECT packet with a will.
    fn connect_v5_will(client_id: &str, topic: &str, payload: &[u8]) -> Vec<u8> {
        let mut body = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x06, 0x00, 0x3C];
        body.push(0x00);
        body.extend_from_slice(&(client_id.len() as u16).to_be_bytes());
        body.extend_from_slice(client_id.as_bytes());
        body.push(0x00);
        body.extend_from_slice(&(topic.len() as u16).to_be_bytes());
        body.extend_from_slice(topic.as_bytes());
        body.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        body.extend_from_slice(payload);
        let mut packet = vec![0x10];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    // Build one 3.1.1 SUBSCRIBE packet for one filter.
    fn subscribe_once(packet_id: u16, filter: &str) -> Vec<u8> {
        let mut body = packet_id.to_be_bytes().to_vec();
        body.extend_from_slice(&(filter.len() as u16).to_be_bytes());
        body.extend_from_slice(filter.as_bytes());
        body.push(0x00);
        let mut packet = vec![0x82];
        packet.extend_from_slice(&remaining_len(body.len()));
        packet.extend_from_slice(&body);
        packet
    }
    // Read one full MQTT packet from a socket.
    async fn read_packet(stream: &mut tokio::net::TcpStream) -> std::io::Result<Vec<u8>> {
        use tokio::io::AsyncReadExt as _;
        let mut first = [0u8; 1];
        stream.read_exact(&mut first).await?;
        let mut packet = vec![first[0]];
        let mut multiplier = 1usize;
        let mut remaining = 0usize;
        loop {
            let mut byte = [0u8; 1];
            stream.read_exact(&mut byte).await?;
            packet.push(byte[0]);
            remaining += ((byte[0] & 0x7F) as usize) * multiplier;
            multiplier *= 128;
            if byte[0] & 0x80 == 0 {
                break;
            }
            if multiplier > 128 * 128 * 128 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "length runs away",
                ));
            }
        }
        if remaining > 0 {
            let mut body = vec![0u8; remaining];
            stream.read_exact(&mut body).await?;
            packet.extend_from_slice(&body);
        }
        Ok(packet)
    }
    // Read one packet, at most five seconds.
    async fn read_soon(stream: &mut tokio::net::TcpStream) -> std::io::Result<Vec<u8>> {
        tokio::time::timeout(std::time::Duration::from_secs(5), read_packet(stream))
            .await
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "no packet in time"))?
    }

    use tokio::io::AsyncWriteExt as _;
    let shared = test_shared();
    // One free loopback port for the test listener.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("probe binds");
    let port = probe.local_addr().expect("probe addr").port();
    drop(probe);
    // Boot one kernel-owned TCP row on that port. No `native` key:
    // the kernel serves the row by default.
    let mut listeners = broker_config::schema::ListenersConf::default();
    listeners.tcp.enabled = true;
    listeners.tcp.bind = format!("127.0.0.1:{port}");
    listeners.tls.enabled = false;
    listeners.ws.enabled = false;
    listeners.wss.enabled = false;
    shared.listener_store.reset_from_config(&listeners);
    let control = std::sync::Arc::new(crate::native::NativeListenerController::new(
        shared.clone(),
        listeners,
    ));
    shared.listener_store.set_controller(control.clone());
    let row = shared
        .listener_store
        .get("tcp:default")
        .expect("tcp row exists");
    broker_api::v5::listeners::ListenerController::start_listener(&*control, &row)
        .expect("listener starts");
    let mut running = row;
    running.running = true;
    shared.listener_store.update(running);
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    // A watcher waits on the will topic.
    let mut watcher = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("watcher dials");
    watcher
        .write_all(&connect_v4("shutdown-watcher"))
        .await
        .expect("watcher CONNECT sends");
    assert_eq!(
        read_soon(&mut watcher)
            .await
            .expect("watcher CONNACK arrives"),
        vec![0x20, 0x02, 0x00, 0x00]
    );
    watcher
        .write_all(&subscribe_once(1, "shutdown/will"))
        .await
        .expect("watcher SUBSCRIBE sends");
    let suback = read_soon(&mut watcher)
        .await
        .expect("watcher SUBACK arrives");
    assert_eq!(suback[0], 0x90);
    assert_eq!(suback[suback.len() - 1], 0x00);
    // A v5 client with a will connects.
    let mut fifth = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("v5 dials");
    fifth
        .write_all(&connect_v5_will("shutdown-v5", "shutdown/will", b"bye"))
        .await
        .expect("v5 CONNECT sends");
    let connack = read_soon(&mut fifth).await.expect("v5 CONNACK arrives");
    assert_eq!(connack[0], 0x20);
    let mut at = 1usize;
    while connack[at] & 0x80 != 0 {
        at += 1;
    }
    let body = &connack[at + 1..];
    assert!(body.len() >= 2 && body[1] == 0, "v5 CONNACK accepts");
    // A plain v4 client connects.
    let mut fourth = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("v4 dials");
    fourth
        .write_all(&connect_v4("shutdown-v4"))
        .await
        .expect("v4 CONNECT sends");
    assert_eq!(
        read_soon(&mut fourth).await.expect("v4 CONNACK arrives"),
        vec![0x20, 0x02, 0x00, 0x00]
    );
    assert_eq!(shared.sessions.connected_count(), 3);
    // Stop the broker in order.
    shutdown(&shared).await;
    // v5 sees DISCONNECT 0x8B, then the close.
    assert_eq!(
        read_soon(&mut fifth).await.expect("v5 DISCONNECT arrives"),
        vec![0xE0, 0x01, 0x8B]
    );
    assert!(read_soon(&mut fifth).await.is_err(), "v5 socket closes");
    // v4 sees a plain close with no packet.
    assert!(read_soon(&mut fourth).await.is_err(), "v4 socket closes");
    // The will never arrives: the close is clean.
    if let Ok(Ok(packet)) =
        tokio::time::timeout(std::time::Duration::from_secs(1), read_packet(&mut watcher)).await
    {
        assert_ne!(packet[0], 0x30, "no will publish arrives");
    }
    // The count is zero and new accepts are refused.
    assert_eq!(shared.sessions.connected_count(), 0);
    assert!(
        tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_err(),
        "accepts stop"
    );
}

#[test]
fn check_config_accepts_a_valid_dir_and_refuses_a_bad_dir() {
    // The production entries are the flag and the file.
    // The broker validates the layers and reports `ok`.
    let dir = unique_data_dir();
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(dir.join("indra.toml"), "[node]\nid = \"check-ok\"\n")
        .expect("write indra.toml");
    let argv = vec![
        "indramqtt".to_string(),
        "--config-dir".to_string(),
        dir.to_str().expect("temp path is UTF-8").to_string(),
        "--check-config".to_string(),
    ];
    let matches = <Args as clap::CommandFactory>::command().get_matches_from(argv);
    let mut args = <Args as clap::FromArgMatches>::from_arg_matches(&matches).expect("flags parse");
    assert!(args.check_config);
    // `Ok("ok")` is stdout `ok` with exit 0 in `main`.
    let outcome = check_config_outcome(&mut args, &matches);
    assert_eq!(outcome.expect("valid dir reports ok"), "ok");
    let _ = std::fs::remove_dir_all(&dir);

    // An unknown setting fails closed. The broker reports the setting.
    let bad = unique_data_dir();
    std::fs::create_dir_all(&bad).expect("create config dir");
    std::fs::write(bad.join("indra.toml"), "[node]\nidak = \"typo\"\n").expect("write indra.toml");
    let argv = vec![
        "indramqtt".to_string(),
        "--config-dir".to_string(),
        bad.to_str().expect("temp path is UTF-8").to_string(),
        "--check-config".to_string(),
    ];
    let matches = <Args as clap::CommandFactory>::command().get_matches_from(argv);
    let mut args = <Args as clap::FromArgMatches>::from_arg_matches(&matches).expect("flags parse");
    assert!(args.check_config);
    // `Err` is stderr with exit 2 in `main`.
    let err = check_config_outcome(&mut args, &matches).expect_err("unknown key must fail");
    assert!(
        err.contains("node.idak"),
        "error must name the setting, got: {err}"
    );
    let _ = std::fs::remove_dir_all(&bad);
}

#[test]
fn tm08_verify_flags_apply_from_file_and_layers() {
    // The production entries are the file and the flag layer.
    // The broker applies the setting through the resolved config.
    let dir = unique_data_dir();
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(
        dir.join("indra.toml"),
        "[listeners.tls]\nverify_client_cert = true\ncacertfile = \"/etc/indramqtt/ca.pem\"\n\
         [listeners.wss]\nverify_client_cert = true\ncacertfile = \"/etc/indramqtt/ca.pem\"\n",
    )
    .expect("write indra.toml");
    let args = resolve_boot(&dir, &[]);
    assert_eq!(args.tls_verify_client_cert, Some(true));
    assert_eq!(args.wss_verify_client_cert, Some(true));
    let layered = load_startup_config(&args, &{
        let cmd = <Args as clap::CommandFactory>::command();
        cmd.get_matches_from(vec!["indramqtt", "--config-dir", dir.to_str().unwrap()])
    })
    .expect("config resolves");
    assert!(layered.config().listeners.tls.verify_client_cert);
    assert!(layered.config().listeners.wss.verify_client_cert);
    let _ = std::fs::remove_dir_all(&dir);
    // Defaults stay off, so current handshakes keep working.
    let defaults = broker_config::schema::BrokerConfig::default();
    assert!(!defaults.listeners.tls.verify_client_cert);
    assert!(!defaults.listeners.wss.verify_client_cert);
}

#[test]
fn deprecated_edge_settings_warn_at_startup() {
    // The production entries are the file, the flag layer and
    // the environment layer. The broker warns per deprecated
    // setting that a layer sets.
    let dir = unique_data_dir();
    std::fs::create_dir_all(&dir).expect("create config dir");
    std::fs::write(
        dir.join("indra.toml"),
        "[listeners.tcp]\nnative = true\n[listeners.tls]\nnative = false\n[node]\nbrokerlink_bind = \"127.0.0.1:18883\"\n",
    )
    .expect("write indra.toml");
    let layered = broker_config::layers::load_layered_with_env(
        &dir,
        &[],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    let warnings = deprecated_startup_warnings(&layered);
    assert_eq!(warnings.len(), 3, "got: {warnings:?}");
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("listeners.tcp.native") && w.contains("deprecated and ignored")),
        "got: {warnings:?}"
    );
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("listeners.tls.native") && w.contains("the edge was removed")),
        "got: {warnings:?}"
    );
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("node.brokerlink_bind") && w.contains("deprecated")),
        "got: {warnings:?}"
    );
    std::fs::remove_dir_all(&dir).ok();

    // Defaults set no deprecated setting. The broker stays quiet.
    let clean = unique_data_dir();
    std::fs::create_dir_all(&clean).expect("create config dir");
    let layered = broker_config::layers::load_layered_with_env(
        &clean,
        &[],
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    assert!(deprecated_startup_warnings(&layered).is_empty());
    std::fs::remove_dir_all(&clean).ok();

    // The flag layer warns for `--brokerlink-bind`.
    // The broker ignores the flag and serves clients directly.
    let flag_dir = unique_data_dir();
    std::fs::create_dir_all(&flag_dir).expect("create config dir");
    let flag_cli = broker_config::layers::CliOverrides {
        brokerlink_bind: Some("127.0.0.1:18883".to_string()),
        ..Default::default()
    };
    let layered = broker_config::layers::load_layered_with_env(&flag_dir, &[], &flag_cli)
        .expect("layers load");
    let warnings = deprecated_startup_warnings(&layered);
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("--brokerlink-bind") && w.contains("deprecated")),
        "got: {warnings:?}"
    );
    std::fs::remove_dir_all(&flag_dir).ok();

    // The environment layer warns for `INDRA_NODE__BROKERLINK_BIND`.
    // The broker ignores the value and serves clients directly.
    let env_dir = unique_data_dir();
    std::fs::create_dir_all(&env_dir).expect("create config dir");
    let env = vec![(
        "INDRA_NODE__BROKERLINK_BIND".to_string(),
        "127.0.0.1:18883".to_string(),
    )];
    let layered = broker_config::layers::load_layered_with_env(
        &env_dir,
        &env,
        &broker_config::layers::CliOverrides::default(),
    )
    .expect("layers load");
    let warnings = deprecated_startup_warnings(&layered);
    assert!(
        warnings
            .iter()
            .any(|w| w.contains("node.brokerlink_bind") && w.contains("deprecated")),
        "got: {warnings:?}"
    );
    std::fs::remove_dir_all(&env_dir).ok();
}

#[test]
fn datadir_boot_creates_and_loads() {
    let dir = unique_data_dir();
    let data_dir = dir.to_str().expect("temp path is UTF-8").to_string();
    assert!(!dir.exists(), "scratch data dir must start missing");

    // Fresh boot: creates the directory, loads validated defaults.
    let registry = load_data_dir_registry(&data_dir).expect("fresh data dir boots");
    assert!(dir.is_dir(), "boot creates the data directory");
    assert_eq!(
        *registry.snapshot(),
        broker_config::FullSnapshot::default(),
        "fresh boot loads empty defaults"
    );

    // First save path creates `state.toml`.
    registry.save().expect("save defaults");
    assert!(
        dir.join(broker_config::STATE_FILE_NAME).is_file(),
        "first save creates state.toml"
    );

    // A pre-existing valid `state.toml` loads its contents into the
    // snapshot exposed through the API state.
    registry
        .commit_admin_users(broker_config::AdminUsersConf {
            users: vec![broker_config::AdminUser {
                username: "admin".to_string(),
                password_hash: "hash-admin".to_string(),
                role: "administrator".to_string(),
                ..Default::default()
            }],
        })
        .expect("commit admin root");
    registry.save().expect("save admin root");
    let reloaded = load_data_dir_registry(&data_dir).expect("reload data dir");
    let (edge_tx, edge_rx) = unbounded_channel::<BrokerFrame>();
    drop(edge_rx);
    let api = broker_api::ApiState::new(
        Arc::new(RuleEngine::new(16, BackpressurePolicy::DropOldest)),
        Arc::new(SessionManager::new()),
        Arc::new(Router::new()),
        Arc::new(Metrics::new()),
        Arc::new(MemoryAuth::new()),
        Arc::new(ConnTable::default()),
        Arc::new(reloaded),
        "indra-node-1".to_string(),
        edge_tx,
    );
    let snapshot = api.config.snapshot();
    assert_eq!(snapshot.admin_users.users.len(), 1);
    assert_eq!(snapshot.admin_users.users[0].username, "admin");
    assert_eq!(api.node_id, "indra-node-1");

    std::fs::remove_dir_all(&dir).ok();
}
