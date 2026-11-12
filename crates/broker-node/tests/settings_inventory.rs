//! Entry proof for the temporary accepted-settings inventory.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[test]
fn az01_settings_through_the_real_broker() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("python3")
        .arg(root.join("tests/settings_inventory/entry.py"))
        .arg(env!("CARGO_BIN_EXE_indramqtt"))
        .env(
            "AZ01_RABBITMQ_COMPILED",
            if cfg!(feature = "rabbitmq") { "1" } else { "0" },
        )
        .env(
            "AZ01_CLICKHOUSE_COMPILED",
            if cfg!(feature = "clickhouse") {
                "1"
            } else {
                "0"
            },
        )
        .env(
            "AZ01_ELASTICSEARCH_COMPILED",
            if cfg!(feature = "elasticsearch") {
                "1"
            } else {
                "0"
            },
        )
        .env(
            "AZ01_S3_COMPILED",
            if cfg!(feature = "s3") { "1" } else { "0" },
        )
        .env(
            "AZ01_AZURE_BLOB_COMPILED",
            if cfg!(feature = "azure_blob") {
                "1"
            } else {
                "0"
            },
        )
        .env(
            "AZ01_DYNAMODB_COMPILED",
            if cfg!(feature = "dynamodb") { "1" } else { "0" },
        )
        .env(
            "AZ01_KINESIS_COMPILED",
            if cfg!(feature = "kinesis") { "1" } else { "0" },
        )
        .env(
            "AZ01_BIGQUERY_COMPILED",
            if cfg!(feature = "bigquery") { "1" } else { "0" },
        )
        .current_dir(&root)
        .output()
        .expect("run the inventory entry checks");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stdout}\n{stderr}");
    assert!(stderr.contains("Ran "), "the entry suite must run cases");
    println!("{stdout}\n{stderr}");
}

#[test]
fn az01_typed_field_coverage_and_table_are_current() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for (script, args) in [("audit.py", &[][..]), ("render.py", &["--check"][..])] {
        let output = std::process::Command::new("python3")
            .arg(root.join("tests/settings_inventory").join(script))
            .args(args)
            .current_dir(&root)
            .output()
            .expect("run the inventory document checks");
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(output.status.success(), "{script}: {stdout}\n{stderr}");
        println!("{stdout}");
    }
}

// Regression test: LDAP pool_size and timeouts from indra.toml reach the
// live authenticator. Boots the kernel from a file with pool_size=4 and
// 3000 ms timeouts, drives concurrent credentialed CONNECTs against a fake
// directory that never answers binds, and asserts at most 4 simultaneous
// directory connections (the pool bound). Before the fix the default
// pool_size of 8 lets 8 concurrent connections reach the directory, so
// this assert fails. The explain API then reports the live values
// 4 / 3000 / 3000 instead of the defaults 8 / 5000 / 5000.
#[tokio::test]
async fn ldap_file_pool_and_timeouts_reach_live_authenticator() {
    const POOL_SIZE: usize = 4;
    const CONNECT_TIMEOUT_MS: u64 = 3000;
    const READ_TIMEOUT_MS: u64 = 3000;
    const API_KEY: &str = "ldap-pool-test-key-000001";
    const CLIENT_COUNT: usize = 8;

    // Fake LDAP directory: accepts TCP connections and holds them open.
    // The bind never completes, so the LDAP client times out after
    // read_timeout_ms and releases its pool slot. Tracks the maximum
    // number of simultaneous connections ever accepted.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake ldap listener binds");
    let ldap_port = listener.local_addr().unwrap().port();
    let current = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));
    let curr = current.clone();
    let maxc = max_seen.clone();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let c = curr.clone();
            let m = maxc.clone();
            tokio::spawn(async move {
                let n = c.fetch_add(1, Ordering::Relaxed) + 1;
                m.fetch_max(n, Ordering::Relaxed);
                let mut buf = [0u8; 1024];
                loop {
                    match stream.read(&mut buf).await {
                        Ok(0) => break,
                        Ok(_) => continue,
                        Err(_) => break,
                    }
                }
                let _ = c.fetch_sub(1, Ordering::Relaxed);
            });
        }
    });

    // Boot the kernel from indra.toml with pool_size=4 and 3000 ms timeouts.
    let tag = format!(
        "ldap-pool-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}-data"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");
    std::fs::create_dir_all(&scratch).expect("create temp data dir");
    let mqtt_port = ephemeral_port();
    let api_port = ephemeral_port();
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"ldap-pool-test\"\n\
             data_dir = \"{}\"\n\
             \n\
             [listeners.tcp]\n\
             bind = \"127.0.0.1:{mqtt_port}\"\n\
             \n\
             [listeners.tls]\n\
             enabled = false\n\
             \n\
             [listeners.ws]\n\
             enabled = false\n\
             \n\
             [listeners.wss]\n\
             enabled = false\n\
             \n\
             [listeners.api]\n\
             bind = \"127.0.0.1:{api_port}\"\n\
             \n\
             [logging]\n\
             level = \"error\"\n\
             \n\
             [auth]\n\
             enabled = true\n\
             allow_anonymous = true\n\
             password_hash = \"bcrypt\"\n\
             \n\
             [ldap]\n\
             server_url = \"ldap://127.0.0.1:{ldap_port}\"\n\
             base_dn = \"dc=example,dc=com\"\n\
             bind_dn = \"cn=admin,dc=example,dc=com\"\n\
             bind_password = \"secret\"\n\
             user_filter = \"(uid={{username}})\"\n\
              pool_size = {POOL_SIZE}\n\
              connect_timeout_ms = {CONNECT_TIMEOUT_MS}\n\
              read_timeout_ms = {READ_TIMEOUT_MS}\n",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");
    let log_path = std::env::temp_dir().join(format!("indramqtt-{tag}-kernel.log"));
    let log_file = std::fs::File::create(&log_path).expect("create kernel log");
    let mut child = tokio::process::Command::new(kernel_binary())
        .arg("--config-dir")
        .arg(&config_dir)
        .env("INDRA_API_KEYS", API_KEY)
        .stdout(std::process::Stdio::null())
        .stderr(log_file)
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real kernel");

    // Wait for the API and MQTT port to open.
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}");
    let boot_deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let up = tokio::time::timeout(
            Duration::from_secs(2),
            http.get(format!("{base}/healthz")).send(),
        )
        .await;
        if let Ok(Ok(res)) = up {
            if let Ok(text) = res.text().await {
                if text == "OK"
                    && tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
                        .await
                        .is_ok()
                {
                    break;
                }
            }
        }
        if std::time::Instant::now() >= boot_deadline {
            let _ = child.kill().await;
            panic!("kernel never opened API {api_port} and MQTT {mqtt_port}");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("kernel exited during boot: {status}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Drive CLIENT_COUNT concurrent credentialed CONNECTs. Each triggers
    // LDAP auth, which acquires the pool semaphore before the TCP connect
    // to the fake directory. With pool_size=4, at most 4 simultaneous
    // directory connections are ever open. The binds time out after
    // read_timeout_ms, freeing slots for the remaining connects.
    let gate = Arc::new(tokio::sync::Barrier::new(CLIENT_COUNT + 1));
    let mut tasks = Vec::with_capacity(CLIENT_COUNT);
    for index in 0..CLIENT_COUNT {
        let gate = gate.clone();
        let port = mqtt_port;
        tasks.push(tokio::spawn(async move {
            gate.wait().await;
            let mut sock = tokio::time::timeout(
                Duration::from_secs(10),
                tokio::net::TcpStream::connect(("127.0.0.1", port)),
            )
            .await
            .expect("dial in time")
            .expect("dial succeeds");
            let packet = connect311_auth(
                &format!("ldap-client-{index:03}"),
                "ldap-user",
                b"ldap-password",
                60,
            );
            tokio::time::timeout(Duration::from_secs(10), sock.write_all(&packet))
                .await
                .expect("send in time")
                .expect("send succeeds");
            // Wait for the refusal CONNACK (LDAP bind times out) or close.
            let _ =
                tokio::time::timeout(Duration::from_secs(45), read_mqtt_packet(&mut sock)).await;
        }))
    }
    // Release all clients at once.
    gate.wait().await;
    for task in tasks {
        let _ = tokio::time::timeout(Duration::from_secs(60), task)
            .await
            .expect("client task ends");
    }

    // Pool bound applied: at most POOL_SIZE simultaneous directory
    // connections. Before the fix (default pool_size=8) this would be 8.
    let max = max_seen.load(Ordering::Relaxed);
    assert_eq!(
        max, POOL_SIZE,
        "pool bound not applied: {max} concurrent directory connections, expected {POOL_SIZE}"
    );

    // The explain API reports the live file values, not the defaults
    // 8 / 5000 / 5000.
    for (key, expected) in [
        ("ldap.pool_size", "4"),
        ("ldap.connect_timeout_ms", "3000"),
        ("ldap.read_timeout_ms", "3000"),
    ] {
        let res = tokio::time::timeout(
            Duration::from_secs(10),
            http.get(format!("{base}/api/v1/config/explain?key={key}"))
                .bearer_auth(API_KEY)
                .send(),
        )
        .await
        .expect("explain answers in time")
        .expect("explain sends");
        assert_eq!(
            res.status(),
            reqwest::StatusCode::OK,
            "explain {key} answers 200"
        );
        let body: serde_json::Value = res.json().await.expect("explain decodes");
        assert_eq!(body["value"], expected, "explain {key} value is {expected}");
    }

    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&scratch);
    let _ = std::fs::remove_dir_all(&config_dir);
    let _ = std::fs::remove_file(&log_path);
}

// Locates the kernel binary under test.
fn kernel_binary() -> std::path::PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return std::path::PathBuf::from(built);
    }
    let manifest = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for candidate in [
        manifest.join("../../target/debug/indramqtt"),
        manifest.join("../../target/ci-check/debug/indramqtt"),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("kernel binary not found: build the workspace first");
}

// Takes one free loopback port.
fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe addr")
        .port()
}

// Encodes a remaining length.
fn rl(mut len: usize) -> Vec<u8> {
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

// Encodes an MQTT string.
fn mqtt_str(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

// Encodes a password field.
fn mqtt_bytes(raw: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + raw.len());
    out.extend_from_slice(&(raw.len() as u16).to_be_bytes());
    out.extend_from_slice(raw);
    out
}

// Builds an MQTT 3.1.1 CONNECT with username and password.
fn connect311_auth(client_id: &str, username: &str, password: &[u8], keepalive: u16) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0xC2];
    rest.extend_from_slice(&keepalive.to_be_bytes());
    rest.extend_from_slice(&mqtt_str(client_id));
    rest.extend_from_slice(&mqtt_str(username));
    rest.extend_from_slice(&mqtt_bytes(password));
    let mut packet = vec![0x10];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(&rest);
    packet
}

// Reads one MQTT packet from the socket.
async fn read_mqtt_packet(sock: &mut tokio::net::TcpStream) -> std::io::Result<(u8, Vec<u8>)> {
    let mut head = [0u8; 1];
    sock.read_exact(&mut head).await?;
    let mut multiplier = 1usize;
    let mut remaining = 0usize;
    loop {
        let mut byte = [0u8; 1];
        sock.read_exact(&mut byte).await?;
        remaining += ((byte[0] & 0x7F) as usize) * multiplier;
        if byte[0] & 0x80 == 0 {
            break;
        }
        multiplier *= 128;
        if multiplier > 128 * 128 * 128 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "remaining length overflow",
            ));
        }
    }
    let mut body = vec![0u8; remaining];
    sock.read_exact(&mut body).await?;
    Ok((head[0], body))
}
