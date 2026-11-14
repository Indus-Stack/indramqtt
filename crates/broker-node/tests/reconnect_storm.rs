//! Burst reconnect test for the kernel listener.
//!
//! One kernel boots from a file with password users and one tenant
//! rule. Clients connect in one burst, then the listener stops and
//! starts again through the management API, and all clients reconnect
//! in one burst against the same process. The test asserts that each
//! reconnect gets CONNACK success inside the deadline and that the log
//! holds no accept error. It uses real sockets and the production
//! config file and API. It uses no harness and no store call.
//!
//! Scale: full scale uses 10 000 clients and 100 users with a 15 s
//! deadline. Reason: the plan names these numbers for the storm.
//! Default scale uses 200 clients and 5 users with a 60 s deadline.
//! Reason: the default run stays fast on small runners. Set
//! STORM_FULL=1 for full scale on a host with raised limits. The
//! soak job runs full scale; the default run stays reduced.
//!
//! Authentication cost: the kernel counts each blocking password hash
//! and reports it through `GET /api/v1/auth/verify_stats`. The storm
//! burst prints `BENCH auth_verifications` with the delta over the
//! burst and `BENCH reconnect_seconds` with the burst time. Reason:
//! the ruling requires the measured cost, not a constant.
//! A wrong password still fails after the restart. Reason: it proves
//! the listener still enforces the users, so the burst authenticates.
//!
//! File descriptors: one client holds one socket and the kernel holds
//! one socket per client. Reason: the operator raises `RLIMIT_NOFILE`
//! above twice the client count plus 100 for logs and API use.
//! The accept backlog stays at the default 1024. Reason: 1024 absorbs
//! bursts without an unbounded accept queue (schema default).

use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

// Bound for one socket step. Reason: a missing packet fails fast.
const STEP: Duration = Duration::from_secs(10);
// Bound for one kernel boot. Reason: slow disks still boot in time.
const BOOT: Duration = Duration::from_secs(60);
// API key for user creation. Reason: length passes the minimum check.
const API_KEY: &str = "storm-test-key-000001";
// Task cap for one burst. Reason: one task per client stays bounded.
const MAX_BURST_TASKS: usize = 10_000;
// Listener row of the kernel TCP listener. Reason: the boot registers
// the file section under this id.
const LISTENER_ID: &str = "tcp:default";

// One booted kernel for the test.
struct Kernel {
    child: tokio::process::Child,
    mqtt_port: u16,
    api_port: u16,
    scratch: std::path::PathBuf,
    config_dir: std::path::PathBuf,
    log_path: std::path::PathBuf,
}

// Scale of one run.
struct Scale {
    clients: usize,
    users: usize,
    deadline: Duration,
}

impl Scale {
    // Reads scale from the environment. Full scale needs STORM_FULL=1.
    fn read() -> Self {
        if std::env::var("STORM_FULL").as_deref() == Ok("1") {
            // Reason: the plan names 10 000 clients and a 15 s deadline.
            Self {
                clients: 10_000,
                users: 100,
                deadline: Duration::from_secs(15),
            }
        } else {
            // Reason: 200 clients prove the burst path in seconds.
            Self {
                clients: 200,
                users: 5,
                deadline: Duration::from_secs(60),
            }
        }
    }
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

// Unique tag for temp dirs of one run.
fn tag() -> String {
    format!(
        "storm-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

// Writes the kernel config file with ephemeral ports.
fn write_config(
    config_dir: &std::path::Path,
    scratch: &std::path::Path,
    tag: &str,
    mqtt_port: u16,
    api_port: u16,
) {
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"storm-{tag}\"\n\
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
             level = \"warn\"\n\
             \n\
             [auth]\n\
             password_hash = \"bcrypt\"\n\
             \n\
             [[tenants.rules]]\n\
             expression = \"t-storm\"\n",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");
}

// Starts the kernel binary. Append keeps the old log lines.
fn spawn_child(
    config_dir: &std::path::Path,
    log_path: &std::path::Path,
    append: bool,
) -> tokio::process::Child {
    let log_file = if append {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .expect("open kernel log")
    } else {
        std::fs::File::create(log_path).expect("create kernel log")
    };
    tokio::process::Command::new(kernel_binary())
        .arg("--config-dir")
        .arg(config_dir)
        .env("INDRA_API_KEYS", API_KEY)
        .stdout(std::process::Stdio::null())
        .stderr(log_file)
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real kernel")
}

// Waits until the kernel serves the API and MQTT.
async fn wait_ready(child: &mut tokio::process::Child, api_port: u16, mqtt_port: u16) {
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}");
    let deadline = std::time::Instant::now() + BOOT;
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
        if std::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            panic!("kernel never opened API {api_port} and MQTT {mqtt_port}");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("kernel exited during boot: {status}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// Boots one kernel with users disabled at start. The test adds users
// through the API, so the burst authenticates on the production path.
async fn spawn_kernel() -> Kernel {
    let tag = tag();
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}-data"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");
    let mqtt_port = ephemeral_port();
    let api_port = ephemeral_port();
    write_config(&config_dir, &scratch, &tag, mqtt_port, api_port);
    let log_path = std::env::temp_dir().join(format!("indramqtt-{tag}-kernel.log"));
    let mut child = spawn_child(&config_dir, &log_path, false);
    wait_ready(&mut child, api_port, mqtt_port).await;
    Kernel {
        child,
        mqtt_port,
        api_port,
        scratch,
        config_dir,
        log_path,
    }
}

impl Kernel {
    // Stops the kernel and removes its temp dirs and log.
    async fn finish(mut self) {
        let _ = self.child.kill().await;
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
        let _ = std::fs::remove_file(&self.log_path);
    }
}

// Base URL of the management API of one kernel.
fn api_base(kernel: &Kernel) -> String {
    format!("http://127.0.0.1:{}", kernel.api_port)
}

// Stops the kernel TCP listener through the management API.
async fn listener_stop(base: &str) {
    let http = reqwest::Client::new();
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/listeners/{LISTENER_ID}/stop"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("stop answers in time")
    .expect("stop sends");
    assert_eq!(
        res.status(),
        reqwest::StatusCode::OK,
        "listener stop answers 200"
    );
}

// Starts the kernel TCP listener through the management API.
async fn listener_start(base: &str) {
    let http = reqwest::Client::new();
    let res = tokio::time::timeout(
        STEP,
        http.post(format!("{base}/api/v5/listeners/{LISTENER_ID}/start"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("start answers in time")
    .expect("start sends");
    assert_eq!(
        res.status(),
        reqwest::StatusCode::OK,
        "listener start answers 200"
    );
}

// Waits until the MQTT port accepts a socket again.
async fn wait_mqtt(mqtt_port: u16) {
    let deadline = std::time::Instant::now() + BOOT;
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
            .await
            .is_ok()
        {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("listener never opened MQTT {mqtt_port} again");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// Asserts that the MQTT port refuses a socket.
async fn assert_mqtt_closed(mqtt_port: u16) {
    // Wait 500 ms. Reason: the accept task releases the socket.
    tokio::time::sleep(Duration::from_millis(500)).await;
    for _ in 0..10 {
        if tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
            .await
            .is_err()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("stopped listener still accepts on MQTT {mqtt_port}");
}

// Reads the live password verification counters.
async fn verify_stats(base: &str) -> (u64, u64) {
    let http = reqwest::Client::new();
    let res = tokio::time::timeout(
        STEP,
        http.get(format!("{base}/api/v1/auth/verify_stats"))
            .bearer_auth(API_KEY)
            .send(),
    )
    .await
    .expect("stats answer in time")
    .expect("stats send");
    assert_eq!(
        res.status(),
        reqwest::StatusCode::OK,
        "verify stats answer 200"
    );
    let body: serde_json::Value = res.json().await.expect("stats decode");
    (
        body["verifications"].as_u64().expect("a count"),
        body["dedup_joins"].as_u64().expect("a count"),
    )
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
    // Flag 0xC2 carries username and password. Reason: the test
    // authenticates each burst client with its own credential.
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

// Creates users through the production API in the storm tenant. The
// config maps each connection to tenant `t-storm`, so the users live
// there. The kernel hashes each password.
async fn create_users(base: &str, count: usize) {
    let http = reqwest::Client::new();
    for i in 0..count {
        let name = format!("storm-user-{i:04}");
        let secret = format!("storm-secret-{i:04}-value");
        let res = tokio::time::timeout(
            STEP,
            http.post(format!("{base}/api/v1/auth/users?tenant=t-storm"))
                .bearer_auth(API_KEY)
                .json(&serde_json::json!({"username": name, "password": secret}))
                .send(),
        )
        .await
        .expect("user create in time")
        .expect("user create sends");
        assert!(
            res.status().is_success(),
            "user {name} creation succeeds: {}",
            res.status()
        );
    }
}

// Connects one client with explicit credentials and waits for CONNACK
// success. It returns the open socket. A refusal fails the test.
async fn connect_with(
    port: u16,
    client_id: &str,
    username: &str,
    password: &[u8],
) -> tokio::net::TcpStream {
    let mut sock = tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial succeeds");
    // Keepalive 60 s. Reason: the value exceeds the burst time, so no
    // keepalive timeout closes a client mid-burst.
    let packet = connect311_auth(client_id, username, password, 60);
    tokio::time::timeout(STEP, sock.write_all(&packet))
        .await
        .expect("send in time")
        .expect("send succeeds");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock))
        .await
        .expect("connack in time")
        .expect("connack reads");
    assert_eq!(head, 0x20, "client {client_id} gets CONNACK");
    assert_eq!(body.len(), 2, "client {client_id} CONNACK holds 2 bytes");
    assert_eq!(body[1], 0, "client {client_id} CONNECT is accepted");
    sock
}

// Connects one client and waits for CONNACK success. It returns the
// open socket. The caller closes it. A refusal fails the test.
async fn connect_one(port: u16, index: usize, users: usize) -> tokio::net::TcpStream {
    let user_no = index % users;
    let client_id = format!("storm-client-{index:05}");
    let username = format!("storm-user-{user_no:04}");
    let secret = format!("storm-secret-{user_no:04}-value");
    connect_with(port, &client_id, &username, secret.as_bytes()).await
}

// Connects with explicit wrong credentials and expects a refusal. A
// close without CONNACK still counts as a refusal; only success fails.
async fn refuse_with(port: u16, client_id: &str, username: &str, wrong: &[u8]) {
    let mut sock = tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial succeeds");
    // Keepalive 60 s. Reason: the value exceeds the probe time, so no
    // keepalive timeout closes the socket mid-probe.
    let packet = connect311_auth(client_id, username, wrong, 60);
    tokio::time::timeout(STEP, sock.write_all(&packet))
        .await
        .expect("send in time")
        .expect("send succeeds");
    let res = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock))
        .await
        .expect("refusal arrives in time");
    match res {
        Ok((0x20, body)) => {
            assert_eq!(body.len(), 2, "refusal CONNACK holds 2 bytes");
            assert_ne!(body[1], 0, "wrong password is refused");
        }
        Ok((head, _)) => panic!("wrong password gets packet {head:02X}, want refusal or close"),
        Err(_) => {}
    }
}

// Connects with a wrong password and expects a refusal. It proves
// the kernel still enforces the users after the restart. A close
// without CONNACK still counts as a refusal; only success fails.
async fn connect_refused(port: u16, index: usize, users: usize) {
    let user_no = index % users;
    let client_id = format!("storm-client-{index:05}");
    let username = format!("storm-user-{user_no:04}");
    let wrong = format!("storm-secret-{user_no:04}-wrong");
    refuse_with(port, &client_id, &username, wrong.as_bytes()).await;
}

// Connects all clients at once and returns open sockets plus time.
// The task list holds at most MAX_BURST_TASKS entries. Reason: the
// burst count itself bounds the task list.
async fn burst_connect(
    port: u16,
    clients: usize,
    users: usize,
) -> (Vec<tokio::net::TcpStream>, Duration) {
    assert!(
        clients <= MAX_BURST_TASKS,
        "burst stays within the task bound"
    );
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(clients);
    for index in 0..clients {
        tasks.push(tokio::spawn(connect_one(port, index, users)));
    }
    let mut sockets = Vec::with_capacity(clients);
    for task in tasks {
        // Bound 120 s per client task. Reason: a full burst with
        // password hashing still ends well inside this bound.
        let sock = tokio::time::timeout(Duration::from_secs(120), task)
            .await
            .expect("client task ends")
            .expect("client connects");
        sockets.push(sock);
    }
    (sockets, start.elapsed())
}

// A burst of clients reconnects at once after a listener restart.
// The first burst opens the sessions against the listener. The
// listener then stops and starts again through the management API,
// and the storm burst reconnects all clients at once against the
// same process. Each reconnect uses the production socket path with
// its password credential. The gate holds 32 permits and fails closed
// past them. The burst shares few users across many clients, so one
// hash serves many waiters and the burst stays inside the bound.
// Removing the dedup map fails this test: without sharing the run
// count reaches the client count, past the credential count.
// Soak only: the plan runs this test in the deep-soak job.
// The default run stays reduced and skips it.
#[ignore]
#[tokio::test]
async fn storm_reconnect_burst_all_get_connack() {
    let scale = Scale::read();
    let kernel = spawn_kernel().await;
    let base = api_base(&kernel);

    create_users(&base, scale.users).await;

    // One timed CONNECT gives the single authentication cost on the
    // production socket path.
    let single_start = Instant::now();
    let probe = connect_one(kernel.mqtt_port, 0, scale.users).await;
    let single_elapsed = single_start.elapsed();
    drop(probe);
    assert!(
        single_elapsed <= STEP,
        "one CONNECT verifies inside {STEP:?}, took {single_elapsed:?}"
    );

    // First burst opens the sessions.
    let (sockets, _) = burst_connect(kernel.mqtt_port, scale.clients, scale.users).await;
    drop(sockets);
    // Wait 500 ms. Reason: the kernel releases the dropped sockets.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Stop the listener. New dials fail while it is down.
    listener_stop(&base).await;
    assert_mqtt_closed(kernel.mqtt_port).await;

    // Start the listener again. The storm burst hits the same process.
    listener_start(&base).await;
    wait_mqtt(kernel.mqtt_port).await;

    // The users survived the restart: a wrong password still fails.
    connect_refused(kernel.mqtt_port, 0, scale.users).await;

    // Read the counters after the restart. The delta over the storm
    // burst is the measured authentication cost of the reconnect.
    let (runs_before, _) = verify_stats(&base).await;

    // Storm burst reconnects all clients at once.
    let (sockets, elapsed) = burst_connect(kernel.mqtt_port, scale.clients, scale.users).await;
    assert!(
        elapsed <= scale.deadline,
        "all {} CONNACKs arrive within {:?}, took {:?}",
        scale.clients,
        scale.deadline,
        elapsed
    );
    let (runs_after, _) = verify_stats(&base).await;
    let verifications = runs_after.saturating_sub(runs_before);
    assert!(
        verifications <= scale.users as u64,
        "burst of {} runs {} verifications, at most {} credentials",
        scale.clients,
        verifications,
        scale.users
    );
    println!("BENCH auth_verifications {verifications} verifications");
    println!("BENCH reconnect_seconds {:.2} s", elapsed.as_secs_f64());
    drop(sockets);

    // The log holds no accept error. The accept loop logs each error
    // with this text, so its absence means no storm drop.
    // Wait 300 ms. Reason: the log flushes before the read.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let log = std::fs::read_to_string(&kernel.log_path).unwrap_or_default();
    assert!(
        !log.contains("accept error"),
        "storm causes no accept error"
    );
    kernel.finish().await;
}

// One credential shared by a burst costs one verification. N clients
// with the same username and password connect at the same time
// through real sockets. The kernel runs one blocking hash and the
// rest share it. Removing the dedup map runs N hashes and fails the
// count assert. A wrong password still fails every connection.
#[tokio::test]
async fn same_credential_burst_shares_one_verification() {
    // 16 clients prove sharing at once. Reason: 16 waiters exceed one
    // hash window while staying inside the 32 verify permits.
    const BURST: usize = 16;
    // 8 wrong passwords prove refusal propagates. Reason: 8 waiters
    // share one failed check without exhausting the permits.
    const WRONG_BURST: usize = 8;
    let kernel = spawn_kernel().await;
    let base = api_base(&kernel);
    create_users(&base, 1).await;

    let (runs_before, joins_before) = verify_stats(&base).await;
    let gate = std::sync::Arc::new(tokio::sync::Barrier::new(BURST + 1));
    let mut tasks = Vec::with_capacity(BURST);
    for index in 0..BURST {
        let gate = std::sync::Arc::clone(&gate);
        let port = kernel.mqtt_port;
        tasks.push(tokio::spawn(async move {
            gate.wait().await;
            connect_with(
                port,
                &format!("dedup-client-{index:02}"),
                "storm-user-0000",
                b"storm-secret-0000-value",
            )
            .await
        }));
    }
    gate.wait().await;
    let mut sockets = Vec::with_capacity(BURST);
    for task in tasks {
        // Bound 120 s per client task. Reason: one shared hash ends
        // well inside this bound.
        let sock = tokio::time::timeout(Duration::from_secs(120), task)
            .await
            .expect("client task ends")
            .expect("client connects");
        sockets.push(sock);
    }
    drop(sockets);
    let (runs_after, joins_after) = verify_stats(&base).await;
    assert_eq!(
        runs_after.saturating_sub(runs_before),
        1,
        "one credential costs one verification"
    );
    assert_eq!(
        joins_after.saturating_sub(joins_before),
        (BURST - 1) as u64,
        "each waiter after the leader shares the check"
    );

    // A wrong password fails every connection, even at once. The burst
    // shares one failed check and each waiter still gets a refusal.
    let (wrong_before, _) = verify_stats(&base).await;
    let gate = std::sync::Arc::new(tokio::sync::Barrier::new(WRONG_BURST + 1));
    let mut tasks = Vec::with_capacity(WRONG_BURST);
    for index in 0..WRONG_BURST {
        let gate = std::sync::Arc::clone(&gate);
        let port = kernel.mqtt_port;
        tasks.push(tokio::spawn(async move {
            gate.wait().await;
            refuse_with(
                port,
                &format!("dedup-wrong-{index:02}"),
                "storm-user-0000",
                b"storm-secret-0000-wrong",
            )
            .await;
        }));
    }
    gate.wait().await;
    for task in tasks {
        tokio::time::timeout(Duration::from_secs(120), task)
            .await
            .expect("refusal task ends")
            .expect("refusal holds");
    }
    let (wrong_after, _) = verify_stats(&base).await;
    assert_eq!(
        wrong_after.saturating_sub(wrong_before),
        1,
        "one wrong credential costs one verification"
    );
    kernel.finish().await;
}
