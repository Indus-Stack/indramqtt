//! Time-budgeted fuzz of the MQTT listener state machine.
//!
//! One real kernel serves random and hand-built packet streams over
//! TCP. The budget comes from `FUZZ_SECONDS` (default 20). The seed
//! comes from `FUZZ_SEED` or the clock. On failure the test prints the
//! seed and stores the stream in `tests/corpus/`. Properties: the
//! kernel never panics, every connection ends or answers inside a time
//! limit, and kernel memory stays bounded.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Budget when `FUZZ_SECONDS` is unset. The ticket fixes the normal run.
const DEFAULT_FUZZ_SECONDS: u64 = 20;
/// Bound for one fuzz connection end. Reason: the broker reacts to a
/// protocol error at once; ten seconds is the CONNECT timeout, so five
/// seconds proves the connection ends without a long wait.
const END_LIMIT: Duration = Duration::from_secs(5);
/// Bound for one socket step with a short read.
const STEP: Duration = Duration::from_secs(5);
/// Bound for one kernel boot.
const BOOT: Duration = Duration::from_secs(60);
/// Kernel RSS growth bound during the fuzz run, in KiB. Reason: each
/// fuzz connection holds at most a 1 MiB packet cap and a 256-byte
/// read buffer; 64 MiB covers allocator noise and forbids real growth.
const RSS_GROWTH_KIB: u64 = 64 * 1024;
/// Largest corpus stream replayed in bytes. Reason: a corpus file is
/// one failing stream; a huge file is a write bug, not a case.
const MAX_CORPUS_BYTES: u64 = 64 * 1024;
/// API key for the management API. Long enough for the minimum length.
const API_KEY: &str = "fz-listener-fuzz-key-0001";

/// xorshift64star. Seeded and printable on failure.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, max: usize) -> usize {
        (self.next() as usize) % (max + 1)
    }

    fn byte(&mut self) -> u8 {
        self.next() as u8
    }
}

fn fuzz_seconds() -> u64 {
    std::env::var("FUZZ_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_FUZZ_SECONDS)
}

fn fuzz_seed() -> u64 {
    if let Ok(text) = std::env::var("FUZZ_SEED") {
        let trimmed = text.trim();
        if let Some(hex) = trimmed.strip_prefix("0x") {
            if let Ok(value) = u64::from_str_radix(hex, 16) {
                return value;
            }
        }
        if let Ok(value) = trimmed.parse::<u64>() {
            return value;
        }
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ (u64::from(std::process::id()) << 32)
}

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("corpus")
}

fn save_corpus(kind: &str, seed: u64, index: u64, bytes: &[u8]) -> PathBuf {
    let dir = corpus_dir();
    std::fs::create_dir_all(&dir).expect("corpus directory");
    let path = dir.join(format!("{kind}-seed{seed:016x}-i{index}.bin"));
    std::fs::write(&path, bytes).expect("write corpus file");
    path
}

fn fail(seed: u64, index: u64, kind: &str, bytes: &[u8], message: String) -> ! {
    let path = save_corpus(kind, seed, index, bytes);
    panic!(
        "listener fuzz failure seed={seed:#x} index={index} saved {}: {message}",
        path.display()
    );
}

/// One booted kernel on ephemeral ports.
struct Kernel {
    child: tokio::process::Child,
    mqtt_port: u16,
    scratch: PathBuf,
    config_dir: PathBuf,
}

impl Kernel {
    async fn finish(mut self) {
        let _ = self.child.kill().await;
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }

    fn pid(&self) -> u32 {
        self.child.id().expect("kernel pid")
    }
}

fn kernel_binary() -> PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return PathBuf::from(built);
    }
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
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

fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe addr")
        .port()
}

fn tag(name: &str) -> String {
    format!(
        "fz-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    )
}

/// Reads VmRSS of a process in KiB from procfs.
fn rss_kib(pid: u32) -> u64 {
    let text = std::fs::read_to_string(format!("/proc/{pid}/status"))
        .unwrap_or_else(|_| panic!("the kernel process exposes /proc/{pid}/status"));
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let digits: String = rest.chars().filter(|c| c.is_ascii_digit()).collect();
            return digits
                .parse()
                .unwrap_or_else(|_| panic!("VmRSS is a number"));
        }
    }
    panic!("VmRSS is missing from /proc/{pid}/status");
}

/// Boots one kernel with the kernel MQTT listener on ephemeral ports.
async fn spawn_kernel(name: &str) -> Kernel {
    for attempt in 1..=3 {
        let run_tag = format!("{}-{attempt}", tag(name));
        let scratch = std::env::temp_dir().join(format!("indramqtt-{run_tag}-data"));
        let config_dir = std::env::temp_dir().join(format!("indramqtt-{run_tag}-cfg"));
        std::fs::create_dir_all(&config_dir).expect("create temp config dir");
        let bl_port = ephemeral_port();
        let mqtt_port = ephemeral_port();
        let api_port = ephemeral_port();
        std::fs::write(
            config_dir.join("indra.toml"),
            format!(
                "[node]\n\
                 id = \"fz-{run_tag}\"\n\
                 data_dir = \"{}\"\n\
                 brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
                 \n\
                 [listeners.tcp]\n\
                 bind = \"127.0.0.1:{mqtt_port}\"\n\
                 native = true\n\
                 \n\
                 [listeners.ws]\n\
                 enabled = false\n\
                 \n\
                 [listeners.tls]\n\
                 enabled = false\n\
                 \n\
                 [listeners.wss]\n\
                 enabled = false\n\
                 \n\
                 [listeners.api]\n\
                 bind = \"127.0.0.1:{api_port}\"\n\
                 \n\
                 [logging]\n\
                 level = \"warn\"\n",
                scratch.display(),
            ),
        )
        .expect("write indra.toml");
        let mut child = tokio::process::Command::new(kernel_binary())
            .arg("--config-dir")
            .arg(&config_dir)
            .arg("--allow-anonymous")
            .env("INDRA_API_KEYS", API_KEY)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("spawn real kernel");
        let http = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{api_port}");
        let deadline = Instant::now() + BOOT;
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
                        return Kernel {
                            child,
                            mqtt_port,
                            scratch,
                            config_dir,
                        };
                    }
                }
            }
            if Instant::now() >= deadline {
                let _ = child.kill().await;
                panic!("kernel never opened API {api_port} and MQTT {mqtt_port}");
            }
            if let Ok(Some(status)) = child.try_wait() {
                let _ = child.kill().await;
                let _ = std::fs::remove_dir_all(&scratch);
                let _ = std::fs::remove_dir_all(&config_dir);
                if attempt >= 3 {
                    panic!("kernel exited during boot: {status}");
                }
                break;
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    panic!("kernel never started after retries");
}

async fn dial(port: u16) -> tokio::net::TcpStream {
    tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
        .await
        .expect("dial in time")
        .expect("dial")
}

async fn send(sock: &mut tokio::net::TcpStream, bytes: &[u8]) {
    tokio::time::timeout(STEP, sock.write_all(bytes))
        .await
        .expect("send in time")
        .expect("send");
}

/// Reads one MQTT packet: the first byte plus the body.
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

async fn recv(sock: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
    tokio::time::timeout(STEP, read_mqtt_packet(sock))
        .await
        .expect("packet in time")
        .expect("packet")
}

/// Reads until the peer closes. `true` means the connection ended
/// inside the limit; `false` is a hang defect.
async fn drain_until_close(mut sock: tokio::net::TcpStream) -> bool {
    let mut buf = [0u8; 512];
    let end = Instant::now() + END_LIMIT;
    loop {
        let left = end.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return false;
        }
        match tokio::time::timeout(left, sock.read(&mut buf)).await {
            Ok(Ok(0)) | Ok(Err(_)) => return true,
            Ok(Ok(_)) => continue,
            Err(_) => return false,
        }
    }
}

/// Named scenarios assert the end; a hang fails with the seed.
async fn expect_close(sock: tokio::net::TcpStream, seed: u64, index: u64, what: &str) {
    if !drain_until_close(sock).await {
        fail(
            seed,
            index,
            "named",
            what.as_bytes(),
            format!("{what}: connection did not end within {END_LIMIT:?}"),
        );
    }
}

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

fn mqtt_str(s: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(2 + s.len());
    out.extend_from_slice(&(s.len() as u16).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
    out
}

fn varint(mut value: u32) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
    out
}

fn wrap(first: u8, rest: &[u8]) -> Vec<u8> {
    let mut packet = vec![first];
    packet.extend_from_slice(&rl(rest.len()));
    packet.extend_from_slice(rest);
    packet
}

fn connect311(client_id: &str) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x04, 0x02, 0, 60];
    rest.extend_from_slice(&mqtt_str(client_id));
    wrap(0x10, &rest)
}

fn connect5(client_id: &str) -> Vec<u8> {
    let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0, 60, 0x00];
    rest.extend_from_slice(&mqtt_str(client_id));
    wrap(0x10, &rest)
}

fn pingreq() -> Vec<u8> {
    vec![0xC0, 0x00]
}

fn subscribe(packet_id: u16, filter: &str) -> Vec<u8> {
    let mut rest = packet_id.to_be_bytes().to_vec();
    rest.extend_from_slice(&mqtt_str(filter));
    rest.push(0);
    wrap(0x82, &rest)
}

fn pubrel(packet_id: u16) -> Vec<u8> {
    wrap(0x62, &packet_id.to_be_bytes())
}

fn publish_qos0(topic: &str, payload: &[u8]) -> Vec<u8> {
    let mut rest = mqtt_str(topic);
    rest.extend_from_slice(payload);
    wrap(0x30, &rest)
}

/// Connects and checks an accepted CONNACK. `level` is 4 or 5.
async fn connect_ok(sock: &mut tokio::net::TcpStream, client_id: &str, level: u8) {
    if level == 5 {
        send(sock, &connect5(client_id)).await;
    } else {
        send(sock, &connect311(client_id)).await;
    }
    let (head, body) = recv(sock).await;
    assert_eq!(head & 0xF0, 0x20, "CONNECT gets CONNACK");
    assert!(body.len() >= 2, "CONNACK has a code");
    assert_eq!(body[1], 0, "CONNECT of {client_id} is accepted");
}

/// Connects without a panic. `false` means the broker refused or the
/// CONNACK did not arrive in time; the caller opens a new connection.
async fn connect_soft(sock: &mut tokio::net::TcpStream, client_id: &str, level: u8) -> bool {
    if level == 5 {
        send(sock, &connect5(client_id)).await;
    } else {
        send(sock, &connect311(client_id)).await;
    }
    match tokio::time::timeout(STEP, read_mqtt_packet(sock)).await {
        Ok(Ok((head, body))) => (head & 0xF0) == 0x20 && body.len() >= 2 && body[1] == 0,
        _ => false,
    }
}

/// Builds one random packet: a valid template or junk with a legal
/// fixed-header shape so the body paths are reached.
fn random_packet(rng: &mut Rng) -> Vec<u8> {
    match rng.below(4) {
        0 => pingreq(),
        1 => publish_qos0("fz/rand", b"payload"),
        2 => {
            let len = 1 + rng.below(32);
            let body = rng_body(rng, len);
            wrap(0x30 | ((rng.below(3) as u8) << 1), &body)
        }
        _ => {
            let len = rng.below(48);
            let mut bytes = vec![0u8; len];
            for slot in bytes.iter_mut() {
                *slot = rng.byte();
            }
            if !bytes.is_empty() {
                let kind = 1 + (bytes[0] >> 4) % 15;
                let flags = match kind {
                    6 | 8 | 10 => 0x02,
                    3 => bytes[0] & 0x0F,
                    _ => 0x00,
                };
                bytes[0] = (kind << 4) | flags;
            }
            bytes
        }
    }
}

fn rng_body(rng: &mut Rng, len: usize) -> Vec<u8> {
    let mut body = Vec::with_capacity(len);
    for _ in 0..len {
        body.push(rng.byte());
    }
    body
}

/// Named illegal or hostile sequences. Each uses a fresh connection.
/// Returns the number of connections it opened.
async fn named_scenarios(port: u16, seed: u64) -> u64 {
    let mut opened = 0u64;

    // SUBSCRIBE before CONNECT.
    {
        let stream = subscribe(1, "fz/early");
        let mut sock = dial(port).await;
        send(&mut sock, &stream).await;
        expect_close(sock, seed, opened, "subscribe before connect").await;
        opened += 1;
    }

    // CONNECT twice.
    {
        let mut sock = dial(port).await;
        let id = format!("fz-twice-{opened}");
        connect_ok(&mut sock, &id, 4).await;
        send(&mut sock, &connect311(&format!("{id}-2"))).await;
        // TODO(parity): the specification may require a DISCONNECT or
        // CONNACK with reason 0x82 before the close on a second
        // CONNECT. The broker closes without a packet today.
        expect_close(sock, seed, opened, "second connect").await;
        opened += 1;
    }

    // PUBREL with no PUBREC.
    {
        let mut sock = dial(port).await;
        let id = format!("fz-pubrel-{opened}");
        connect_ok(&mut sock, &id, 4).await;
        send(&mut sock, &pubrel(42)).await;
        // TODO(parity): the reason code for an unsolicited PUBREL is
        // not decided. The broker must answer or end inside the limit.
        let read = tokio::time::timeout(END_LIMIT, read_mqtt_packet(&mut sock)).await;
        match read {
            Ok(Ok(_)) => {
                // The broker answered. A clean disconnect must end it.
                send(&mut sock, &[0xE0, 0x00]).await;
                expect_close(sock, seed, opened, "pubrel answer then close").await;
            }
            Ok(Err(_)) => {}
            Err(_) => {
                fail(
                    seed,
                    opened,
                    "named",
                    b"pubrel",
                    "unsolicited PUBREL neither answered nor closed".to_string(),
                );
            }
        }
        opened += 1;
    }

    // Packet split at every byte boundary. A valid stream must still
    // produce its replies in order.
    {
        let id = format!("fz-split-{opened}");
        let mut stream = connect311(&id);
        stream.extend_from_slice(&pingreq());
        stream.extend_from_slice(&subscribe(2, "fz/split"));
        stream.extend_from_slice(&pingreq());
        let mut sock = dial(port).await;
        for byte in stream {
            send(&mut sock, &[byte]).await;
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        for want in [0x20u8, 0xD0, 0x90, 0xD0] {
            let (head, _) = recv(&mut sock).await;
            assert_eq!(head, want, "ordered replies on a byte-split stream");
        }
        send(&mut sock, &[0xE0, 0x00]).await;
        expect_close(sock, seed, opened, "byte split clean disconnect").await;
        opened += 1;
    }

    // Remaining length at each limit under the packet cap: a header
    // with no body, then a write shutdown. The broker must end the
    // connection.
    for remaining in [0usize, 127, 128, 16_383, 16_384, 2_097_151] {
        let mut sock = dial(port).await;
        let id = format!("fz-rl-{opened}");
        connect_ok(&mut sock, &id, 4).await;
        let mut header = vec![0x30u8];
        header.extend_from_slice(&varint(remaining as u32));
        send(&mut sock, &header).await;
        let _ = sock.shutdown().await;
        expect_close(sock, seed, opened, "truncated remaining length").await;
        opened += 1;
    }

    // Remaining length over the packet cap: a v5 client gets
    // DISCONNECT 0x95 before the close.
    {
        let mut sock = dial(port).await;
        let id = format!("fz-rl-big-{opened}");
        connect_ok(&mut sock, &id, 5).await;
        let mut header = vec![0x30u8];
        header.extend_from_slice(&varint(2_097_152));
        send(&mut sock, &header).await;
        let (head, body) = recv(&mut sock).await;
        assert_eq!(head, 0xE0, "oversize gets DISCONNECT");
        assert_eq!(body, vec![0x95], "reason is 0x95");
        expect_close(sock, seed, opened, "oversize close").await;
        opened += 1;
    }

    // A five-byte remaining length is malformed.
    {
        let mut sock = dial(port).await;
        let id = format!("fz-rl5-{opened}");
        connect_ok(&mut sock, &id, 4).await;
        send(&mut sock, &[0x30, 0x80, 0x80, 0x80, 0x80, 0x01]).await;
        expect_close(sock, seed, opened, "five byte remaining length").await;
        opened += 1;
    }

    // Property length that lies on CONNECT: claims 50, carries 5.
    {
        let mut rest = vec![0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0, 60];
        rest.push(50);
        rest.extend_from_slice(&[17, 0, 0, 0, 1]);
        rest.extend_from_slice(&mqtt_str("fz/lie-props"));
        let packet = wrap(0x10, &rest);
        let mut sock = dial(port).await;
        send(&mut sock, &packet).await;
        expect_close(sock, seed, opened, "lying connect property length").await;
        opened += 1;
    }

    // Property length that lies on a PUBLISH after CONNECT.
    {
        let mut sock = dial(port).await;
        let id = format!("fz-lie-pub-{opened}");
        connect_ok(&mut sock, &id, 5).await;
        let props = [35u8, 0, 2, 1, 1];
        let mut rest = mqtt_str("fz/lie");
        rest.push(50);
        rest.extend_from_slice(&props);
        rest.extend_from_slice(b"x");
        send(&mut sock, &wrap(0x30, &rest)).await;
        expect_close(sock, seed, opened, "lying publish property length").await;
        opened += 1;
    }

    // QoS 3 PUBLISH flags close the connection.
    {
        let mut sock = dial(port).await;
        let id = format!("fz-qos3-{opened}");
        connect_ok(&mut sock, &id, 4).await;
        let mut rest = mqtt_str("fz/qos3");
        rest.extend_from_slice(b"x");
        let mut packet = vec![0x36u8];
        packet.extend_from_slice(&rl(rest.len()));
        packet.extend_from_slice(&rest);
        send(&mut sock, &packet).await;
        expect_close(sock, seed, opened, "qos 3 publish").await;
        opened += 1;
    }

    opened
}

/// Replays one committed corpus stream: raw bytes to a fresh socket.
async fn replay_corpus_file(port: u16, path: &Path, index: u64, seed: u64) {
    let Ok(bytes) = std::fs::read(path) else {
        return;
    };
    if bytes.len() as u64 > MAX_CORPUS_BYTES {
        return;
    }
    let mut sock = dial(port).await;
    let mut offset = 0usize;
    while offset < bytes.len() {
        let end = (offset + 4096).min(bytes.len());
        let write = tokio::time::timeout(STEP, sock.write_all(&bytes[offset..end])).await;
        if write.is_err() || matches!(write, Ok(Err(_))) {
            break;
        }
        offset = end;
    }
    let _ = sock.shutdown().await;
    if !drain_until_close(sock).await {
        fail(
            seed,
            index,
            "corpus",
            &bytes,
            "replayed corpus stream did not end".to_string(),
        );
    }
}

#[tokio::test]
async fn listener_wire_fuzz_stays_correct() {
    let seed = fuzz_seed();
    let budget = Duration::from_secs(fuzz_seconds());
    let deadline = Instant::now() + budget;
    eprintln!(
        "listener wire fuzz seed={seed:#x} budget={}s",
        budget.as_secs()
    );

    let kernel = spawn_kernel("wirefuzz").await;
    // Warm-up connection so the baseline RSS is after first load.
    {
        let mut sock = dial(kernel.mqtt_port).await;
        connect_ok(&mut sock, "fz-warmup", 4).await;
        send(&mut sock, &pingreq()).await;
        let (head, _) = recv(&mut sock).await;
        assert_eq!(head, 0xD0, "warm-up PINGRESP");
        send(&mut sock, &[0xE0, 0x00]).await;
        expect_close(sock, seed, 0, "warm-up disconnect").await;
    }
    let rss_before = rss_kib(kernel.pid());

    let mut connections: u64 = 1;
    let mut packets: u64 = 0;
    let named = named_scenarios(kernel.mqtt_port, seed).await;
    connections += named;

    // Committed corpus streams.
    let mut corpus_files: Vec<PathBuf> = std::fs::read_dir(corpus_dir())
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .filter(|path| path.extension().is_some_and(|ext| ext == "bin"))
                .filter(|path| {
                    std::fs::metadata(path)
                        .map(|meta| meta.len() <= MAX_CORPUS_BYTES)
                        .unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_default();
    corpus_files.sort();
    for (index, path) in corpus_files.iter().enumerate() {
        replay_corpus_file(kernel.mqtt_port, path, index as u64, seed).await;
        connections += 1;
    }

    // Random illegal-order streams until the budget ends.
    let mut rng = Rng(seed | 1);
    let mut index: u64 = 0;
    while Instant::now() < deadline {
        let mut sock = dial(kernel.mqtt_port).await;
        let id = format!("fz-rand-{index}");
        let level = if rng.below(1) == 0 { 4 } else { 5 };
        if !connect_soft(&mut sock, &id, level).await {
            let _ = sock.shutdown().await;
            index += 1;
            continue;
        }
        let count = 1 + rng.below(8);
        let mut stream = Vec::new();
        for _ in 0..count {
            stream.extend_from_slice(&random_packet(&mut rng));
        }
        let mut offset = 0usize;
        while offset < stream.len() {
            let end = (offset + 1 + rng.below(64)).min(stream.len());
            let write = tokio::time::timeout(STEP, sock.write_all(&stream[offset..end])).await;
            if write.is_err() || matches!(write, Ok(Err(_))) {
                break;
            }
            offset = end;
        }
        packets += count as u64;
        let _ = sock.shutdown().await;
        if !drain_until_close(sock).await {
            fail(
                seed,
                index,
                "stream",
                &stream,
                "random stream did not end the connection".to_string(),
            );
        }
        connections += 1;
        index += 1;
        if index.is_multiple_of(32) {
            eprintln!(
                "listener wire fuzz seed={seed:#x} connections={connections} packets={packets}"
            );
        }
    }

    // The kernel must still accept a clean client.
    {
        let mut sock = dial(kernel.mqtt_port).await;
        connect_ok(&mut sock, "fz-after", 4).await;
        send(&mut sock, &subscribe(9, "fz/after")).await;
        let (head, _) = recv(&mut sock).await;
        assert_eq!(head, 0x90, "the kernel survives the fuzz");
        send(&mut sock, &[0xE0, 0x00]).await;
        expect_close(sock, seed, index, "clean client after fuzz").await;
    }

    let rss_after = rss_kib(kernel.pid());
    let growth = rss_after.saturating_sub(rss_before);
    assert!(
        growth <= RSS_GROWTH_KIB,
        "kernel RSS grew {growth} KiB over the bound {RSS_GROWTH_KIB} KiB"
    );
    eprintln!(
        "WIRE_FUZZ listener connections={connections} packets={packets} seed={seed:#x} rss_growth_kib={growth}"
    );
    kernel.finish().await;
}
