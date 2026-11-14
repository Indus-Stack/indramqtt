// Checklist client built on the rumqttc crate.
// It speaks MQTT 3.1.1 over plain TCP through `rumqttc::AsyncClient`
// and MQTT 5.0 through `rumqttc::v5::AsyncClient`.
// It uses sockets only. It uses no broker internals.
use rumqttc::{AsyncClient, Event, EventLoop, LastWill, MqttOptions, Packet, QoS};
use std::time::Duration;

fn host() -> String {
    std::env::var("MATRIX_HOST").unwrap_or_else(|_| "127.0.0.1".to_string())
}

fn tcp_port() -> u16 {
    std::env::var("MATRIX_TCP_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1883)
}

fn api_base() -> String {
    std::env::var("MATRIX_API_BASE").unwrap_or_default()
}

fn api_key() -> String {
    std::env::var("MATRIX_API_KEY").unwrap_or_default()
}

fn prefix() -> String {
    std::env::var("MATRIX_PREFIX").unwrap_or_else(|_| "mx".to_string())
}

fn user() -> String {
    std::env::var("MATRIX_USER").unwrap_or_default()
}

fn password() -> String {
    std::env::var("MATRIX_PASSWORD").unwrap_or_default()
}

fn restart_cmd() -> String {
    std::env::var("MATRIX_RESTART_CMD").unwrap_or_default()
}

fn tag(suffix: &str) -> String {
    format!("{}-{}-{:08x}", prefix(), suffix, rand_u32())
}

fn rand_u32() -> u32 {
    let mut buf = [0u8; 4];
    let mut source = std::fs::File::open("/dev/urandom").expect("random source opens");
    use std::io::Read as _;
    source.read_exact(&mut buf).expect("random bytes read");
    u32::from_be_bytes(buf)
}

fn emit(name: &str, status: &str, detail: &str) {
    println!("RESULT {name} {status} {detail}");
}

fn opts(client_id: &str) -> MqttOptions {
    let mut options = MqttOptions::new(client_id, host(), tcp_port());
    options.set_keep_alive(Duration::from_secs(60));
    // Bound in/out so the 1 MiB payload check fits one packet budget.
    options.set_max_packet_size(2 * 1024 * 1024, 2 * 1024 * 1024);
    options
}

fn window_contains(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    haystack.windows(needle.len()).any(|w| w == needle)
}

async fn wait_connack(loop_: &mut EventLoop) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match loop_.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(_))) => return Ok(()),
                Ok(_) => continue,
                Err(err) => return Err(anyhow::anyhow!("connect refused: {err}")),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("connack timeout"))??;
    Ok(())
}

async fn wait_suback(loop_: &mut EventLoop) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match loop_.poll().await {
                Ok(Event::Incoming(Packet::SubAck(_))) => return Ok(()),
                Ok(_) => continue,
                Err(err) => return Err(anyhow::anyhow!("subscribe failed: {err}")),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("suback timeout"))??;
    Ok(())
}

async fn wait_puback(loop_: &mut EventLoop) -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match loop_.poll().await {
                Ok(Event::Incoming(Packet::PubAck(_))) => return Ok(()),
                Ok(_) => continue,
                Err(err) => return Err(anyhow::anyhow!("publish failed: {err}")),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("puback timeout"))??;
    Ok(())
}

async fn await_publish(
    loop_: &mut EventLoop,
    needle: &[u8],
    timeout: Duration,
) -> anyhow::Result<Vec<u8>> {
    tokio::time::timeout(timeout, async {
        loop {
            match loop_.poll().await {
                Ok(Event::Incoming(Packet::Publish(job)))
                    if window_contains(&job.payload, needle) =>
                {
                    return Ok(job.payload.to_vec());
                }
                Ok(_) => continue,
                Err(err) => return Err(anyhow::anyhow!("connection lost: {err}")),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("no delivery"))?
}

async fn check_v311_connect() -> anyhow::Result<String> {
    let (_client, mut loop_) = AsyncClient::new(opts(&tag("v4")), 10);
    wait_connack(&mut loop_).await?;
    Ok("CONNACK accepted (MQTT 3.1.1)".to_string())
}

async fn check_v5_connect() -> anyhow::Result<String> {
    let mut options = rumqttc::v5::MqttOptions::new(&tag("v5"), host(), tcp_port());
    options.set_keep_alive(Duration::from_secs(60));
    let (_client, mut loop_) = rumqttc::v5::AsyncClient::new(options, 10);
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match loop_.poll().await {
                Ok(rumqttc::v5::Event::Incoming(rumqttc::v5::mqttbytes::v5::Packet::ConnAck(
                    _,
                ))) => return Ok(()),
                Ok(_) => continue,
                Err(err) => return Err(anyhow::anyhow!("v5 connect refused: {err}")),
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("v5 connack timeout"))??;
    Ok("CONNACK accepted (MQTT 5.0)".to_string())
}

async fn check_qos(qos_level: u8) -> anyhow::Result<String> {
    let qos = match qos_level {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    };
    let topic = tag(&format!("qos{qos_level}"));
    let payload = format!("q{qos_level}-{:08x}", rand_u32());
    let (sub_client, mut sub_loop) = AsyncClient::new(opts(&tag("sub")), 10);
    sub_client
        .subscribe(topic.clone(), qos)
        .await
        .map_err(|e| anyhow::anyhow!("subscribe send: {e}"))?;
    wait_suback(&mut sub_loop).await?;
    let (pub_client, mut pub_loop) = AsyncClient::new(opts(&tag("pub")), 10);
    pub_client
        .publish(topic.clone(), qos, false, payload.clone().into_bytes())
        .await
        .map_err(|e| anyhow::anyhow!("publish send: {e}"))?;
    // QoS 0 has no PUBACK; the delivery check below is the proof.
    if qos != QoS::AtMostOnce {
        wait_puback(&mut pub_loop).await?;
    }
    let got = await_publish(&mut sub_loop, payload.as_bytes(), Duration::from_secs(10)).await?;
    if got != payload.as_bytes() {
        return Err(anyhow::anyhow!("payload mismatch"));
    }
    drop(pub_client);
    drop(sub_client);
    Ok(format!("delivered at QoS {qos_level}"))
}

async fn check_retained() -> anyhow::Result<String> {
    let topic = tag("retain");
    let payload = format!("retained-{:08x}", rand_u32());
    let (pub_client, mut pub_loop) = AsyncClient::new(opts(&tag("rpub")), 10);
    pub_client
        .publish(
            topic.clone(),
            QoS::AtLeastOnce,
            true,
            payload.clone().into_bytes(),
        )
        .await
        .map_err(|e| anyhow::anyhow!("publish send: {e}"))?;
    wait_puback(&mut pub_loop).await?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    drop(pub_client);
    let (sub_client, mut sub_loop) = AsyncClient::new(opts(&tag("rsub")), 10);
    sub_client
        .subscribe(topic.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|e| anyhow::anyhow!("subscribe send: {e}"))?;
    wait_suback(&mut sub_loop).await?;
    let got = await_publish(&mut sub_loop, payload.as_bytes(), Duration::from_secs(10)).await?;
    if got != payload.as_bytes() {
        return Err(anyhow::anyhow!("retained payload mismatch"));
    }
    drop(sub_client);
    Ok("late subscriber got the retained message".to_string())
}

async fn check_will() -> anyhow::Result<String> {
    let topic = tag("will");
    let payload = format!("will-{:08x}", rand_u32());
    let (obs_client, mut obs_loop) = AsyncClient::new(opts(&tag("wobs")), 10);
    obs_client
        .subscribe(topic.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|e| anyhow::anyhow!("subscribe send: {e}"))?;
    wait_suback(&mut obs_loop).await?;
    let will = LastWill::new(
        topic.clone(),
        payload.clone().into_bytes(),
        QoS::AtLeastOnce,
        false,
    );
    let mut victim_opts = opts("mx-will-victim");
    victim_opts.set_last_will(will);
    let (_victim, mut victim_loop) = AsyncClient::new(victim_opts, 10);
    wait_connack(&mut victim_loop).await?;
    // Hold the connection in a task, then abort the task for an abrupt close.
    let holder = tokio::spawn(async move {
        loop {
            if victim_loop.poll().await.is_err() {
                break;
            }
        }
    });
    tokio::time::sleep(Duration::from_millis(400)).await;
    holder.abort();
    let got = await_publish(&mut obs_loop, payload.as_bytes(), Duration::from_secs(10)).await?;
    if got != payload.as_bytes() {
        return Err(anyhow::anyhow!("will payload mismatch"));
    }
    drop(obs_client);
    Ok("will message published on an abrupt close".to_string())
}

async fn check_session_resume() -> anyhow::Result<String> {
    let cid = tag("sess");
    let topic = tag("sessq");
    let payloads: Vec<String> = (0..3)
        .map(|i| format!("queued-{i}-{:08x}", rand_u32()))
        .collect();
    let (first_client, mut first_loop) = AsyncClient::new(
        {
            let mut o = opts(&cid);
            o.set_clean_session(false);
            o
        },
        10,
    );
    first_client
        .subscribe(topic.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|e| anyhow::anyhow!("subscribe send: {e}"))?;
    wait_suback(&mut first_loop).await?;
    tokio::time::sleep(Duration::from_millis(500)).await;
    drop(first_client);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let (pub_client, mut pub_loop) = AsyncClient::new(opts(&tag("spub")), 10);
    for payload in &payloads {
        pub_client
            .publish(
                topic.clone(),
                QoS::AtLeastOnce,
                false,
                payload.clone().into_bytes(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("publish send: {e}"))?;
        wait_puback(&mut pub_loop).await?;
    }
    tokio::time::sleep(Duration::from_millis(800)).await;
    drop(pub_client);
    let (resumed_client, mut resumed_loop) = AsyncClient::new(
        {
            let mut o = opts(&cid);
            o.set_clean_session(false);
            o
        },
        10,
    );
    let mut got: Vec<Vec<u8>> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while got.len() < payloads.len() && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_secs(1), resumed_loop.poll()).await {
            Ok(Ok(Event::Incoming(Packet::Publish(job)))) => {
                got.push(job.payload.to_vec());
            }
            Ok(Ok(_)) => continue,
            Ok(Err(err)) => return Err(anyhow::anyhow!("resume loop: {err}")),
            Err(_) => continue,
        }
    }
    drop(resumed_client);
    if got.len() != payloads.len() {
        return Err(anyhow::anyhow!(
            "queued messages missing: got {} of {}",
            got.len(),
            payloads.len()
        ));
    }
    for payload in &payloads {
        if !got.iter().any(|p| p == payload.as_bytes()) {
            return Err(anyhow::anyhow!("a queued payload never arrived"));
        }
    }
    Ok(format!(
        "session resumed and replayed {} queued messages",
        got.len()
    ))
}

async fn check_shared() -> anyhow::Result<String> {
    let group = tag("grp");
    let inner = tag("shared");
    let filter = format!("$share/{group}/{inner}");
    let count = 10usize;
    let (c1, mut l1) = AsyncClient::new(opts(&tag("sh1")), 10);
    let (c2, mut l2) = AsyncClient::new(opts(&tag("sh2")), 10);
    c1.subscribe(filter.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|e| anyhow::anyhow!("subscribe1: {e}"))?;
    wait_suback(&mut l1).await?;
    c2.subscribe(filter.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|e| anyhow::anyhow!("subscribe2: {e}"))?;
    wait_suback(&mut l2).await?;
    let (pub_client, mut pub_loop) = AsyncClient::new(opts(&tag("shpub")), 10);
    for index in 0..count {
        pub_client
            .publish(
                inner.clone(),
                QoS::AtLeastOnce,
                false,
                format!("s{index}").into_bytes(),
            )
            .await
            .map_err(|e| anyhow::anyhow!("publish send: {e}"))?;
        wait_puback(&mut pub_loop).await?;
    }
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while seen.len() < count && tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(200), l1.poll()).await {
            Ok(Ok(Event::Incoming(Packet::Publish(job)))) => seen.push(job.payload.to_vec()),
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {}
        }
        match tokio::time::timeout(Duration::from_millis(200), l2.poll()).await {
            Ok(Ok(Event::Incoming(Packet::Publish(job)))) => seen.push(job.payload.to_vec()),
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {}
        }
    }
    drop(pub_client);
    drop(c1);
    drop(c2);
    if seen.len() != count {
        return Err(anyhow::anyhow!(
            "shared group got {} of {count} messages",
            seen.len()
        ));
    }
    let unique: std::collections::HashSet<&[u8]> = seen.iter().map(|p| p.as_slice()).collect();
    if unique.len() != count {
        return Err(anyhow::anyhow!("shared group delivered duplicates"));
    }
    Ok(format!(
        "shared group delivered each of {count} messages once"
    ))
}

async fn check_payload_1mib() -> anyhow::Result<String> {
    let topic = tag("big");
    let mut payload = vec![0u8; 1024 * 1024];
    let mut source = std::fs::File::open("/dev/urandom").expect("random source opens");
    use std::io::Read as _;
    source.read_exact(&mut payload).expect("random bytes read");
    let (sub_client, mut sub_loop) = AsyncClient::new(opts(&tag("bigsub")), 10);
    sub_client
        .subscribe(topic.clone(), QoS::AtLeastOnce)
        .await
        .map_err(|e| anyhow::anyhow!("subscribe send: {e}"))?;
    wait_suback(&mut sub_loop).await?;
    let (pub_client, mut pub_loop) = AsyncClient::new(opts(&tag("bigpub")), 10);
    pub_client
        .publish(topic.clone(), QoS::AtLeastOnce, false, payload.clone())
        .await
        .map_err(|e| anyhow::anyhow!("publish send: {e}"))?;
    wait_puback(&mut pub_loop).await?;
    let got = await_publish(&mut sub_loop, &payload[..16], Duration::from_secs(20)).await?;
    if got.len() != payload.len() {
        return Err(anyhow::anyhow!("1 MiB payload length {}", got.len()));
    }
    drop(pub_client);
    drop(sub_client);
    Ok("1 MiB payload delivered".to_string())
}

fn http_post(path: &str, body: &str) -> anyhow::Result<u16> {
    // Blocking HTTP for the one user-setup call. A minimal hand-rolled
    // HTTP/1.1 POST keeps this detached project free of an HTTP crate.
    use std::io::{Read as _, Write as _};
    let base = api_base();
    let key = api_key();
    if base.is_empty() || key.is_empty() {
        return Err(anyhow::anyhow!("no API entry for user setup"));
    }
    let rest = base
        .strip_prefix("http://")
        .ok_or_else(|| anyhow::anyhow!("api base must be http"))?;
    let hostport = rest.split('/').next().unwrap_or(rest).to_string();
    let mut stream = std::net::TcpStream::connect(hostport.as_str())
        .map_err(|e| anyhow::anyhow!("api connect: {e}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let request = format!(
        "POST {path} HTTP/1.1\r\nHost: {hostport}\r\nAuthorization: Bearer {key}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| anyhow::anyhow!("api write: {e}"))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| anyhow::anyhow!("api read: {e}"))?;
    response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| anyhow::anyhow!("api: bad status line"))
}

async fn check_auth() -> anyhow::Result<String> {
    let user = tag("user");
    let good = format!("Good-pw-{:08x}!", rand_u32());
    let body = format!("{{\"username\":\"{user}\",\"password\":\"{good}\"}}");
    let status = http_post("/api/v1/auth/users", &body)?;
    if status != 200 && status != 201 {
        return Err(anyhow::anyhow!("create user: HTTP {status}"));
    }
    let mut accepted_opts = opts(&tag("au-ok"));
    accepted_opts.set_credentials(user.clone(), good.clone());
    let (_accepted, mut accepted_loop) = AsyncClient::new(accepted_opts, 10);
    wait_connack(&mut accepted_loop).await?;
    let mut refused_opts = opts(&tag("au-bad"));
    refused_opts.set_credentials(user, "wrong-pw");
    let (_refused, mut refused_loop) = AsyncClient::new(refused_opts, 10);
    let refused_result = wait_connack(&mut refused_loop).await;
    if refused_result.is_ok() {
        return Err(anyhow::anyhow!("wrong password accepted"));
    }
    Ok(format!(
        "good accepted, bad refused ({})",
        refused_result.unwrap_err()
    ))
}

async fn check_keepalive() -> anyhow::Result<String> {
    // rumqttc keeps the connection alive for us, so this check uses one
    // raw socket to observe the broker's own keep-alive timeout.
    use std::io::{Read as _, Write as _};
    let keepalive: u16 = 2;
    let mut sock = std::net::TcpStream::connect((host().as_str(), tcp_port()))
        .map_err(|e| anyhow::anyhow!("probe connect: {e}"))?;
    sock.set_read_timeout(Some(Duration::from_secs(u64::from(keepalive) * 3 + 4)))
        .ok();
    let field = |value: &[u8]| {
        let mut out = Vec::with_capacity(2 + value.len());
        out.extend_from_slice(&(value.len() as u16).to_be_bytes());
        out.extend_from_slice(value);
        out
    };
    let mut body = Vec::new();
    body.extend_from_slice(b"\x00\x04MQTT\x04\xc2");
    body.extend_from_slice(&keepalive.to_be_bytes());
    body.extend_from_slice(&field(tag("ka").as_bytes()));
    body.extend_from_slice(&field(user().as_bytes()));
    body.extend_from_slice(&field(password().as_bytes()));
    let mut packet = vec![0x10u8, body.len() as u8];
    packet.extend_from_slice(&body);
    sock.write_all(&packet)
        .map_err(|e| anyhow::anyhow!("probe write: {e}"))?;
    let mut ack = [0u8; 4];
    sock.read_exact(&mut ack)
        .map_err(|e| anyhow::anyhow!("probe ack: {e}"))?;
    if ack[0] != 0x20 {
        return Err(anyhow::anyhow!("probe: not a CONNACK"));
    }
    if ack[3] != 0 {
        return Err(anyhow::anyhow!("keepalive probe CONNACK {}", ack[3]));
    }
    let start = std::time::Instant::now();
    let mut buf = [0u8; 16];
    match sock.read(&mut buf) {
        Ok(0) => Ok(format!(
            "broker closed the idle connection after {:.1}s",
            start.elapsed().as_secs_f32()
        )),
        Ok(n) => Err(anyhow::anyhow!(
            "broker sent {n} bytes on the idle connection"
        )),
        Err(err) => Err(anyhow::anyhow!("idle read: {err}")),
    }
}

async fn check_restart() -> anyhow::Result<Option<String>> {
    let cmd = restart_cmd();
    if cmd.is_empty() {
        return Ok(None);
    }
    let (_client, mut loop_) = AsyncClient::new(opts(&tag("rs")), 10);
    wait_connack(&mut loop_).await?;
    let status = std::process::Command::new("bash")
        .arg("-c")
        .arg(&cmd)
        .status()
        .map_err(|e| anyhow::anyhow!("restart command failed: {e}"))?;
    if !status.success() {
        return Err(anyhow::anyhow!("restart command failed with {status}"));
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        let (_again, mut again_loop) = AsyncClient::new(opts(&tag("rs2")), 10);
        if wait_connack(&mut again_loop).await.is_ok() {
            return Ok(Some("reconnect after restart works".to_string()));
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    Err(anyhow::anyhow!("no reconnect within 30s of the restart"))
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let transport = args.get(1).cloned().unwrap_or_else(|| "tcp".to_string());
    if transport != "tcp" {
        eprintln!("rust backend: rumqttc client uses plain TCP only");
        std::process::exit(2);
    }
    let wanted: &[String] = &args[2..];
    macro_rules! step {
        ($name:literal, $fut:expr) => {
            if wanted.is_empty() || wanted.iter().any(|w| w == $name) {
                match $fut.await {
                    Ok(detail) => emit($name, "pass", &detail),
                    Err(err) => emit($name, "fail", &err.to_string()),
                }
            }
        };
    }
    step!("v311-connect", check_v311_connect());
    step!("v5-connect", check_v5_connect());
    step!("qos0", check_qos(0));
    step!("qos1", check_qos(1));
    step!("qos2", check_qos(2));
    step!("retained", check_retained());
    step!("will", check_will());
    step!("session-resume", check_session_resume());
    step!("shared", check_shared());
    step!("payload-1mib", check_payload_1mib());
    step!("auth", check_auth());
    step!("keepalive", check_keepalive());
    if wanted.is_empty() || wanted.iter().any(|w| w == "restart") {
        match check_restart().await {
            Ok(Some(detail)) => emit("restart", "pass", &detail),
            Ok(None) => emit("restart", "not-supported", "no restart command in this run"),
            Err(err) => emit("restart", "fail", &err.to_string()),
        }
    }
}
