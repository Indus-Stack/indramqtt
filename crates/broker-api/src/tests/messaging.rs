use super::*;

/// Minimal RESP array parser for the Redis loopback fake: splits
/// one `*<n>` array of bulk strings into its arguments. Returns
/// `None` when the bytes do not hold a full array yet.
#[cfg(test)]
fn mv03a_parse_resp(frame: &[u8]) -> Option<(Vec<Vec<u8>>, usize)> {
    if !frame.starts_with(b"*") {
        return None;
    }
    let mut pos = frame.iter().position(|b| *b == b'\r')?;
    if frame.get(pos + 1) != Some(&b'\n') {
        return None;
    }
    let count: usize = std::str::from_utf8(frame.get(1..pos)?).ok()?.parse().ok()?;
    pos += 2;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        if frame.get(pos) != Some(&b'$') {
            return None;
        }
        let end = pos + frame.get(pos..)?.iter().position(|b| *b == b'\r')?;
        if frame.get(end + 1) != Some(&b'\n') {
            return None;
        }
        let len: usize = std::str::from_utf8(frame.get(pos + 1..end)?)
            .ok()?
            .parse()
            .ok()?;
        pos = end + 2;
        if pos + len + 2 > frame.len() {
            return None;
        }
        args.push(frame[pos..pos + len].to_vec());
        pos += len;
        if frame.get(pos..pos + 2) != Some(b"\r\n".as_slice()) {
            return None;
        }
        pos += 2;
    }
    Some((args, pos))
}

/// One captured Redis command of the MV-03a Redis fake: the raw
/// argument list in order.
#[cfg(test)]
struct Mv03aRedisFake {
    port: u16,
    received: std::sync::Arc<std::sync::Mutex<Vec<Vec<Vec<u8>>>>>,
    _task: tokio::task::JoinHandle<()>,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[cfg(test)]
impl Drop for Mv03aRedisFake {
    #[cfg(test)]
    fn drop(&mut self) {
        // Stop the listener and every accepted connection. A
        // dropped fake must refuse the next publish, as a stopped
        // server does. Finished handles are not kept.
        self._task.abort();
        let mut connections = self.connections.lock().expect("fake tracks connections");
        for connection in connections.drain(..) {
            connection.abort();
        }
    }
}

#[cfg(test)]
impl Mv03aRedisFake {
    #[cfg(test)]
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn start_on(port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn serve(listener: tokio::net::TcpListener) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let port = listener.local_addr().expect("fake addr").port();
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_received = std::sync::Arc::clone(&received);
        let task_connections = std::sync::Arc::clone(&connections);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let received = std::sync::Arc::clone(&task_received);
                let connection = tokio::spawn(async move {
                    let mut stream = stream;
                    let mut buf = [0u8; 4096];
                    let mut pending = Vec::new();
                    loop {
                        let n = match stream.read(&mut buf).await {
                            Ok(0) => return,
                            Ok(n) => n,
                            Err(_) => return,
                        };
                        pending.extend_from_slice(&buf[..n]);
                        while !pending.is_empty() {
                            let Some((args, used)) = mv03a_parse_resp(&pending) else {
                                // A split TCP segment: wait for more.
                                break;
                            };
                            pending.drain(..used);
                            let name = args
                                .first()
                                .map(|arg| arg.to_ascii_uppercase())
                                .unwrap_or_default();
                            let reply: &[u8] = match name.as_slice() {
                                b"AUTH" | b"SELECT" | b"CLIENT" | b"HELLO" => b"+OK\r\n",
                                b"HSET" | b"SADD" | b"PUBLISH" | b"RPUSH" | b"LPUSH" | b"SETNX" => {
                                    b":1\r\n"
                                }
                                b"XADD" => b"$7\r\n1750000\r\n",
                                _ => b"+OK\r\n",
                            };
                            received.lock().expect("fake records").push(args);
                            if stream.write_all(reply).await.is_err() {
                                return;
                            }
                        }
                    }
                });
                let mut tracked = task_connections.lock().expect("fake tracks connections");
                tracked.retain(|connection| !connection.is_finished());
                tracked.push(connection);
            }
        });
        Self {
            port,
            received,
            _task: task,
            connections,
        }
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<Vec<Vec<u8>>> {
        for _ in 0..300 {
            let got = self.received.lock().expect("fake reads").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "redis fake got {} commands, wanted {count}",
            self.received.lock().expect("fake reads").len()
        );
    }
}

/// One captured RocketMQ envelope of the MV-03a RocketMQ fake.
#[cfg(test)]
struct Mv03aRocketFake {
    port: u16,
    received: std::sync::Arc<std::sync::Mutex<Vec<serde_json::Value>>>,
    _task: tokio::task::JoinHandle<()>,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[cfg(test)]
impl Drop for Mv03aRocketFake {
    #[cfg(test)]
    fn drop(&mut self) {
        // Stop the listener and every accepted connection. A
        // dropped fake must refuse the next publish, as a stopped
        // server does. Finished handles are not kept.
        self._task.abort();
        let mut connections = self.connections.lock().expect("fake tracks connections");
        for connection in connections.drain(..) {
            connection.abort();
        }
    }
}

#[cfg(test)]
impl Mv03aRocketFake {
    #[cfg(test)]
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn start_on(port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn serve(listener: tokio::net::TcpListener) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let port = listener.local_addr().expect("fake addr").port();
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_received = std::sync::Arc::clone(&received);
        let task_connections = std::sync::Arc::clone(&connections);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let received = std::sync::Arc::clone(&task_received);
                let connection = tokio::spawn(async move {
                    let mut stream = stream;
                    loop {
                        let mut prefix = [0u8; 5];
                        if stream.read_exact(&mut prefix).await.is_err() {
                            return;
                        }
                        assert_eq!(prefix[0], 0x01, "envelope version");
                        let len = u32::from_be_bytes([prefix[1], prefix[2], prefix[3], prefix[4]])
                            as usize;
                        let mut body = vec![0u8; len];
                        if stream.read_exact(&mut body).await.is_err() {
                            return;
                        }
                        let envelope: serde_json::Value =
                            serde_json::from_slice(&body).expect("envelope is JSON");
                        received.lock().expect("fake records").push(envelope);
                        let ack = br#"{"code":0}"#;
                        let mut framed = (ack.len() as u32).to_be_bytes().to_vec();
                        framed.extend_from_slice(ack);
                        if stream.write_all(&framed).await.is_err() {
                            return;
                        }
                    }
                });
                let mut tracked = task_connections.lock().expect("fake tracks connections");
                tracked.retain(|connection| !connection.is_finished());
                tracked.push(connection);
            }
        });
        Self {
            port,
            received,
            _task: task,
            connections,
        }
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<serde_json::Value> {
        for _ in 0..300 {
            let got = self.received.lock().expect("fake reads").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "rocketmq fake got {} envelopes, wanted {count}",
            self.received.lock().expect("fake reads").len()
        );
    }
}

/// AMQP frame helpers for the MV-03a RabbitMQ fake: method frames
/// carry class and method ids; content frames are captured raw.
#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn mv03a_amqp_read_frame(stream: &mut tokio::net::TcpStream) -> (u8, u16, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    let mut head = [0u8; 7];
    stream
        .read_exact(&mut head)
        .await
        .expect("fake reads the header");
    let size = u32::from_be_bytes([head[3], head[4], head[5], head[6]]) as usize;
    assert!(size < 8 * 1024 * 1024, "amqp frame too large: {size}");
    let mut payload = vec![0u8; size];
    stream
        .read_exact(&mut payload)
        .await
        .expect("fake reads the payload");
    let mut end = [0u8; 1];
    stream
        .read_exact(&mut end)
        .await
        .expect("fake reads the end");
    assert_eq!(end[0], 0xCE, "amqp frame end");
    let channel = u16::from_be_bytes([head[1], head[2]]);
    (head[0], channel, payload)
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn mv03a_amqp_write_frame(
    stream: &mut tokio::net::TcpStream,
    frame_type: u8,
    channel: u16,
    payload: &[u8],
) {
    use tokio::io::AsyncWriteExt;
    let mut frame = vec![frame_type];
    frame.extend_from_slice(&channel.to_be_bytes());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(payload);
    frame.push(0xCE);
    stream
        .write_all(&frame)
        .await
        .expect("fake writes the frame");
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn mv03a_amqp_write_method(
    stream: &mut tokio::net::TcpStream,
    channel: u16,
    class_id: u16,
    method_id: u16,
    args: &[u8],
) {
    let mut payload = class_id.to_be_bytes().to_vec();
    payload.extend_from_slice(&method_id.to_be_bytes());
    payload.extend_from_slice(args);
    mv03a_amqp_write_frame(stream, 1, channel, &payload).await;
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn mv03a_amqp_read_method(stream: &mut tokio::net::TcpStream) -> (u16, (u16, u16), Vec<u8>) {
    let (frame_type, channel, payload) = mv03a_amqp_read_frame(stream).await;
    assert_eq!(frame_type, 1, "fake reads a method frame");
    let class_id = u16::from_be_bytes([payload[0], payload[1]]);
    let method_id = u16::from_be_bytes([payload[2], payload[3]]);
    (channel, (class_id, method_id), payload[4..].to_vec())
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
fn mv03a_amqp_longstr(text: &[u8]) -> Vec<u8> {
    let mut out = (text.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(text);
    out
}

/// One captured RabbitMQ publish triple of the MV-03a fake: the
/// method, content-header and body frames in order.
#[cfg(feature = "rabbitmq")]
#[derive(Clone)]
#[cfg(test)]
struct Mv03aRabbitPublished {
    method: Vec<u8>,
    header: Vec<u8>,
    body: Vec<u8>,
}

/// Minimal AMQP broker for the MV-03a delivery tests: scripted
/// PLAIN handshake, then captures publish triples and answers
/// publisher confirms with Basic.Ack. An armed `nack_all` flag
/// answers every publish with Basic.Nack instead, which the
/// driver reports as a failed forward.
#[cfg(feature = "rabbitmq")]
#[cfg(test)]
struct Mv03aRabbitFake {
    port: u16,
    published: std::sync::Arc<std::sync::Mutex<Vec<Mv03aRabbitPublished>>>,
    nack_all: std::sync::Arc<std::sync::Mutex<bool>>,
    _task: tokio::task::JoinHandle<()>,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
impl Drop for Mv03aRabbitFake {
    #[cfg(test)]
    fn drop(&mut self) {
        // Stop the listener and every accepted connection. A
        // dropped fake must refuse the next publish, as a stopped
        // server does. Finished handles are not kept.
        self._task.abort();
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for connection in connections.drain(..) {
            connection.abort();
        }
    }
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
impl Mv03aRabbitFake {
    #[cfg(test)]
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn start_on(port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn serve(listener: tokio::net::TcpListener) -> Self {
        use tokio::io::AsyncReadExt;
        let port = listener.local_addr().expect("fake addr").port();
        let published = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let nack_all = std::sync::Arc::new(std::sync::Mutex::new(false));
        let connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_published = std::sync::Arc::clone(&published);
        let task_nack = std::sync::Arc::clone(&nack_all);
        let task_connections = std::sync::Arc::clone(&connections);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let published = std::sync::Arc::clone(&task_published);
                let nack_all = std::sync::Arc::clone(&task_nack);
                let connection = tokio::spawn(async move {
                    let mut stream = stream;
                    // Protocol header.
                    let mut header = [0u8; 8];
                    if stream.read_exact(&mut header).await.is_err() {
                        return;
                    }
                    assert_eq!(&header, b"AMQP\x00\x00\x09\x01", "protocol header");
                    // Connection.Start: version, properties, PLAIN.
                    let mut start = vec![0u8, 9];
                    start.extend_from_slice(&0u32.to_be_bytes());
                    start.extend_from_slice(&mv03a_amqp_longstr(b"PLAIN"));
                    start.extend_from_slice(&mv03a_amqp_longstr(b"en_US"));
                    mv03a_amqp_write_method(&mut stream, 0, 10, 10, &start).await;
                    // Start-Ok names PLAIN with credentials.
                    let (_, method, args) = mv03a_amqp_read_method(&mut stream).await;
                    assert_eq!(method, (10, 11), "connection start-ok");
                    assert!(
                        args.windows(5).any(|w| w == b"PLAIN"),
                        "client offers PLAIN"
                    );
                    // Tune bounds the connection. Reason: frame-max 0 means
                    // no limit and the driver allocates 32 frames, which
                    // exhausts memory. RabbitMQ uses 131072.
                    let tune: &[u8] = &[0x07, 0xFF, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00];
                    mv03a_amqp_write_method(&mut stream, 0, 10, 30, tune).await;
                    let _ = mv03a_amqp_read_method(&mut stream).await; // Tune-Ok
                    let (_, method, _) = mv03a_amqp_read_method(&mut stream).await; // Open
                    assert_eq!(method, (10, 40), "connection open");
                    mv03a_amqp_write_method(&mut stream, 0, 10, 41, &[0]).await;
                    // Channel open.
                    let (channel, method, _) = mv03a_amqp_read_method(&mut stream).await;
                    assert_eq!(method, (20, 10), "channel open");
                    mv03a_amqp_write_method(&mut stream, channel, 20, 11, &[0, 0, 0, 0]).await;
                    // Confirm.Select.
                    let (_, method, _) = mv03a_amqp_read_method(&mut stream).await;
                    assert_eq!(method, (85, 10), "confirm select");
                    mv03a_amqp_write_method(&mut stream, channel, 85, 11, &[]).await;
                    // Publish triples: capture, then ack or nack. Each
                    // publish is a method frame, a header frame and a
                    // body frame. The driver awaits the ack.
                    let mut tag: u64 = 0;
                    loop {
                        let (close_channel, method, method_payload) =
                            mv03a_amqp_read_method(&mut stream).await;
                        // An update drops the old client. The old client closes its channel.
                        // Answer the close and stop. Only a publish counts.
                        if method == (20, 40) {
                            mv03a_amqp_write_method(&mut stream, close_channel, 20, 41, &[]).await;
                            break;
                        }
                        if method == (10, 60) {
                            mv03a_amqp_write_method(&mut stream, 0, 10, 61, &[]).await;
                            break;
                        }
                        assert_eq!(method, (60, 40), "basic publish");
                        let (header_type, _, header_payload) =
                            mv03a_amqp_read_frame(&mut stream).await;
                        assert_eq!(header_type, 2, "content header frame");
                        let (body_type, _, body_payload) = mv03a_amqp_read_frame(&mut stream).await;
                        assert_eq!(body_type, 3, "content body frame");
                        tag += 1;
                        published
                            .lock()
                            .expect("fake records")
                            .push(Mv03aRabbitPublished {
                                method: method_payload.clone(),
                                header: header_payload.clone(),
                                body: body_payload.clone(),
                            });
                        let nack = *nack_all.lock().expect("fake reads");
                        // Basic.Ack and Basic.Nack carry the flags
                        // in one octet: multiple and requeue bits.
                        let mut args = tag.to_be_bytes().to_vec();
                        args.push(if nack { 1 } else { 0 });
                        let method_id = if nack { 120u16 } else { 80u16 };
                        mv03a_amqp_write_method(&mut stream, channel, 60, method_id, &args).await;
                    }
                });
                let mut tracked = task_connections.lock().expect("fake tracks connections");
                tracked.retain(|connection| !connection.is_finished());
                tracked.push(connection);
            }
        });
        Self {
            port,
            published,
            nack_all,
            _task: task,
            connections,
        }
    }

    #[cfg(test)]
    fn arm_nacks(&self) {
        *self.nack_all.lock().expect("fake arms") = true;
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<Mv03aRabbitPublished> {
        for _ in 0..300 {
            let got = self.published.lock().expect("fake reads").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "rabbit fake got {} publishes, wanted {count}",
            self.published.lock().expect("fake reads").len()
        );
    }
}

/// MQTT variable-byte integer reader for the bridge fake.
#[cfg(test)]
async fn mv03a_mqtt_read_varint(stream: &mut tokio::net::TcpStream) -> Option<usize> {
    use tokio::io::AsyncReadExt;
    let mut value = 0usize;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await.ok()?;
        value |= ((byte[0] & 0x7F) as usize) << shift;
        if byte[0] & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift > 21 {
            return None;
        }
    }
}

/// One captured MQTT publish of the MV-03a bridge fake.
#[derive(Clone)]
#[cfg(test)]
struct Mv03aMqttPublished {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
}

/// Minimal MQTT broker for the MV-03a bridge tests: answers
/// CONNECT with CONNACK, captures PUBLISH frames and answers QoS
/// 1 with PUBACK. The test asserts the remapped topic, the body,
/// the QoS and the retain flag on the wire.
#[cfg(test)]
struct Mv03aMqttFake {
    port: u16,
    published: std::sync::Arc<std::sync::Mutex<Vec<Mv03aMqttPublished>>>,
    _task: tokio::task::JoinHandle<()>,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[cfg(test)]
impl Drop for Mv03aMqttFake {
    #[cfg(test)]
    fn drop(&mut self) {
        // Stop the listener and every accepted connection. A
        // dropped fake must refuse the next publish, as a stopped
        // server does. Finished handles are not kept.
        self._task.abort();
        let mut connections = self.connections.lock().expect("fake tracks connections");
        for connection in connections.drain(..) {
            connection.abort();
        }
    }
}

#[cfg(test)]
impl Mv03aMqttFake {
    #[cfg(test)]
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn start_on(port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn serve(listener: tokio::net::TcpListener) -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let port = listener.local_addr().expect("fake addr").port();
        let published = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_published = std::sync::Arc::clone(&published);
        let task_connections = std::sync::Arc::clone(&connections);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let published = std::sync::Arc::clone(&task_published);
                let connection = tokio::spawn(async move {
                    let mut stream = stream;
                    // CONNECT: fixed header, length, body.
                    let mut fixed = [0u8; 1];
                    if stream.read_exact(&mut fixed).await.is_err() {
                        return;
                    }
                    assert_eq!(fixed[0] >> 4, 1, "CONNECT packet");
                    let len = mv03a_mqtt_read_varint(&mut stream)
                        .await
                        .expect("connect length");
                    let mut body = vec![0u8; len];
                    if stream.read_exact(&mut body).await.is_err() {
                        return;
                    }
                    // CONNACK: session 0, code 0.
                    if stream.write_all(&[0x20, 0x02, 0x00, 0x00]).await.is_err() {
                        return;
                    }
                    loop {
                        let mut head = [0u8; 1];
                        if stream.read_exact(&mut head).await.is_err() {
                            return;
                        }
                        match head[0] >> 4 {
                            3 => {
                                let qos = (head[0] >> 1) & 0x03;
                                let retain = head[0] & 0x01 == 1;
                                let len = mv03a_mqtt_read_varint(&mut stream)
                                    .await
                                    .expect("publish length");
                                let mut body = vec![0u8; len];
                                if stream.read_exact(&mut body).await.is_err() {
                                    return;
                                }
                                let topic_len = u16::from_be_bytes([body[0], body[1]]) as usize;
                                let topic =
                                    String::from_utf8_lossy(&body[2..2 + topic_len]).to_string();
                                let mut rest = &body[2 + topic_len..];
                                let packet_id = if qos > 0 {
                                    let id = u16::from_be_bytes([rest[0], rest[1]]);
                                    rest = &rest[2..];
                                    id
                                } else {
                                    0
                                };
                                published
                                    .lock()
                                    .expect("fake records")
                                    .push(Mv03aMqttPublished {
                                        topic,
                                        payload: rest.to_vec(),
                                        qos,
                                        retain,
                                    });
                                if qos == 1
                                    && stream
                                        .write_all(&[
                                            0x40,
                                            0x02,
                                            (packet_id >> 8) as u8,
                                            packet_id as u8,
                                        ])
                                        .await
                                        .is_err()
                                {
                                    return;
                                }
                            }
                            12 => {
                                // PINGREQ: answer PINGRESP.
                                let _ = mv03a_mqtt_read_varint(&mut stream).await;
                                if stream.write_all(&[0xD0, 0x00]).await.is_err() {
                                    return;
                                }
                            }
                            14 => {
                                // DISCONNECT: stop reading.
                                return;
                            }
                            other => panic!("fake speaks no mqtt packet {other}"),
                        }
                    }
                });
                let mut tracked = task_connections.lock().expect("fake tracks connections");
                tracked.retain(|connection| !connection.is_finished());
                tracked.push(connection);
            }
        });
        Self {
            port,
            published,
            _task: task,
            connections,
        }
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<Mv03aMqttPublished> {
        for _ in 0..300 {
            let got = self.published.lock().expect("fake reads").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "mqtt fake got {} publishes, wanted {count}",
            self.published.lock().expect("fake reads").len()
        );
    }
}

#[tokio::test]
async fn mv03a_redis_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-redis");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aRedisFake::start().await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let servers = format!("127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "redis", "name": "mv03a-redis", "servers": servers, "password": "pw"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "redis", "name": "mv03a-redis-bad"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a serverless connector refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("servers"),
        "{body}"
    );
    let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "redis", "name": "mv03a-redis-act",
                       "connector": "mv03a-redis",
                       "parameters": {"command_template": ["HSET", "sensors:${topic}", "payload", "${payload}"]}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-redis-rule", "name": "mv03a-redis-rule",
                       "sql": "SELECT * FROM \"mv03a/redis\"", "actions": ["mv03a-redis-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv03a/redis", "{\"v\":1}").await;
    let got = fake.wait_for(3).await;
    let auth = got
        .iter()
        .find(|args| args.first().is_some_and(|name| name == b"AUTH"))
        .expect("the driver authenticates");
    assert_eq!(auth[1], b"pw".to_vec());
    let hset = got
        .iter()
        .find(|args| args.first().is_some_and(|name| name == b"HSET"))
        .expect("the driver sends HSET");
    assert_eq!(hset[1], b"sensors:mv03a/redis".to_vec());
    assert_eq!(hset[2], b"payload".to_vec());
    assert_eq!(hset[3], br#"{"v":1}"#.to_vec());
    mv02b_wait_success(&server, &token, "mv03a-redis-act", 1).await;

    // A value with spaces stays one argument on the wire.
    mv02b_publish(&server, &token, "mv03a/redis", "{\"v\": \"x y\"}").await;
    let got = fake.wait_for(4).await;
    let spaces = got
        .iter()
        .filter(|args| args.first().is_some_and(|name| name == b"HSET"))
        .nth(1)
        .expect("a second HSET");
    assert_eq!(spaces[3], br#"{"v": "x y"}"#.to_vec());
    mv02b_wait_success(&server, &token, "mv03a-redis-act", 2).await;

    let (status, _) = server
            .put_auth(
                "/api/v5/actions/mv03a-redis-act",
                json!({"type": "redis", "name": "mv03a-redis-act",
                       "connector": "mv03a-redis",
                       "parameters": {"command_template": ["HSET", "upd:${topic}", "f", "${payload}"]}}),
                &token,
            )
            .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv03a/redis", "{\"v\":3}").await;
    let got = fake.wait_for(5).await;
    let upd = got
        .iter()
        .find(|args| {
            args.first().is_some_and(|name| name == b"HSET")
                && args.get(1).is_some_and(|key| key == b"upd:mv03a/redis")
        })
        .expect("updated target on the wire");
    assert_eq!(upd[1], b"upd:mv03a/redis".to_vec());
    mv02b_wait_success(&server, &token, "mv03a-redis-act", 3).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03a-redis-act",
            json!({"type": "redis", "name": "mv03a-redis-act",
                       "connector": "mv03a-redis",
                       "parameters": {"command_template": []}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an empty command refuses: {body}");
    mv02b_publish(&server, &token, "mv03a/redis", "{\"v\":4}").await;
    let got = fake.wait_for(6).await;
    let kept = got
        .iter()
        .filter(|args| {
            args.first().is_some_and(|name| name == b"HSET")
                && args.get(1).is_some_and(|key| key == b"upd:mv03a/redis")
        })
        .nth(1)
        .expect("refused update keeps the old target");
    assert_eq!(
        kept[1],
        b"upd:mv03a/redis".to_vec(),
        "the refused update keeps the old target"
    );
    mv02b_wait_success(&server, &token, "mv03a-redis-act", 4).await;

    let down_port = fake.port;
    drop(fake);
    mv02b_publish(&server, &token, "mv03a/redis", "{\"v\":5}").await;
    mv03a_wait_failed(&server, &token, "mv03a-redis-act", 1).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let fake = Mv03aRedisFake::start_on(down_port).await;
    mv02b_publish(&server, &token, "mv03a/redis", "{\"v\":6}").await;
    let got = fake.wait_for(3).await;
    let resumed = got
        .iter()
        .find(|args| {
            args.first().is_some_and(|name| name == b"HSET")
                && args.iter().any(|arg| arg == br#"{"v":6}"#)
        })
        .expect("restart redelivers");
    assert_eq!(resumed[3], br#"{"v":6}"#.to_vec());
    mv02b_wait_success(&server, &token, "mv03a-redis-act", 5).await;

    // A connector update through the production entry keeps delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv03a-redis",
            json!({"type": "redis", "name": "mv03a-redis",
                       "servers": format!("127.0.0.1:{}", fake.port),
                       "password": "pw"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    // A restart keeps the stored connector and delivery resumes.
    let restart_port = fake.port;
    drop(fake);
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    // The restart keeps the changed password. Use it.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["must_change_password"], json!(false));
    let token = body["token"].as_str().expect("login token").to_string();
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03a-redis", &token)
        .await;
    assert_eq!(status, 200, "stored connector survives the restart");
    let fake = Mv03aRedisFake::start_on(restart_port).await;
    mv02b_publish(&server, &token, "mv03a/redis", "{\"v\":7}").await;
    let got = fake.wait_for(1).await;
    let resumed = got
        .iter()
        .find(|args| {
            args.first().is_some_and(|name| name == b"HSET")
                && args.iter().any(|arg| arg == br#"{"v":7}"#)
        })
        .expect("delivery resumes after the restart");
    assert_eq!(resumed[3], br#"{"v":7}"#.to_vec());
    mv02b_wait_success(&server, &token, "mv03a-redis-act", 1).await;
    drop(_state);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03a_rocketmq_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-rocketmq");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aRocketFake::start().await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let servers = format!("127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "rocketmq", "name": "mv03a-rmq", "servers": servers,
                       "access_key": "k", "secret_key": "s"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "rocketmq", "name": "mv03a-rmq-half",
                       "servers": servers, "access_key": "k"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 400,
        "a half ACL pair refuses at build, tested here at create: {body}"
    );
    let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "rocketmq", "name": "mv03a-rmq-act",
                       "connector": "mv03a-rmq",
                       "parameters": {"topic": "events", "tag": "t1", "key": "${client_id}-${message_id}", "strategy": "key_dispatch"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-rmq-rule", "name": "mv03a-rmq-rule",
                       "sql": "SELECT * FROM \"mv03a/rocketmq\"", "actions": ["mv03a-rmq-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(
        &server,
        &token,
        "mv03a/rocketmq",
        "{\"client_id\":\"edge-5\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0]["topic"], json!("events"));
    let messages = got[0]["messages"].as_array().expect("envelope messages");
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0]["system_properties"]["tag"], json!("t1"));
    assert_eq!(
        messages[0]["system_properties"]["keys"],
        json!(["edge-5-0"])
    );
    assert!(
        got[0]["authorization"]
            .as_str()
            .unwrap_or("")
            .starts_with("MQ k:"),
        "the keyed envelope carries a signature"
    );
    mv02b_wait_success(&server, &token, "mv03a-rmq-act", 1).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03a-rmq-act",
            json!({"type": "rocketmq", "name": "mv03a-rmq-act",
                       "connector": "mv03a-rmq",
                       "parameters": {"topic": "events", "tag": "t2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(
        &server,
        &token,
        "mv03a/rocketmq",
        "{\"client_id\":\"edge-5\"}",
    )
    .await;
    let got = fake.wait_for(2).await;
    assert_eq!(
        got[1]["messages"][0]["system_properties"]["tag"],
        json!("t2")
    );
    mv02b_wait_success(&server, &token, "mv03a-rmq-act", 2).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03a-rmq-act",
            json!({"type": "rocketmq", "name": "mv03a-rmq-act",
                       "connector": "mv03a-rmq",
                       "parameters": {"strategy": "bogus"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an unknown strategy refuses: {body}");
    mv02b_publish(
        &server,
        &token,
        "mv03a/rocketmq",
        "{\"client_id\":\"edge-5\"}",
    )
    .await;
    let got = fake.wait_for(3).await;
    assert_eq!(
        got[2]["messages"][0]["system_properties"]["tag"],
        json!("t2"),
        "the refused update keeps the old target"
    );
    mv02b_wait_success(&server, &token, "mv03a-rmq-act", 3).await;

    let down_port = fake.port;
    drop(fake);
    mv02b_publish(
        &server,
        &token,
        "mv03a/rocketmq",
        "{\"client_id\":\"edge-5\"}",
    )
    .await;
    mv03a_wait_failed(&server, &token, "mv03a-rmq-act", 1).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let fake = Mv03aRocketFake::start_on(down_port).await;
    mv02b_publish(
        &server,
        &token,
        "mv03a/rocketmq",
        "{\"client_id\":\"edge-5\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    let bodies: Vec<String> = got
        .iter()
        .flat_map(|envelope| envelope["messages"].as_array().cloned().unwrap_or_default())
        .filter_map(|message| {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(message["body_b64"].as_str().unwrap_or(""))
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
        })
        .collect();
    assert!(
        bodies.iter().any(|body| body.contains("edge-5")),
        "the restart redelivers: {bodies:?}"
    );
    mv02b_wait_success(&server, &token, "mv03a-rmq-act", 4).await;

    // A connector update through the production entry keeps delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv03a-rmq",
            json!({"type": "rocketmq", "name": "mv03a-rmq",
                       "servers": format!("127.0.0.1:{}", fake.port),
                       "access_key": "k", "secret_key": "s"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    // A restart keeps the stored connector and delivery resumes.
    let restart_port = fake.port;
    drop(fake);
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    // The restart keeps the changed password. Use it.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["must_change_password"], json!(false));
    let token = body["token"].as_str().expect("login token").to_string();
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03a-rmq", &token)
        .await;
    assert_eq!(status, 200, "stored connector survives the restart");
    let fake = Mv03aRocketFake::start_on(restart_port).await;
    mv02b_publish(
        &server,
        &token,
        "mv03a/rocketmq",
        "{\"client_id\":\"edge-5\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0]["topic"], json!("events"));
    mv02b_wait_success(&server, &token, "mv03a-rmq-act", 1).await;
    drop(_state);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
async fn mv03a_rabbitmq_delivers_confirmed_publishes_and_counts_a_nack() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-rabbitmq");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aRabbitFake::start().await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let servers = format!("127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "rabbitmq", "name": "mv03a-rb",
                       "servers": servers, "username": "u", "password": "p"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "rabbitmq", "name": "mv03a-rb-bad", "servers": servers}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a passwordless connector refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("password"),
        "{body}"
    );
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "rabbitmq", "name": "mv03a-rb-act",
                       "connector": "mv03a-rb",
                       "parameters": {"exchange": "events",
                                      "routing_key": "sensor.${topic}",
                                      "delivery_mode": "persistent",
                                      "headers_template": [{"key": "src", "value": "${topic}"}],
                                      "payload_template": "${topic}=${payload}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-rb-rule", "name": "mv03a-rb-rule",
                       "sql": "SELECT * FROM \"mv03a/rabbit\"", "actions": ["mv03a-rb-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv03a/rabbit", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].method.windows(6).any(|w| w == b"events"),
        "exchange on the wire"
    );
    assert!(
        got[0]
            .method
            .windows(19)
            .any(|w| w == b"sensor.mv03a.rabbit"),
        "dotted routing key on the wire"
    );
    assert!(
        got[0].header.windows(3).any(|w| w == b"src"),
        "header key on the wire"
    );
    assert!(
        got[0].header.windows(12).any(|w| w == b"mv03a/rabbit"),
        "header value on the wire"
    );
    assert!(
        got[0].header.windows(8).any(|w| w == b"mqtt.qos"),
        "broker headers ride along"
    );
    assert_eq!(
        got[0].body,
        b"mv03a/rabbit={\"v\":1}".to_vec(),
        "rendered body on the wire"
    );
    mv02b_wait_success(&server, &token, "mv03a-rb-act", 1).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03a-rb-act",
            json!({"type": "rabbitmq", "name": "mv03a-rb-act",
                       "connector": "mv03a-rb",
                       "parameters": {"exchange": "events2",
                                      "routing_key": "sensor.${topic}",
                                      "delivery_mode": "persistent",
                                      "headers_template": [{"key": "src", "value": "${topic}"}],
                                      "payload_template": "${topic}=${payload}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv03a/rabbit", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got[1].method.windows(7).any(|w| w == b"events2"),
        "updated exchange on the wire"
    );
    mv02b_wait_success(&server, &token, "mv03a-rb-act", 2).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03a-rb-act",
            json!({"type": "rabbitmq", "name": "mv03a-rb-act",
                       "connector": "mv03a-rb",
                       "parameters": {"exchange": "events2",
                                      "routing_key": "sensor.${topic}",
                                      "delivery_mode": "bogus"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an unknown delivery mode refuses: {body}");
    mv02b_publish(&server, &token, "mv03a/rabbit", "{\"v\":3}").await;
    let got = fake.wait_for(3).await;
    assert!(
        got[2].method.windows(7).any(|w| w == b"events2"),
        "the refused update keeps the old target"
    );
    mv02b_wait_success(&server, &token, "mv03a-rb-act", 3).await;

    let down_port = fake.port;
    drop(fake);
    mv02b_publish(&server, &token, "mv03a/rabbit", "{\"v\":4}").await;
    mv03a_wait_failed(&server, &token, "mv03a-rb-act", 1).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let fake = Mv03aRabbitFake::start_on(down_port).await;
    mv02b_publish(&server, &token, "mv03a/rabbit", "{\"v\":5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].body, b"mv03a/rabbit={\"v\":5}".to_vec());
    mv02b_wait_success(&server, &token, "mv03a-rb-act", 4).await;

    // A broker nack counts one failed forward: the confirm logic
    // reports it instead of reading success it never earned.
    fake.arm_nacks();
    mv02b_publish(&server, &token, "mv03a/rabbit", "{\"v\":6}").await;
    mv03a_wait_failed(&server, &token, "mv03a-rb-act", 2).await;

    // A connector update through the production entry keeps delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv03a-rb",
            json!({"type": "rabbitmq", "name": "mv03a-rb",
                       "servers": format!("127.0.0.1:{}", fake.port),
                       "username": "u", "password": "p"}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    // A broker restart keeps the stored connector and delivery resumes.
    let restart_port = fake.port;
    drop(fake);
    drop(_state);
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    // The restart keeps the changed password. Use it.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["must_change_password"], json!(false));
    let token = body["token"].as_str().expect("login token").to_string();
    let (status, _) = server.get_auth("/api/v5/connectors/mv03a-rb", &token).await;
    assert_eq!(status, 200, "stored connector survives the restart");
    let fake = Mv03aRabbitFake::start_on(restart_port).await;
    mv02b_publish(&server, &token, "mv03a/rabbit", "{\"v\":7}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].body, b"mv03a/rabbit={\"v\":7}".to_vec());
    mv02b_wait_success(&server, &token, "mv03a-rb-act", 1).await;
    drop(_state);
    drop(server);
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03a_mqtt_bridge_forwards_without_looping_back() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-mqtt");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aMqttFake::start().await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let broker = format!("127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "mqtt", "name": "mv03a-mq", "server": broker}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "mqtt", "name": "mv03a-mq-act",
                       "connector": "mv03a-mq",
                       "parameters": {"topic": "remote/${topic}",
                                      "payload": "${topic}=${payload}",
                                      "qos": 1, "retain": true}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-mq-rule", "name": "mv03a-mq-rule",
                       "sql": "SELECT * FROM \"mv03a/bridge\"", "actions": ["mv03a-mq-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv03a/bridge", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].topic, "remote/mv03a/bridge");
    assert_eq!(got[0].payload, b"mv03a/bridge={\"v\":1}".to_vec());
    assert_eq!(got[0].qos, 1);
    assert!(got[0].retain, "retain rides the wire");
    mv02b_wait_success(&server, &token, "mv03a-mq-act", 1).await;

    // The remap keeps the bridged topic out of the local rule
    // filter. A bridged topic never matches `mv03a/bridge`, so it
    // is not bridged back. Legitimate traffic still flows.
    mv02b_publish(&server, &token, "mv03a/bridge", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert_eq!(got[1].payload, b"mv03a/bridge={\"v\":2}".to_vec());
    mv02b_wait_success(&server, &token, "mv03a-mq-act", 2).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03a-mq-act",
            json!({"type": "mqtt", "name": "mv03a-mq-act",
                       "connector": "mv03a-mq",
                       "parameters": {"topic": "remote2/${topic}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv03a/bridge", "{\"v\":3}").await;
    let got = fake.wait_for(3).await;
    assert_eq!(got[2].topic, "remote2/mv03a/bridge");
    assert_eq!(
        got[2].payload,
        br#"{"v":3}"#.to_vec(),
        "absent template forwards raw"
    );
    mv02b_wait_success(&server, &token, "mv03a-mq-act", 3).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03a-mq-act",
            json!({"type": "mqtt", "name": "mv03a-mq-act",
                       "connector": "mv03a-mq",
                       "parameters": {"topic": "remote2/${topic}", "qos": 5}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an unknown qos refuses: {body}");
    mv02b_publish(&server, &token, "mv03a/bridge", "{\"v\":4}").await;
    let got = fake.wait_for(4).await;
    assert_eq!(got[3].topic, "remote2/mv03a/bridge");
    mv02b_wait_success(&server, &token, "mv03a-mq-act", 4).await;

    let down_port = fake.port;
    drop(fake);
    mv02b_publish(&server, &token, "mv03a/bridge", "{\"v\":5}").await;
    mv03a_wait_failed(&server, &token, "mv03a-mq-act", 1).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let fake = Mv03aMqttFake::start_on(down_port).await;
    mv02b_publish(&server, &token, "mv03a/bridge", "{\"v\":6}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got.iter()
            .any(|message| message.payload == br#"{"v":6}"#.to_vec()),
        "the restart redelivers: {:?}",
        got.iter()
            .map(|message| &message.payload)
            .collect::<Vec<_>>()
    );
    mv02b_wait_success(&server, &token, "mv03a-mq-act", 5).await;

    // A connector update through the production entry keeps delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv03a-mq",
            json!({"type": "mqtt", "name": "mv03a-mq",
                       "server": format!("127.0.0.1:{}", fake.port)}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    // A broker restart keeps the stored connector and delivery resumes.
    let restart_port = fake.port;
    drop(fake);
    drop(server);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    // The restart keeps the changed password. Use it.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["must_change_password"], json!(false));
    let token = body["token"].as_str().expect("login token").to_string();
    let (status, _) = server.get_auth("/api/v5/connectors/mv03a-mq", &token).await;
    assert_eq!(status, 200, "stored connector survives the restart");
    let fake = Mv03aMqttFake::start_on(restart_port).await;
    mv02b_publish(&server, &token, "mv03a/bridge", "{\"v\":7}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].topic, "remote2/mv03a/bridge");
    mv02b_wait_success(&server, &token, "mv03a-mq-act", 1).await;
    drop(_state);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

/// The bridge keeps the tenant: a rule outside the default tenant
/// cannot forward to the bridge action, and the default rule
/// still delivers through the broker to the remote.
#[tokio::test]
async fn mv03a_mqtt_bridge_tenant_gate_keeps_connector_forward_in_default_tenant() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-mqtt-tenant");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aMqttFake::start().await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let broker = format!("127.0.0.1:{}", fake.port);

    // A second namespace owns the refused tenant below.
    let (status, _) = server
        .post_auth("/api/v5/mt/ns/mv03a-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "mqtt", "name": "mv03a-ten-mq", "server": broker}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "mqtt", "name": "mv03a-ten-mq-act",
                       "connector": "mv03a-ten-mq",
                       "parameters": {"topic": "remote/${topic}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    // A rule outside the default tenant must not forward to the
    // bridge action: the path carries no tenant label.
    let (status, body) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-ten-mq-rule", "name": "mv03a-ten-mq-rule",
                       "sql": "SELECT * FROM \"mv03a/ten\"", "tenant": "mv03a-tenant",
                       "actions": ["mv03a-ten-mq-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "tenant forward refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("tenant"),
        "refusal names tenant: {body}"
    );
    // Nothing is stored for the refused rule.
    let (status, _) = server
        .get_auth("/api/v5/rules/mv03a-ten-mq-rule", &token)
        .await;
    assert_eq!(status, 404, "refused rule stores nothing");

    // The default tenant keeps working: the rule creates, the
    // publish delivers through the broker to the fake.
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-ten-mq-rule", "name": "mv03a-ten-mq-rule",
                       "sql": "SELECT * FROM \"mv03a/ten\"",
                       "actions": ["mv03a-ten-mq-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv03a/ten", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].topic, "remote/mv03a/ten");
    assert_eq!(got[0].payload, br#"{"v":1}"#.to_vec());
    mv02b_wait_success(&server, &token, "mv03a-ten-mq-act", 1).await;

    // A move outside the default tenant refuses and keeps the
    // old rule: the next publish still reaches the remote.
    let (status, body) = server
        .put_auth(
            "/api/v5/rules/mv03a-ten-mq-rule",
            json!({"id": "mv03a-ten-mq-rule", "name": "mv03a-ten-mq-rule",
                       "sql": "SELECT * FROM \"mv03a/ten\"", "tenant": "mv03a-tenant",
                       "actions": ["mv03a-ten-mq-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "move outside default refuses: {body}");
    let (status, fetched) = server
        .get_auth("/api/v5/rules/mv03a-ten-mq-rule", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(fetched["tenant"], json!("default"));
    mv02b_publish(&server, &token, "mv03a/ten", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert_eq!(got[1].topic, "remote/mv03a/ten");
    assert_eq!(got[1].payload, br#"{"v":2}"#.to_vec());
    mv02b_wait_success(&server, &token, "mv03a-ten-mq-act", 2).await;
    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "rabbitmq")]
#[derive(Clone)]
#[cfg(test)]
struct Dr01RabbitPublished {
    exchange: String,
    routing_key: String,
    body: Vec<u8>,
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
struct Dr01RabbitFake {
    port: u16,
    published: Arc<std::sync::Mutex<Vec<Dr01RabbitPublished>>>,
    _task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
impl Drop for Dr01RabbitFake {
    #[cfg(test)]
    fn drop(&mut self) {
        self._task.abort();
    }
}

#[cfg(feature = "rabbitmq")]
/// Largest fake frame in bytes. Reason: blocks a huge allocation.
#[cfg(test)]
const DR01_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
#[cfg(feature = "rabbitmq")]
/// Fake poll rounds. Reason: waits 30 seconds for delivery.
#[cfg(test)]
const DR01_WAIT_ROUNDS: usize = 300;
#[cfg(feature = "rabbitmq")]
/// Fake poll step in millis. Reason: polls without a busy wait.
#[cfg(test)]
const DR01_WAIT_STEP_MS: u64 = 100;
#[cfg(feature = "rabbitmq")]
/// First publisher delivery tag. Reason: the driver awaits one
/// ack for the single publish of the test.
#[cfg(test)]
const DR01_FIRST_DELIVERY_TAG: u64 = 1;

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn rabbit_fake_send_method(
    stream: &mut tokio::net::TcpStream,
    channel: u16,
    class_id: u16,
    method_id: u16,
    args: &[u8],
) {
    let mut payload = class_id.to_be_bytes().to_vec();
    payload.extend_from_slice(&method_id.to_be_bytes());
    payload.extend_from_slice(args);
    let frame = broker_connectors::rabbitmq::AmqpFrame {
        frame_type: broker_connectors::rabbitmq::FRAME_METHOD,
        channel,
        payload,
    };
    stream
        .write_all(&frame.encode())
        .await
        .expect("fake writes a frame");
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn rabbit_fake_read_frame(stream: &mut tokio::net::TcpStream) -> Option<(u8, u16, Vec<u8>)> {
    let mut head = [0u8; 7];
    if stream.read_exact(&mut head).await.is_err() {
        return None;
    }
    let size = u32::from_be_bytes([head[3], head[4], head[5], head[6]]) as usize;
    if size > DR01_MAX_FRAME_BYTES {
        return None;
    }
    let mut payload = vec![0u8; size];
    if stream.read_exact(&mut payload).await.is_err() {
        return None;
    }
    let mut end = [0u8; 1];
    if stream.read_exact(&mut end).await.is_err() {
        return None;
    }
    if end[0] != broker_connectors::rabbitmq::FRAME_END {
        return None;
    }
    let channel = u16::from_be_bytes([head[1], head[2]]);
    Some((head[0], channel, payload))
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn rabbit_fake_read_method(
    stream: &mut tokio::net::TcpStream,
) -> Option<(u16, (u16, u16), Vec<u8>)> {
    let (frame_type, channel, payload) = rabbit_fake_read_frame(stream).await?;
    if frame_type != broker_connectors::rabbitmq::FRAME_METHOD || payload.len() < 4 {
        return None;
    }
    let class_id = u16::from_be_bytes([payload[0], payload[1]]);
    let method_id = u16::from_be_bytes([payload[2], payload[3]]);
    Some((channel, (class_id, method_id), payload[4..].to_vec()))
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
fn rabbit_fake_longstr(text: &[u8]) -> Vec<u8> {
    let mut out = (text.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(text);
    out
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
fn rabbit_fake_parse_publish(args: &[u8]) -> Option<(String, String)> {
    if args.len() < 4 {
        return None;
    }
    let mut pos = 2;
    let exchange_len = *args.get(pos)? as usize;
    pos += 1;
    let exchange = std::str::from_utf8(args.get(pos..pos + exchange_len)?)
        .ok()?
        .to_string();
    pos += exchange_len;
    let key_len = *args.get(pos)? as usize;
    pos += 1;
    let routing_key = std::str::from_utf8(args.get(pos..pos + key_len)?)
        .ok()?
        .to_string();
    Some((exchange, routing_key))
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn dr01_handshake(stream: &mut tokio::net::TcpStream) -> Option<()> {
    let mut header = [0u8; 8];
    if stream.read_exact(&mut header).await.is_err() {
        return None;
    }
    if &header != b"AMQP\x00\x00\x09\x01" {
        return None;
    }
    let mut start = vec![0u8, 9];
    start.extend_from_slice(&0u32.to_be_bytes());
    start.extend_from_slice(&rabbit_fake_longstr(b"PLAIN"));
    start.extend_from_slice(&rabbit_fake_longstr(b"en_US"));
    rabbit_fake_send_method(stream, 0, 10, 10, &start).await;
    let (_, method, args) = rabbit_fake_read_method(stream).await?;
    if method != (10, 11) {
        return None;
    }
    if !args.windows(5).any(|w| w == b"PLAIN") {
        return None;
    }
    // Tune bounds the connection. Reason: frame-max 0 means no limit and the driver allocates 32 frames, which exhausts memory. RabbitMQ uses 131072.
    let tune: &[u8] = &[0x07, 0xFF, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00];
    rabbit_fake_send_method(stream, 0, 10, 30, tune).await;
    let _ = rabbit_fake_read_method(stream).await?;
    let (_, method, _) = rabbit_fake_read_method(stream).await?;
    if method != (10, 40) {
        return None;
    }
    // Open-Ok carries a short string. Reason: the driver parses one short string.
    rabbit_fake_send_method(stream, 0, 10, 41, &[0x00]).await;
    let (channel, method, _) = rabbit_fake_read_method(stream).await?;
    if method != (20, 10) {
        return None;
    }
    // Channel Open-Ok carries a long string. Reason: the driver parses one long string.
    rabbit_fake_send_method(stream, channel, 20, 11, &[0x00, 0x00, 0x00, 0x00]).await;
    Some(())
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
async fn dr01_read_publish(stream: &mut tokio::net::TcpStream) -> Option<Dr01RabbitPublished> {
    // Publish triple: capture the method, header and
    // body frames. This matches the loopback fake pattern.
    // One triple proves delivery with the exact routing key.
    // Confirm mode: answer Confirm.Select and Basic.Ack.
    // Reason: the driver enables confirms and awaits the ack.
    let (mut channel, mut method, mut args) = rabbit_fake_read_method(stream).await?;
    if method == (85, 10) {
        rabbit_fake_send_method(stream, channel, 85, 11, &[]).await;
        (channel, method, args) = rabbit_fake_read_method(stream).await?;
    }
    if method != (60, 40) {
        return None;
    }
    let (exchange, routing_key) = rabbit_fake_parse_publish(&args)?;
    let (header_type, _, _) = rabbit_fake_read_frame(stream).await?;
    if header_type != 2 {
        return None;
    }
    let (body_type, _, body) = rabbit_fake_read_frame(stream).await?;
    if body_type != 3 {
        return None;
    }
    let mut ack = DR01_FIRST_DELIVERY_TAG.to_be_bytes().to_vec();
    ack.push(0);
    rabbit_fake_send_method(stream, channel, 60, 80, &ack).await;
    Some(Dr01RabbitPublished {
        exchange,
        routing_key,
        body,
    })
}

#[cfg(feature = "rabbitmq")]
#[cfg(test)]
impl Dr01RabbitFake {
    #[cfg(test)]
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn serve(listener: tokio::net::TcpListener) -> Self {
        let port = listener.local_addr().expect("fake addr").port();
        let published = Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_published = Arc::clone(&published);
        // Single accept and single publish. Reason: the test
        // publishes once, so one handshake and one triple prove
        // delivery. A single task keeps the future small.
        let task = tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            if dr01_handshake(&mut stream).await.is_none() {
                return;
            }
            let Some(item) = dr01_read_publish(&mut stream).await else {
                return;
            };
            task_published.lock().expect("fake records").push(item);
        });
        Self {
            port,
            published,
            _task: task,
        }
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<Dr01RabbitPublished> {
        for _ in 0..DR01_WAIT_ROUNDS {
            let got = self.published.lock().expect("fake reads").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(DR01_WAIT_STEP_MS)).await;
        }
        panic!(
            "rabbit fake got {} publishes, wanted {count}",
            self.published.lock().expect("fake reads").len()
        );
    }
}

#[cfg(feature = "rabbitmq")]
#[tokio::test]
async fn dr01_rabbitmq_default_exchange_delivers_through_the_broker() {
    // Empty exchange selects the default exchange.
    // See #components/schemas/bridge_rabbitmq.action_parameters.
    // The v1 entries accept rabbitmq while the kind is pending,
    // so the test delivers through the broker now.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("dr01-rabbit");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Dr01RabbitFake::start().await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("amqp://u:p@127.0.0.1:{}/%2f", fake.port);
    let (status, _) = server
        .post_auth(
            "/api/v1/connectors",
            json!({"id": "dr01-rb", "kind": "rabbitmq",
                       "config": {"endpoint": endpoint, "exchange": "",
                                  "routing_key_template": "dr01queue.${topic}",
                                  "delivery_mode": 2}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v1/rules",
            json!({"name": "dr01-rb-rule", "topic_filter": "dr01/rabbit",
                       "actions": [{"type": "forwardconnector",
                                    "connector_id": "dr01-rb"}]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "dr01/rabbit", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert!(got[0].exchange.is_empty(), "default exchange is empty");
    assert_eq!(got[0].routing_key, "dr01queue.dr01.rabbit");
    assert_eq!(got[0].body, b"{\"v\":1}", "payload reaches the server");
    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}
