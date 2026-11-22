use super::*;

/// Protobuf helpers for the MV-03a Pulsar fake: varint fields and
/// length-delimited fields with real PulsarApi field numbers.
#[cfg(test)]
fn mv03a_pb_varint(field: u32, value: u64, out: &mut Vec<u8>) {
    let mut tag = u64::from(field) << 3;
    loop {
        let mut byte = (tag & 0x7F) as u8;
        tag >>= 7;
        if tag != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if tag == 0 {
            break;
        }
    }
    let mut value = value;
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

#[cfg(test)]
fn mv03a_pb_string(field: u32, value: &str, out: &mut Vec<u8>) {
    let mut tag = (u64::from(field) << 3) | 2;
    loop {
        let mut byte = (tag & 0x7F) as u8;
        tag >>= 7;
        if tag != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if tag == 0 {
            break;
        }
    }
    let mut len = value.len() as u64;
    loop {
        let mut byte = (len & 0x7F) as u8;
        len >>= 7;
        if len != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out.extend_from_slice(value.as_bytes());
}

#[cfg(test)]
fn mv03a_pb_sub(field: u32, body: &[u8], out: &mut Vec<u8>) {
    mv03a_pb_string(field, "", out);
    out.truncate(out.len() - 1);
    let mut len = body.len() as u64;
    loop {
        let mut byte = (len & 0x7F) as u8;
        len >>= 7;
        if len != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
    out.extend_from_slice(body);
}

/// Scan one protobuf command for a varint field value.
#[cfg(test)]
fn mv03a_pb_find_varint(command: &[u8], want: u32) -> Option<u64> {
    let mut pos = 0;
    while pos < command.len() {
        let (tag, used) = mv03a_pb_read_varint(&command[pos..])?;
        pos += used;
        let field = (tag >> 3) as u32;
        let wire = (tag & 0x07) as u32;
        match wire {
            0 => {
                let (value, used) = mv03a_pb_read_varint(&command[pos..])?;
                if field == want {
                    return Some(value);
                }
                pos += used;
            }
            2 => {
                let (len, used) = mv03a_pb_read_varint(&command[pos..])?;
                pos += used;
                pos += len as usize;
            }
            1 => pos += 8,
            5 => pos += 4,
            _ => return None,
        }
    }
    None
}

#[cfg(test)]
fn mv03a_pb_read_varint(buf: &[u8]) -> Option<(u64, usize)> {
    let mut value = 0u64;
    let mut shift = 0u32;
    for (index, byte) in buf.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return Some((value, index + 1));
        }
        shift += 7;
        if shift > 63 {
            return None;
        }
    }
    None
}

/// Scan one protobuf command for a length-delimited field.
#[cfg(test)]
fn mv03a_pb_find_bytes(command: &[u8], want: u32) -> Option<&[u8]> {
    let mut pos = 0;
    while pos < command.len() {
        let (tag, used) = mv03a_pb_read_varint(&command[pos..])?;
        pos += used;
        let field = (tag >> 3) as u32;
        let wire = (tag & 0x07) as u32;
        match wire {
            0 => {
                let (_, used) = mv03a_pb_read_varint(&command[pos..])?;
                pos += used;
            }
            2 => {
                let (len, used) = mv03a_pb_read_varint(&command[pos..])?;
                pos += used;
                if field == want {
                    return Some(&command[pos..pos + len as usize]);
                }
                pos += len as usize;
            }
            1 => pos += 8,
            5 => pos += 4,
            _ => return None,
        }
    }
    None
}

/// Wrap one command in a Pulsar frame: total size, command size,
/// command bytes.
#[cfg(test)]
fn mv03a_pulsar_frame(command: &[u8]) -> Vec<u8> {
    let mut frame = ((command.len() + 4) as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&(command.len() as u32).to_be_bytes());
    frame.extend_from_slice(command);
    frame
}

/// Read one Pulsar frame: total size, command size, command bytes,
/// plus the optional message payload after the magic.
#[cfg(test)]
async fn mv03a_pulsar_read_frame(
    stream: &mut tokio::net::TcpStream,
) -> Option<(Vec<u8>, Vec<u8>, Vec<u8>)> {
    use tokio::io::AsyncReadExt;
    let mut total = [0u8; 4];
    // A closed client socket ends the fake connection, as a real
    // broker ends it: stop the task instead of panicking.
    stream.read_exact(&mut total).await.ok()?;
    let total = u32::from_be_bytes(total) as usize;
    if total < 4 {
        return None;
    }
    let mut command_size = [0u8; 4];
    stream.read_exact(&mut command_size).await.ok()?;
    let command_size = u32::from_be_bytes(command_size) as usize;
    if command_size > total - 4 {
        return None;
    }
    let mut command = vec![0u8; command_size];
    stream.read_exact(&mut command).await.ok()?;
    let mut rest = vec![0u8; total - 4 - command_size];
    if !rest.is_empty() {
        stream.read_exact(&mut rest).await.ok()?;
    }
    // Split metadata and payload after the 0x0E01 magic.
    if rest.len() > 10 && rest[0] == 0x0E && rest[1] == 0x01 {
        let checksum_len = 4;
        let meta_len = u32::from_be_bytes([
            rest[2 + checksum_len],
            rest[3 + checksum_len],
            rest[4 + checksum_len],
            rest[5 + checksum_len],
        ]) as usize;
        let meta_start = 2 + checksum_len + 4;
        if meta_start + meta_len > rest.len() {
            return None;
        }
        let metadata = rest[meta_start..meta_start + meta_len].to_vec();
        let payload = rest[meta_start + meta_len..].to_vec();
        Some((command, metadata, payload))
    } else {
        Some((command, Vec::new(), rest))
    }
}

/// One captured Pulsar message of the MV-03a Pulsar fake.
#[derive(Clone)]
#[cfg(test)]
struct Mv03aPulsarMessage {
    topic_path: String,
    payload: Vec<u8>,
    partition_key: Option<String>,
    properties: Vec<(String, String)>,
}

/// Minimal Pulsar broker for the MV-03a delivery tests: answers
/// Connect, PartitionedMetadata (zero partitions), Lookup
/// (Connect to self), Producer and Send with canned receipts.
/// The test asserts the topic path, the payload, the partition
/// key and the properties on the wire.
#[cfg(test)]
struct Mv03aPulsarFake {
    port: u16,
    received: std::sync::Arc<std::sync::Mutex<Vec<Mv03aPulsarMessage>>>,
    error_sends: std::sync::Arc<std::sync::Mutex<bool>>,
    _task: tokio::task::JoinHandle<()>,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[cfg(test)]
impl Drop for Mv03aPulsarFake {
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
impl Mv03aPulsarFake {
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
        use tokio::io::AsyncWriteExt;
        let port = listener.local_addr().expect("fake addr").port();
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let error_sends = std::sync::Arc::new(std::sync::Mutex::new(false));
        let connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_connections = std::sync::Arc::clone(&connections);
        let task_received = std::sync::Arc::clone(&received);
        let task_errors = std::sync::Arc::clone(&error_sends);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let received = std::sync::Arc::clone(&task_received);
                let error_sends = std::sync::Arc::clone(&task_errors);
                let connection = tokio::spawn(async move {
                    let mut stream = stream;
                    let mut last_topic = String::new();
                    // BaseCommand helpers, real PulsarApi numbers:
                    // type field 1; Connect 2; Connected 3; Producer
                    // 5; Send 6; SendReceipt 7; ProducerSuccess 17;
                    // Ping 18; Pong 19; PartitionedMetadata 21 and
                    // its response 22; Lookup 23 and its response 24.
                    let command = |message_type: u64, body: &[u8]| {
                        let mut command = Vec::new();
                        mv03a_pb_varint(1, message_type, &mut command);
                        command.extend_from_slice(body);
                        command
                    };
                    loop {
                        let Some((frame, metadata, payload)) =
                            mv03a_pulsar_read_frame(&mut stream).await
                        else {
                            break;
                        };
                        let message_type =
                            mv03a_pb_find_varint(&frame, 1).expect("command carries a type");
                        match message_type as u32 {
                            2 => {
                                // Connect: Connected with a version.
                                let mut body = Vec::new();
                                mv03a_pb_string(1, "pulsar-fake-3.0", &mut body);
                                mv03a_pb_varint(2, 12, &mut body);
                                let frame = command(3, &{
                                    let mut connected = Vec::new();
                                    mv03a_pb_sub(3, &body, &mut connected);
                                    connected
                                });
                                stream
                                    .write_all(&mv03a_pulsar_frame(&frame))
                                    .await
                                    .expect("fake writes connected");
                            }
                            21 => {
                                // PartitionedMetadata: zero
                                // partitions for a plain topic.
                                let inner = mv03a_pb_find_bytes(&frame, 21)
                                    .expect("partitioned metadata body");
                                let request_id =
                                    mv03a_pb_find_varint(inner, 2).expect("request id");
                                let mut body = Vec::new();
                                mv03a_pb_varint(1, 0, &mut body);
                                mv03a_pb_varint(2, request_id, &mut body);
                                mv03a_pb_varint(3, 0, &mut body);
                                let mut response = Vec::new();
                                mv03a_pb_sub(22, &body, &mut response);
                                let frame = command(22, &response);
                                stream
                                    .write_all(&mv03a_pulsar_frame(&frame))
                                    .await
                                    .expect("fake writes partitioned metadata");
                            }
                            23 => {
                                // Lookup: Connect to this fake.
                                let inner = mv03a_pb_find_bytes(&frame, 23).expect("lookup body");
                                let request_id =
                                    mv03a_pb_find_varint(inner, 2).expect("request id");
                                let mut body = Vec::new();
                                mv03a_pb_string(
                                    1,
                                    &format!("pulsar://127.0.0.1:{port}"),
                                    &mut body,
                                );
                                mv03a_pb_varint(3, 1, &mut body);
                                mv03a_pb_varint(4, request_id, &mut body);
                                mv03a_pb_varint(5, 1, &mut body);
                                let mut response = Vec::new();
                                mv03a_pb_sub(24, &body, &mut response);
                                let frame = command(24, &response);
                                stream
                                    .write_all(&mv03a_pulsar_frame(&frame))
                                    .await
                                    .expect("fake writes lookup");
                            }
                            5 => {
                                // Producer: success with the name.
                                let inner = mv03a_pb_find_bytes(&frame, 5).expect("producer body");
                                let request_id =
                                    mv03a_pb_find_varint(inner, 3).expect("request id");
                                if let Some(topic) = mv03a_pb_find_bytes(inner, 1) {
                                    last_topic = String::from_utf8_lossy(topic).to_string();
                                }
                                let mut body = Vec::new();
                                mv03a_pb_varint(1, request_id, &mut body);
                                mv03a_pb_string(2, "pulsar-fake-producer", &mut body);
                                let mut response = Vec::new();
                                mv03a_pb_sub(17, &body, &mut response);
                                let frame = command(17, &response);
                                stream
                                    .write_all(&mv03a_pulsar_frame(&frame))
                                    .await
                                    .expect("fake writes producer success");
                            }
                            6 => {
                                // Send: capture the message, answer
                                // the receipt - or an armed
                                // authorization error.
                                let inner = mv03a_pb_find_bytes(&frame, 6).expect("send body");
                                let producer_id =
                                    mv03a_pb_find_varint(inner, 1).expect("producer id");
                                let sequence_id =
                                    mv03a_pb_find_varint(inner, 2).expect("sequence id");
                                if *error_sends.lock().expect("fake reads") {
                                    let mut body = Vec::new();
                                    mv03a_pb_varint(1, producer_id, &mut body);
                                    mv03a_pb_varint(2, sequence_id, &mut body);
                                    mv03a_pb_varint(3, 4, &mut body);
                                    let mut response = Vec::new();
                                    mv03a_pb_sub(8, &body, &mut response);
                                    let frame = command(8, &response);
                                    stream
                                        .write_all(&mv03a_pulsar_frame(&frame))
                                        .await
                                        .expect("fake writes send error");
                                    continue;
                                }
                                let mut properties = Vec::new();
                                let mut cursor = 0;
                                while let Some((tag, used)) =
                                    mv03a_pb_read_varint(&metadata[cursor..])
                                {
                                    cursor += used;
                                    let field = (tag >> 3) as u32;
                                    let wire = (tag & 0x07) as u32;
                                    if wire != 2 {
                                        if wire == 0 {
                                            let (_, used) =
                                                mv03a_pb_read_varint(&metadata[cursor..])
                                                    .expect("metadata varint");
                                            cursor += used;
                                        } else {
                                            break;
                                        }
                                        continue;
                                    }
                                    let (len, used) = mv03a_pb_read_varint(&metadata[cursor..])
                                        .expect("metadata length");
                                    cursor += used;
                                    let bytes = &metadata[cursor..cursor + len as usize];
                                    cursor += len as usize;
                                    if field == 4 {
                                        // properties: repeated KeyValue.
                                        let key = mv03a_pb_find_bytes(bytes, 1)
                                            .map(|b| String::from_utf8_lossy(b).to_string());
                                        let value = mv03a_pb_find_bytes(bytes, 2)
                                            .map(|b| String::from_utf8_lossy(b).to_string());
                                        if let (Some(key), Some(value)) = (key, value) {
                                            properties.push((key, value));
                                        }
                                    }
                                }
                                let partition_key = mv03a_pb_find_bytes(&metadata, 6)
                                    .map(|b| String::from_utf8_lossy(b).to_string());
                                // The topic path stays in the fake:
                                // the driver opens one producer per
                                // path before it sends.
                                received
                                    .lock()
                                    .expect("fake records")
                                    .push(Mv03aPulsarMessage {
                                        topic_path: last_topic.clone(),
                                        payload,
                                        partition_key,
                                        properties,
                                    });
                                let mut body = Vec::new();
                                mv03a_pb_varint(1, producer_id, &mut body);
                                mv03a_pb_varint(2, sequence_id, &mut body);
                                let mut response = Vec::new();
                                mv03a_pb_sub(7, &body, &mut response);
                                let frame = command(7, &response);
                                stream
                                    .write_all(&mv03a_pulsar_frame(&frame))
                                    .await
                                    .expect("fake writes receipt");
                            }
                            18 => {
                                // Ping: Pong.
                                let frame = command(19, &[]);
                                stream
                                    .write_all(&mv03a_pulsar_frame(&frame))
                                    .await
                                    .expect("fake writes pong");
                            }
                            other @ 15 | other @ 16 => {
                                // Close producer or consumer: answer
                                // success. The driver sends this on
                                // cleanup. A real broker answers it.
                                let inner = mv03a_pb_find_bytes(&frame, other).unwrap_or(&[]);
                                let request_id = mv03a_pb_find_varint(inner, 2).unwrap_or(0);
                                let mut body = Vec::new();
                                mv03a_pb_varint(1, request_id, &mut body);
                                let mut response = Vec::new();
                                mv03a_pb_sub(13, &body, &mut response);
                                let frame = command(13, &response);
                                stream
                                    .write_all(&mv03a_pulsar_frame(&frame))
                                    .await
                                    .expect("fake writes close success");
                            }
                            other => panic!("fake speaks no pulsar command {other}"),
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
            error_sends,
            _task: task,
            connections,
        }
    }

    #[cfg(test)]
    fn arm_error_sends(&self) {
        *self.error_sends.lock().expect("fake arms") = true;
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<Mv03aPulsarMessage> {
        for _ in 0..300 {
            let got = self.received.lock().expect("fake reads").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "pulsar fake got {} messages, wanted {count}",
            self.received.lock().expect("fake reads").len()
        );
    }
}

#[tokio::test]
async fn mv03a_pulsar_delivers_keyed_messages_and_counts_a_send_error() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-pulsar");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aPulsarFake::start().await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let servers = format!("pulsar://127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "pulsar", "name": "mv03a-pu", "servers": servers}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "pulsar", "name": "mv03a-pu-token",
                       "servers": servers, "authentication": "token"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 400,
        "token auth is refused at the setting's home: {body}"
    );
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("authentication"),
        "{body}"
    );
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "pulsar", "name": "mv03a-pu-act",
                       "connector": "mv03a-pu",
                       "parameters": {"pulsar_topic": "events",
                                      "message": {"key": "${client_id}"},
                                      "strategy": "key_dispatch"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-pu-rule", "name": "mv03a-pu-rule",
                       "sql": "SELECT * FROM \"mv03a/pulsar\"", "actions": ["mv03a-pu-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(
        &server,
        &token,
        "mv03a/pulsar",
        "{\"client_id\":\"edge-3\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].topic_path, "persistent://public/default/events");
    assert_eq!(got[0].payload, br#"{"client_id":"edge-3"}"#.to_vec());
    assert_eq!(got[0].partition_key.as_deref(), Some("edge-3"));
    assert!(
        got[0]
            .properties
            .iter()
            .any(|(key, value)| key == "mqtt_topic" && value == "mv03a/pulsar"),
        "broker properties ride along: {:?}",
        got[0].properties
    );
    mv02b_wait_success(&server, &token, "mv03a-pu-act", 1).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03a-pu-act",
            json!({"type": "pulsar", "name": "mv03a-pu-act",
                       "connector": "mv03a-pu",
                       "parameters": {"pulsar_topic": "events2",
                                      "message": {"key": "${client_id}"},
                                      "strategy": "key_dispatch"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(
        &server,
        &token,
        "mv03a/pulsar",
        "{\"client_id\":\"edge-3\"}",
    )
    .await;
    let got = fake.wait_for(2).await;
    assert_eq!(got[1].topic_path, "persistent://public/default/events2");
    mv02b_wait_success(&server, &token, "mv03a-pu-act", 2).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03a-pu-act",
            json!({"type": "pulsar", "name": "mv03a-pu-act",
                       "connector": "mv03a-pu",
                       "parameters": {"pulsar_topic": "events2", "strategy": "bogus"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an unknown strategy refuses: {body}");
    mv02b_publish(
        &server,
        &token,
        "mv03a/pulsar",
        "{\"client_id\":\"edge-3\"}",
    )
    .await;
    let got = fake.wait_for(3).await;
    assert_eq!(
        got[2].topic_path, "persistent://public/default/events2",
        "the refused update keeps the old target"
    );
    mv02b_wait_success(&server, &token, "mv03a-pu-act", 3).await;

    let down_port = fake.port;
    drop(fake);
    mv02b_publish(
        &server,
        &token,
        "mv03a/pulsar",
        "{\"client_id\":\"edge-3\"}",
    )
    .await;
    mv03a_wait_failed(&server, &token, "mv03a-pu-act", 1).await;
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    let fake = Mv03aPulsarFake::start_on(down_port).await;
    mv02b_publish(
        &server,
        &token,
        "mv03a/pulsar",
        "{\"client_id\":\"edge-3\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].payload, br#"{"client_id":"edge-3"}"#.to_vec());
    mv02b_wait_success(&server, &token, "mv03a-pu-act", 4).await;

    // A broker send error counts one failed forward after the
    // driver retries exhaust.
    fake.arm_error_sends();
    mv02b_publish(
        &server,
        &token,
        "mv03a/pulsar",
        "{\"client_id\":\"edge-3\"}",
    )
    .await;
    mv03a_wait_failed(&server, &token, "mv03a-pu-act", 2).await;

    // A connector update through the production entry keeps delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv03a-pu",
            json!({"type": "pulsar", "name": "mv03a-pu",
                       "servers": format!("pulsar://127.0.0.1:{}", fake.port)}),
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
    let (status, _) = server.get_auth("/api/v5/connectors/mv03a-pu", &token).await;
    assert_eq!(status, 200, "stored connector survives the restart");
    let fake = Mv03aPulsarFake::start_on(restart_port).await;
    mv02b_publish(
        &server,
        &token,
        "mv03a/pulsar",
        "{\"client_id\":\"edge-3\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].topic_path, "persistent://public/default/events2");
    mv02b_wait_success(&server, &token, "mv03a-pu-act", 1).await;
    drop(_state);
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}
