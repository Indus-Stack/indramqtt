use super::*;

/// Kafka protocol helpers for the MV-03a broker fakes: one
/// length-prefixed frame read or write, and big-endian string and
/// int readers over a request body.
#[cfg(any(feature = "kafka", feature = "confluent"))]
#[cfg(test)]
async fn mv03a_kafka_read_frame(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut len_buf = [0u8; 4];
    // A closed client socket ends the fake connection, as a real
    // broker ends it: stop the task instead of panicking. The
    // probe dials and closes without sending, so EOF is common.
    stream.read_exact(&mut len_buf).await.ok()?;
    let len = i32::from_be_bytes(len_buf);
    if len < 0 || len as usize > 8 * 1024 * 1024 {
        return None;
    }
    let len = len as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).await.ok()?;
    Some(frame)
}

#[cfg(any(feature = "kafka", feature = "confluent"))]
#[cfg(test)]
async fn mv03a_kafka_write_frame(stream: &mut tokio::net::TcpStream, body: &[u8]) -> bool {
    use tokio::io::AsyncWriteExt;
    let mut framed = (body.len() as i32).to_be_bytes().to_vec();
    framed.extend_from_slice(body);
    stream.write_all(&framed).await.is_ok()
}

#[cfg(any(feature = "kafka", feature = "confluent"))]
#[cfg(test)]
fn mv03a_kafka_str(frame: &[u8], pos: &mut usize) -> Option<String> {
    if *pos + 2 > frame.len() {
        return None;
    }
    let len = i16::from_be_bytes([frame[*pos], frame[*pos + 1]]);
    if len < 0 {
        return None;
    }
    *pos += 2;
    let len = len as usize;
    if *pos + len > frame.len() {
        return None;
    }
    let text = String::from_utf8_lossy(&frame[*pos..*pos + len]).to_string();
    *pos += len;
    Some(text)
}

/// One captured Kafka produce request of the MV-03a Kafka fake:
/// the raw request frame plus the api version the driver used.
#[cfg(any(feature = "kafka", feature = "confluent"))]
#[derive(Clone)]
#[cfg(test)]
struct Mv03aKafkaProduced {
    api_version: i16,
    frame: Vec<u8>,
}

/// Minimal Kafka broker for the MV-03a delivery tests: answers
/// ApiVersions (v0-v3, newer versions close so the driver
/// downgrades), Metadata (v0/v1, one broker, twelve partitions)
/// and Produce (v0-v3 success) with canned data. Every other
/// api version closes the connection. The test asserts the topic,
/// key, value and header bytes on the wire.
#[cfg(any(feature = "kafka", feature = "confluent"))]
#[cfg(test)]
struct Mv03aKafkaFake {
    port: u16,
    produced: std::sync::Arc<std::sync::Mutex<Vec<Mv03aKafkaProduced>>>,
    #[cfg(feature = "confluent")]
    auth_bytes: std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    error_acks: std::sync::Arc<std::sync::Mutex<bool>>,
    _task: tokio::task::JoinHandle<()>,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[cfg(any(feature = "kafka", feature = "confluent"))]
#[cfg(test)]
impl Drop for Mv03aKafkaFake {
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

#[cfg(any(feature = "kafka", feature = "confluent"))]
#[cfg(test)]
impl Mv03aKafkaFake {
    #[cfg(test)]
    async fn start(sasl: bool) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake");
        let port = listener.local_addr().expect("fake addr").port();
        Self::serve(listener, port, sasl).await
    }

    #[cfg(test)]
    async fn start_on(port: u16, sasl: bool) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind fake");
        Self::serve(listener, port, sasl).await
    }

    #[cfg(test)]
    async fn serve(listener: tokio::net::TcpListener, port: u16, sasl: bool) -> Self {
        let produced = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        #[cfg(feature = "confluent")]
        let auth_bytes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let error_acks = std::sync::Arc::new(std::sync::Mutex::new(false));
        let connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let task_produced = std::sync::Arc::clone(&produced);
        #[cfg(feature = "confluent")]
        let task_auth = std::sync::Arc::clone(&auth_bytes);
        let task_error = std::sync::Arc::clone(&error_acks);
        let task_connections = std::sync::Arc::clone(&connections);
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                let produced = std::sync::Arc::clone(&task_produced);
                #[cfg(feature = "confluent")]
                let auth_bytes = std::sync::Arc::clone(&task_auth);
                let error_acks = std::sync::Arc::clone(&task_error);
                let connection = tokio::spawn(async move {
                    let mut stream = stream;
                    loop {
                        let Some(frame) = mv03a_kafka_read_frame(&mut stream).await else {
                            break;
                        };
                        if frame.len() < 8 {
                            break;
                        }
                        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
                        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
                        let correlation = [frame[4], frame[5], frame[6], frame[7]];
                        let mut response = correlation.to_vec();
                        match api_key {
                            // ApiVersions: v0-v3 list what this fake
                            // speaks. Newer versions close so the
                            // driver downgrades.
                            18 => {
                                if api_version > 3 {
                                    break;
                                }
                                // Produce 0-3, Metadata 0-0, Fetch 0-4
                                // (for RecordBatch headers), ApiVersions
                                // 0-3, SaslHandshake 0-1,
                                // SaslAuthenticate 0-0.
                                let apis: &[(i16, i16, i16)] = if sasl {
                                    &[
                                        (0, 0, 3),
                                        (1, 0, 4),
                                        (3, 0, 0),
                                        (18, 0, 3),
                                        (17, 0, 1),
                                        (36, 0, 0),
                                    ]
                                } else {
                                    &[(0, 0, 3), (1, 0, 4), (3, 0, 0), (18, 0, 3)]
                                };
                                response.extend_from_slice(&0i16.to_be_bytes());
                                if api_version >= 3 {
                                    // Compact array: length plus one as
                                    // varint, then each entry with tags.
                                    let mut len = (apis.len() + 1) as u32;
                                    loop {
                                        let mut byte = (len & 0x7F) as u8;
                                        len >>= 7;
                                        if len != 0 {
                                            byte |= 0x80;
                                        }
                                        response.push(byte);
                                        if len == 0 {
                                            break;
                                        }
                                    }
                                    for (key, min, max) in apis {
                                        response.extend_from_slice(&key.to_be_bytes());
                                        response.extend_from_slice(&min.to_be_bytes());
                                        response.extend_from_slice(&max.to_be_bytes());
                                        response.push(0);
                                    }
                                    response.extend_from_slice(&0i32.to_be_bytes());
                                    response.push(0);
                                } else {
                                    response.extend_from_slice(&(apis.len() as i32).to_be_bytes());
                                    for (key, min, max) in apis {
                                        response.extend_from_slice(&key.to_be_bytes());
                                        response.extend_from_slice(&min.to_be_bytes());
                                        response.extend_from_slice(&max.to_be_bytes());
                                    }
                                    if api_version >= 1 {
                                        response.extend_from_slice(&0i32.to_be_bytes());
                                    }
                                }
                                if !mv03a_kafka_write_frame(&mut stream, &response).await {
                                    break;
                                }
                            }
                            // SaslHandshake: PLAIN only. A newer version
                            // closes so the driver downgrades.
                            17 => {
                                if api_version > 1 {
                                    break;
                                }
                                let mut pos = 8;
                                let Some(_) = mv03a_kafka_str(&frame, &mut pos) else {
                                    break;
                                };
                                let Some(mechanism) = mv03a_kafka_str(&frame, &mut pos) else {
                                    break;
                                };
                                if mechanism != "PLAIN" {
                                    break;
                                }
                                response.extend_from_slice(&0i16.to_be_bytes());
                                response.extend_from_slice(&1i32.to_be_bytes());
                                response.extend_from_slice(&5i16.to_be_bytes());
                                response.extend_from_slice(b"PLAIN");
                                if !mv03a_kafka_write_frame(&mut stream, &response).await {
                                    break;
                                }
                            }
                            // SaslAuthenticate: capture the bytes,
                            // answer success.
                            36 => {
                                if frame.len() < 8 {
                                    break;
                                }
                                #[cfg(feature = "confluent")]
                                auth_bytes
                                    .lock()
                                    .expect("fake records")
                                    .push(frame[8..].to_vec());
                                response.extend_from_slice(&0i16.to_be_bytes());
                                response.extend_from_slice(&(-1i16).to_be_bytes());
                                response.extend_from_slice(&0i32.to_be_bytes());
                                if !mv03a_kafka_write_frame(&mut stream, &response).await {
                                    break;
                                }
                            }
                            // Metadata: v0/v1, one broker, one topic
                            // with twelve partitions. A newer version
                            // closes so the driver downgrades.
                            3 => {
                                if api_version > 1 {
                                    break;
                                }
                                if frame.len() < 12 {
                                    break;
                                }
                                let mut pos = 8;
                                // Skip the client id string, then read
                                // the topics array.
                                let Some(_) = mv03a_kafka_str(&frame, &mut pos) else {
                                    break;
                                };
                                let mut topics = Vec::new();
                                if pos + 4 > frame.len() {
                                    break;
                                }
                                let count = i32::from_be_bytes([
                                    frame[pos],
                                    frame[pos + 1],
                                    frame[pos + 2],
                                    frame[pos + 3],
                                ]);
                                pos += 4;
                                if count < -1 || count as usize > 1024 {
                                    break;
                                }
                                if count == -1 {
                                    // Null array means all topics. Answer
                                    // with no topics; the driver then
                                    // asks for the produce topic.
                                } else if count > 0 {
                                    let mut ok = true;
                                    for _ in 0..count as usize {
                                        let Some(topic) = mv03a_kafka_str(&frame, &mut pos) else {
                                            ok = false;
                                            break;
                                        };
                                        topics.push(topic);
                                    }
                                    if !ok {
                                        break;
                                    }
                                }
                                // brokers: one entry, this fake.
                                response.extend_from_slice(&1i32.to_be_bytes());
                                response.extend_from_slice(&0i32.to_be_bytes());
                                let host = b"127.0.0.1";
                                response.extend_from_slice(&(host.len() as i16).to_be_bytes());
                                response.extend_from_slice(host);
                                response.extend_from_slice(&(port as i32).to_be_bytes());
                                if api_version == 1 {
                                    // rack: absent.
                                    response.extend_from_slice(&(-1i16).to_be_bytes());
                                    response.extend_from_slice(&0i32.to_be_bytes());
                                }
                                response.extend_from_slice(&(topics.len() as i32).to_be_bytes());
                                for topic in &topics {
                                    response.extend_from_slice(&0i16.to_be_bytes());
                                    response.extend_from_slice(&(topic.len() as i16).to_be_bytes());
                                    response.extend_from_slice(topic.as_bytes());
                                    if api_version == 1 {
                                        response.push(0);
                                    }
                                    response.extend_from_slice(&12i32.to_be_bytes());
                                    for partition in 0..12i32 {
                                        response.extend_from_slice(&0i16.to_be_bytes());
                                        response.extend_from_slice(&partition.to_be_bytes());
                                        response.extend_from_slice(&0i32.to_be_bytes());
                                        response.extend_from_slice(&1i32.to_be_bytes());
                                        response.extend_from_slice(&0i32.to_be_bytes());
                                        response.extend_from_slice(&1i32.to_be_bytes());
                                        response.extend_from_slice(&0i32.to_be_bytes());
                                    }
                                }
                                if !mv03a_kafka_write_frame(&mut stream, &response).await {
                                    break;
                                }
                            }
                            // Produce: v0/v1/v2/v3 success, echoing
                            // the topic. A newer version closes so
                            // the driver downgrades. An armed error
                            // ack answers 29.
                            0 => {
                                if api_version > 3 {
                                    break;
                                }
                                if frame.len() < 14 {
                                    break;
                                }
                                produced
                                    .lock()
                                    .expect("fake records")
                                    .push(Mv03aKafkaProduced {
                                        api_version,
                                        frame: frame.clone(),
                                    });
                                let mut pos = 8;
                                // Skip the client id string, then read
                                // the transactional id for v2 and v3.
                                let Some(_) = mv03a_kafka_str(&frame, &mut pos) else {
                                    break;
                                };
                                if api_version >= 2 {
                                    // transactional_id, nullable.
                                    if pos + 2 > frame.len() {
                                        break;
                                    }
                                    let len = i16::from_be_bytes([frame[pos], frame[pos + 1]]);
                                    pos += 2;
                                    if len >= 0 {
                                        if len as usize > frame.len() {
                                            break;
                                        }
                                        pos += len as usize;
                                    }
                                }
                                if pos + 6 > frame.len() {
                                    break;
                                }
                                pos += 2 + 4; // acks, timeout
                                if pos + 4 > frame.len() {
                                    break;
                                }
                                let topic_count = i32::from_be_bytes([
                                    frame[pos],
                                    frame[pos + 1],
                                    frame[pos + 2],
                                    frame[pos + 3],
                                ]);
                                pos += 4;
                                if topic_count != 1 {
                                    break;
                                }
                                let Some(topic) = mv03a_kafka_str(&frame, &mut pos) else {
                                    break;
                                };
                                let error: i16 = if *error_acks.lock().expect("fake reads") {
                                    29
                                } else {
                                    0
                                };
                                // Produce success per version: v0 is
                                // topic, partition, error and base
                                // offset; v1 adds the throttle; v2 and
                                // v3 add the two timestamps.
                                response.extend_from_slice(&1i32.to_be_bytes());
                                response.extend_from_slice(&(topic.len() as i16).to_be_bytes());
                                response.extend_from_slice(topic.as_bytes());
                                response.extend_from_slice(&1i32.to_be_bytes());
                                response.extend_from_slice(&0i32.to_be_bytes());
                                response.extend_from_slice(&error.to_be_bytes());
                                response.extend_from_slice(&1i64.to_be_bytes());
                                if api_version >= 2 {
                                    response.extend_from_slice(&(-1i64).to_be_bytes());
                                    response.extend_from_slice(&(-1i64).to_be_bytes());
                                }
                                if api_version >= 1 {
                                    response.extend_from_slice(&0i32.to_be_bytes());
                                }
                                if !mv03a_kafka_write_frame(&mut stream, &response).await {
                                    break;
                                }
                            }
                            // An api the fake never speaks ends the
                            // connection, as a real broker ends it.
                            // The driver reconnects and downgrades.
                            _ => {
                                break;
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
            produced,
            #[cfg(feature = "confluent")]
            auth_bytes,
            error_acks,
            _task: task,
            connections,
        }
    }

    #[cfg(test)]
    fn arm_error_acks(&self) {
        *self.error_acks.lock().expect("fake arms") = true;
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<Mv03aKafkaProduced> {
        for _ in 0..400 {
            let got = self.produced.lock().expect("fake reads").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "kafka fake got {} produces, wanted {count}",
            self.produced.lock().expect("fake reads").len()
        );
    }
}

/// Partition index of one captured Produce request: the first
/// topic's first partition, parsed per api version.
#[cfg(any(feature = "kafka", feature = "confluent"))]
#[cfg(test)]
fn mv03a_kafka_partition(captured: &Mv03aKafkaProduced) -> i32 {
    let frame = &captured.frame;
    let mut pos = 8;
    let mut cursor = pos;
    let _ = mv03a_kafka_str(frame, &mut cursor);
    pos = cursor;
    if captured.api_version >= 2 {
        let len = i16::from_be_bytes([frame[pos], frame[pos + 1]]);
        pos += 2;
        if len >= 0 {
            pos += len as usize;
        }
    }
    pos += 2 + 4;
    pos += 4; // topic count
    let mut cursor = pos;
    let _ = mv03a_kafka_str(frame, &mut cursor);
    cursor += 4; // partition count
    i32::from_be_bytes([
        frame[cursor],
        frame[cursor + 1],
        frame[cursor + 2],
        frame[cursor + 3],
    ])
}

#[cfg(feature = "kafka")]
#[tokio::test]
async fn mv03a_kafka_delivers_keyed_records_and_counts_a_broker_refusal() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-kafka");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aKafkaFake::start(false).await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let bootstrap = format!("127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "kafka", "name": "mv03a-kafka", "bootstrap_hosts": bootstrap}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "kafka", "name": "mv03a-kafka-sasl",
                       "bootstrap_hosts": bootstrap,
                       "authentication": {"mechanism": "plain", "username": "u", "password": "p"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "SASL is refused at the setting's home: {body}");
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
            json!({"type": "kafka", "name": "mv03a-kafka-act",
                       "connector": "mv03a-kafka",
                       "resource_opts": {"health_check_interval": "1s"},
                       "parameters": {"topic": "events",
                                      "message": {"key": "${payload.k}"},
                                      "partition_strategy": "key_dispatch",
                                      "required_acks": "leader_only",
                                      "kafka_ext_headers": [{"kafka_ext_header_key": "src",
                                                             "kafka_ext_header_value": "${topic}"}],
                                      "kafka_header_value_encode_mode": "json"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-kafka-rule", "name": "mv03a-kafka-rule",
                       "sql": "SELECT * FROM \"mv03a/kafka\"", "actions": ["mv03a-kafka-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].frame.windows(6).any(|w| w == b"events"),
        "topic on the wire"
    );
    assert!(
        got[0].frame.windows(6).any(|w| w == b"edge-7"),
        "key on the wire"
    );
    assert!(
        got[0].frame.windows(14).any(|w| w == br#"{"k":"edge-7"}"#),
        "value on the wire"
    );
    assert!(
        got[0].frame.windows(13).any(|w| w == b"\"mv03a/kafka\""),
        "json header value on the wire"
    );
    assert!(
        got[0].frame.windows(3).any(|w| w == b"src"),
        "header key on the wire"
    );
    let partition = mv03a_kafka_partition(&got[0]);
    assert!(
        (0..12).contains(&partition),
        "partition in range: {partition}"
    );
    mv02b_wait_success(&server, &token, "mv03a-kafka-act", 1).await;

    // The same key sticks to the same partition.
    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    let got = fake.wait_for(2).await;
    assert_eq!(
        mv03a_kafka_partition(&got[1]),
        partition,
        "key_dispatch is sticky"
    );
    mv02b_wait_success(&server, &token, "mv03a-kafka-act", 2).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03a-kafka-act",
            json!({"type": "kafka", "name": "mv03a-kafka-act",
                       "connector": "mv03a-kafka",
                       "parameters": {"topic": "events2",
                                      "message": {"key": "${payload.k}"},
                                      "partition_strategy": "key_dispatch",
                                      "required_acks": "leader_only"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    let got = fake.wait_for(3).await;
    assert!(
        got[2].frame.windows(7).any(|w| w == b"events2"),
        "updated topic on the wire"
    );
    mv02b_wait_success(&server, &token, "mv03a-kafka-act", 3).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03a-kafka-act",
            json!({"type": "kafka", "name": "mv03a-kafka-act",
                       "connector": "mv03a-kafka",
                       "parameters": {"topic": "events2", "partition_strategy": "bogus"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an unknown strategy refuses: {body}");
    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    let got = fake.wait_for(4).await;
    assert!(
        got[3].frame.windows(7).any(|w| w == b"events2"),
        "the refused update keeps the old target"
    );
    mv02b_wait_success(&server, &token, "mv03a-kafka-act", 4).await;

    let down_port = fake.port;
    drop(fake);
    // The 1 s health probe marks the action disconnected while the
    // broker is down; it reads back connected after the restart.
    for _ in 0..400 {
        let (status, action) = server
            .get_auth("/api/v5/actions/mv03a-kafka-act", &token)
            .await;
        assert_eq!(status, 200);
        if action["status"] == json!("disconnected") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, action) = server
        .get_auth("/api/v5/actions/mv03a-kafka-act", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        action["status"],
        json!("disconnected"),
        "down reads disconnected"
    );
    // The probe fails closed and fast, so a publish while the
    // broker is down counts one failed forward.
    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    mv03a_wait_failed(&server, &token, "mv03a-kafka-act", 1).await;
    let fake = Mv03aKafkaFake::start_on(down_port, false).await;
    for _ in 0..400 {
        let (status, action) = server
            .get_auth("/api/v5/actions/mv03a-kafka-act", &token)
            .await;
        assert_eq!(status, 200);
        if action["status"] == json!("connected") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, action) = server
        .get_auth("/api/v5/actions/mv03a-kafka-act", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        action["status"],
        json!("connected"),
        "restart reads connected"
    );
    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].frame.windows(7).any(|w| w == b"events2"),
        "delivery resumes"
    );
    mv02b_wait_success(&server, &token, "mv03a-kafka-act", 5).await;

    // A terminal broker ack counts one failed forward, fast.
    fake.arm_error_acks();
    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    mv03a_wait_failed(&server, &token, "mv03a-kafka-act", 2).await;

    // A connector update through the production entry keeps delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv03a-kafka",
            json!({"type": "kafka", "name": "mv03a-kafka",
                       "bootstrap_hosts": format!("127.0.0.1:{}", fake.port)}),
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
    let (status, _) = server
        .get_auth("/api/v5/connectors/mv03a-kafka", &token)
        .await;
    assert_eq!(status, 200, "stored connector survives the restart");
    let fake = Mv03aKafkaFake::start_on(restart_port, false).await;
    mv02b_publish(&server, &token, "mv03a/kafka", "{\"k\":\"edge-7\"}").await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].frame.windows(7).any(|w| w == b"events2"),
        "delivery resumes after the broker restart"
    );
    mv02b_wait_success(&server, &token, "mv03a-kafka-act", 1).await;
    drop(_state);
    drop(server);
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "confluent")]
#[tokio::test]
async fn mv03a_confluent_delivers_through_sasl_to_the_proxy() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv03a-confluent");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03aKafkaFake::start(true).await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let bootstrap = format!("127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "confluent", "name": "mv03a-cf",
                       "bootstrap_hosts": bootstrap, "api_key": "k", "api_secret": "s",
                       "ssl": {"enable": false}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "confluent", "name": "mv03a-cf-act",
                       "connector": "mv03a-cf",
                       "resource_opts": {"health_check_interval": "1s"},
                       "parameters": {"topic": "events",
                                      "message": {"key": "${client_id}"},
                                      "partition_strategy": "random"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03a-cf-rule", "name": "mv03a-cf-rule",
                       "sql": "SELECT * FROM \"mv03a/confluent\"", "actions": ["mv03a-cf-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(
        &server,
        &token,
        "mv03a/confluent",
        "{\"client_id\":\"edge-9\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    let auth = fake.auth_bytes.lock().expect("fake reads").clone();
    assert!(!auth.is_empty(), "the driver authenticates SASL PLAIN");
    assert!(
        auth[0].windows(4).any(|w| w == b"\x00k\x00s"),
        "the PLAIN payload carries the key and the secret"
    );
    assert!(
        got[0].frame.windows(6).any(|w| w == b"events"),
        "topic on the wire"
    );
    assert!(
        got[0].frame.windows(6).any(|w| w == b"edge-9"),
        "key on the wire"
    );
    let partition = mv03a_kafka_partition(&got[0]);
    assert!(
        (0..12).contains(&partition),
        "partition in range: {partition}"
    );
    mv02b_wait_success(&server, &token, "mv03a-cf-act", 1).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03a-cf-act",
            json!({"type": "confluent", "name": "mv03a-cf-act",
                       "connector": "mv03a-cf",
                       "parameters": {"topic": "events2", "partition_strategy": "random"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(
        &server,
        &token,
        "mv03a/confluent",
        "{\"client_id\":\"edge-9\"}",
    )
    .await;
    let got = fake.wait_for(2).await;
    assert!(
        got[1].frame.windows(7).any(|w| w == b"events2"),
        "updated topic on the wire"
    );
    mv02b_wait_success(&server, &token, "mv03a-cf-act", 2).await;

    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv03a-cf-act",
            json!({"type": "confluent", "name": "mv03a-cf-act",
                       "connector": "mv03a-cf",
                       "parameters": {"topic": "events2", "partition_strategy": "bogus"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "an unknown strategy refuses: {body}");
    mv02b_publish(
        &server,
        &token,
        "mv03a/confluent",
        "{\"client_id\":\"edge-9\"}",
    )
    .await;
    let got = fake.wait_for(3).await;
    assert!(
        got[2].frame.windows(7).any(|w| w == b"events2"),
        "the refused update keeps the old target"
    );
    mv02b_wait_success(&server, &token, "mv03a-cf-act", 3).await;

    let down_port = fake.port;
    drop(fake);
    for _ in 0..400 {
        let (status, action) = server
            .get_auth("/api/v5/actions/mv03a-cf-act", &token)
            .await;
        assert_eq!(status, 200);
        if action["status"] == json!("disconnected") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, action) = server
        .get_auth("/api/v5/actions/mv03a-cf-act", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        action["status"],
        json!("disconnected"),
        "down reads disconnected"
    );
    // The probe fails closed and fast, so a publish while the
    // broker is down counts one failed forward.
    mv02b_publish(
        &server,
        &token,
        "mv03a/confluent",
        "{\"client_id\":\"edge-9\"}",
    )
    .await;
    mv03a_wait_failed(&server, &token, "mv03a-cf-act", 1).await;
    let fake = Mv03aKafkaFake::start_on(down_port, true).await;
    for _ in 0..400 {
        let (status, action) = server
            .get_auth("/api/v5/actions/mv03a-cf-act", &token)
            .await;
        assert_eq!(status, 200);
        if action["status"] == json!("connected") {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, action) = server
        .get_auth("/api/v5/actions/mv03a-cf-act", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        action["status"],
        json!("connected"),
        "restart reads connected"
    );
    mv02b_publish(
        &server,
        &token,
        "mv03a/confluent",
        "{\"client_id\":\"edge-9\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].frame.windows(7).any(|w| w == b"events2"),
        "delivery resumes"
    );
    mv02b_wait_success(&server, &token, "mv03a-cf-act", 4).await;

    fake.arm_error_acks();
    mv02b_publish(
        &server,
        &token,
        "mv03a/confluent",
        "{\"client_id\":\"edge-9\"}",
    )
    .await;
    mv03a_wait_failed(&server, &token, "mv03a-cf-act", 2).await;

    // A connector update through the production entry keeps delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv03a-cf",
            json!({"type": "confluent", "name": "mv03a-cf",
                       "bootstrap_hosts": format!("127.0.0.1:{}", fake.port),
                       "api_key": "k", "api_secret": "s",
                       "ssl": {"enable": false}}),
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
    let (status, _) = server.get_auth("/api/v5/connectors/mv03a-cf", &token).await;
    assert_eq!(status, 200, "stored connector survives the restart");
    let fake = Mv03aKafkaFake::start_on(restart_port, true).await;
    mv02b_publish(
        &server,
        &token,
        "mv03a/confluent",
        "{\"client_id\":\"edge-9\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].frame.windows(7).any(|w| w == b"events2"),
        "delivery resumes after the broker restart"
    );
    mv02b_wait_success(&server, &token, "mv03a-cf-act", 1).await;
    drop(_state);
    drop(server);
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}
