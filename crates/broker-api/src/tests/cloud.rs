use super::*;

/// One Publish RPC captured from the GCP gRPC loopback fake.
#[cfg(feature = "gcp_pubsub")]
#[derive(Clone, Debug)]
#[cfg(test)]
struct Mv03bGrpcPublish {
    path: String,
    topic: String,
    data: Vec<Vec<u8>>,
}

/// Limit on one fake handshake or request body. A stuck peer must not
/// hold the test for the whole gate window. 30 s is above a loaded
/// build host's worst local round trip.
#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
const MV03B_GRPC_FAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Minimal gRPC server for the Google Pub/Sub Publish RPC on HTTP/2.
/// The production sink uses the gRPC driver, so this fake decodes the
/// real PublishRequest and answers a PublishResponse. Dropping the
/// fake closes the port and aborts every connection.
#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
struct Mv03bGrpcFake {
    port: u16,
    received: std::sync::Arc<std::sync::Mutex<Vec<Mv03bGrpcPublish>>>,
    connections: std::sync::Arc<std::sync::Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    _task: tokio::task::JoinHandle<()>,
}

#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
impl Drop for Mv03bGrpcFake {
    #[cfg(test)]
    fn drop(&mut self) {
        self._task.abort();
        for handle in self
            .connections
            .lock()
            .expect("grpc fake connections")
            .drain(..)
        {
            handle.abort();
        }
    }
}

#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
impl Mv03bGrpcFake {
    #[cfg(test)]
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind grpc fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn start_on(port: u16) -> Self {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("rebind grpc fake");
        Self::serve(listener).await
    }

    #[cfg(test)]
    async fn serve(listener: tokio::net::TcpListener) -> Self {
        let port = listener.local_addr().expect("grpc fake addr").port();
        let received: std::sync::Arc<std::sync::Mutex<Vec<Mv03bGrpcPublish>>> =
            std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
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
                let connection_tasks = std::sync::Arc::clone(&task_connections);
                let handle = tokio::spawn(async move {
                    let handshake = tokio::time::timeout(
                        MV03B_GRPC_FAKE_TIMEOUT,
                        h2::server::handshake(stream),
                    )
                    .await;
                    let Ok(Ok(mut conn)) = handshake else {
                        return;
                    };
                    while let Some(result) = conn.accept().await {
                        let Ok((request, mut respond)) = result else {
                            break;
                        };
                        let received = std::sync::Arc::clone(&received);
                        let stream_task = tokio::spawn(async move {
                            let path = request.uri().path().to_string();
                            let mut body = request.into_body();
                            let read = tokio::time::timeout(MV03B_GRPC_FAKE_TIMEOUT, async {
                                let mut frame = Vec::new();
                                while let Some(chunk) = body.data().await {
                                    let Ok(chunk) = chunk else {
                                        return None;
                                    };
                                    let _ = body.flow_control().release_capacity(chunk.len());
                                    frame.extend_from_slice(&chunk);
                                }
                                Some(frame)
                            })
                            .await;
                            let Ok(Some(frame)) = read else {
                                return;
                            };
                            let (topic, data) = mv03b_parse_publish_request(&frame);
                            let reply = mv03b_publish_response(data.len().max(1));
                            received
                                .lock()
                                .expect("grpc fake lock")
                                .push(Mv03bGrpcPublish { path, topic, data });
                            let response = http::Response::builder()
                                .status(http::StatusCode::OK)
                                .header("content-type", "application/grpc")
                                .header("grpc-status", "0")
                                .body(())
                                .expect("grpc response");
                            let Ok(mut send) = respond.send_response(response, false) else {
                                return;
                            };
                            let _ = send.send_data(bytes::Bytes::from(reply), true);
                        });
                        let mut tasks = connection_tasks.lock().expect("grpc fake connections");
                        tasks.retain(|task| !task.is_finished());
                        tasks.push(stream_task);
                    }
                });
                let mut tasks = task_connections.lock().expect("grpc fake connections");
                tasks.retain(|task| !task.is_finished());
                tasks.push(handle);
            }
        });
        Self {
            port,
            received,
            connections,
            _task: task,
        }
    }

    #[cfg(test)]
    async fn wait_for(&self, count: usize) -> Vec<Mv03bGrpcPublish> {
        for _ in 0..100 {
            let got = self.received.lock().expect("grpc fake lock").clone();
            if got.len() >= count {
                return got;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!(
            "grpc fake got {} requests, wanted at least {count}",
            self.received.lock().expect("grpc fake lock").len()
        );
    }
}

#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
fn mv03b_read_varint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = *buf.get(*pos)?;
        *pos += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
        shift += 7;
        if shift >= 64 {
            return None;
        }
    }
}

#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
fn mv03b_parse_protobuf_fields(buf: &[u8]) -> Vec<(u64, &[u8])> {
    let mut fields = Vec::new();
    let mut pos = 0;
    while pos < buf.len() {
        let Some(key) = mv03b_read_varint(buf, &mut pos) else {
            break;
        };
        let field = key >> 3;
        match key & 7 {
            0 => {
                if mv03b_read_varint(buf, &mut pos).is_none() {
                    break;
                }
            }
            1 => {
                if pos + 8 > buf.len() {
                    break;
                }
                pos += 8;
            }
            5 => {
                if pos + 4 > buf.len() {
                    break;
                }
                pos += 4;
            }
            2 => {
                let Some(len) = mv03b_read_varint(buf, &mut pos) else {
                    break;
                };
                let len = len as usize;
                if pos + len > buf.len() {
                    break;
                }
                fields.push((field, &buf[pos..pos + len]));
                pos += len;
            }
            _ => break,
        }
    }
    fields
}

#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
fn mv03b_parse_publish_request(frame: &[u8]) -> (String, Vec<Vec<u8>>) {
    // gRPC frame: one compression byte plus a four-byte length.
    if frame.len() < 5 || frame[0] != 0 {
        return (String::new(), Vec::new());
    }
    let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    let end = 5 + len.min(frame.len() - 5);
    let request = &frame[5..end];
    let mut topic = String::new();
    let mut data = Vec::new();
    for (field, value) in mv03b_parse_protobuf_fields(request) {
        match field {
            1 => topic = String::from_utf8_lossy(value).to_string(),
            2 => {
                for (inner, bytes) in mv03b_parse_protobuf_fields(value) {
                    // `PubsubMessage.data` is field 1; field 2 is the
                    // attributes map.
                    if inner == 1 {
                        data.push(bytes.to_vec());
                    }
                }
            }
            _ => {}
        }
    }
    (topic, data)
}

#[cfg(feature = "gcp_pubsub")]
#[cfg(test)]
fn mv03b_publish_response(count: usize) -> Vec<u8> {
    let mut message = Vec::new();
    for index in 0..count {
        let id = format!("m{}", index + 1);
        message.push(0x0A);
        message.push(id.len() as u8);
        message.extend_from_slice(id.as_bytes());
    }
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0);
    framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
    framed.extend_from_slice(&message);
    framed
}

/// Poll one action's metrics until `failed` reaches one.
/// Return the count. The caller asserts the count with its own message.
#[cfg(any(
    feature = "kinesis",
    feature = "gcp_pubsub",
    feature = "azure_eventhubs"
))]
#[cfg(test)]
async fn mv03b_wait_failed(server: &TestServer, token: &str, action: &str) -> u64 {
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth(&format!("/api/v5/actions/{action}/metrics"), token)
            .await;
        assert_eq!(status, 200);
        let failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            return failed;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    0
}

#[cfg(feature = "kinesis")]
#[tokio::test]
async fn mv03b_kinesis_delivers_updates_and_counts_a_dead_server_through_the_broker() {
    use base64::Engine as _;
    let dir = unique_data_dir("mv03b-kinesis");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let ok_body = "{\"FailedRecordCount\":0,\"Records\":[{\"SequenceNumber\":\"1\",\"ShardId\":\"shard-0\"}]}";
    // The restored event and the new event share one PutRecords call
    // after the restart, so the reply carries one result per record.
    let ok_body_restart = "{\"FailedRecordCount\":0,\"Records\":[{\"SequenceNumber\":\"1\",\"ShardId\":\"shard-0\"},{\"SequenceNumber\":\"2\",\"ShardId\":\"shard-0\"}]}";
    let fake = Mv02bFake::start(200, ok_body).await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "kinesis", "name": "mv03b-kinesis",
                       "endpoint": endpoint, "region": "us-east-1",
                       "aws_access_key_id": "AKID", "aws_secret_access_key": "secret"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "kinesis", "name": "mv03b-kinesis-act",
                       "connector": "mv03b-kinesis",
                       "parameters": {"stream_name": "telemetry",
                                      "partition_key": "${client_id}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03b-kinesis-rule", "name": "mv03b-kinesis-rule",
                       "sql": "SELECT * FROM \"mv03b/k\"", "actions": ["mv03b-kinesis-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(
        &server,
        &token,
        "mv03b/k",
        "{\"client_id\":\"dev-1\",\"v\":1}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    let text = Mv02bFake::body_text(&got[0]);
    let wire: serde_json::Value = serde_json::from_str(&text).expect("kinesis body is JSON");
    let records = wire
        .get("Records")
        .and_then(|value| value.as_array())
        .expect("kinesis Records travel");
    let record = records.first().expect("at least one kinesis record");
    assert_eq!(
        wire.get("StreamName").and_then(|v| v.as_str()),
        Some("telemetry"),
        "the stream travels exactly: {text}"
    );
    assert_eq!(
        record.get("PartitionKey").and_then(|v| v.as_str()),
        Some("dev-1"),
        "the key value travels exactly: {text}"
    );
    let data_b64 = record
        .get("Data")
        .and_then(|v| v.as_str())
        .expect("the payload bytes travel");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .expect("kinesis data is base64");
    assert_eq!(
        String::from_utf8_lossy(&decoded),
        "{\"client_id\":\"dev-1\",\"v\":1}",
        "the payload value travels exactly: {text}"
    );
    mv02b_wait_success(&server, &token, "mv03b-kinesis-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03b-kinesis-act",
            json!({"type": "kinesis", "name": "mv03b-kinesis-act",
                       "connector": "mv03b-kinesis",
                       "parameters": {"stream_name": "updated",
                                      "partition_key": "${client_id}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv03b/k", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    let text = Mv02bFake::body_text(&got[1]);
    let wire: serde_json::Value = serde_json::from_str(&text).expect("kinesis body is JSON");
    let records = wire
        .get("Records")
        .and_then(|value| value.as_array())
        .expect("kinesis Records travel");
    let _record = records.first().expect("at least one kinesis record");
    assert_eq!(
        wire.get("StreamName").and_then(|v| v.as_str()),
        Some("updated"),
        "the update travels exactly: {text}"
    );
    mv02b_wait_success(&server, &token, "mv03b-kinesis-act", 2).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03b-kinesis-act",
            json!({"type": "kinesis", "name": "mv03b-kinesis-act",
                       "connector": "mv03b-kinesis",
                       "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a refused update keeps the old target");
    mv02b_publish(&server, &token, "mv03b/k", "{\"v\":3}").await;
    mv02b_wait_success(&server, &token, "mv03b-kinesis-act", 3).await;
    drop(fake);
    mv02b_publish(&server, &token, "mv03b/k", "{\"v\":4}").await;
    let failed = mv03b_wait_failed(&server, &token, "mv03b-kinesis-act").await;
    assert!(
        failed >= 1,
        "a dead kinesis endpoint counts one failed forward"
    );
    let fake = Mv02bFake::start_on(port, 200, ok_body_restart).await;
    // The dead forward arms a 2 s backoff; wait past it so the
    // next forward dials the restarted fake instead of failing fast.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    mv02b_publish(&server, &token, "mv03b/k", "{\"v\":5}").await;
    mv02b_wait_success(&server, &token, "mv03b-kinesis-act", 4).await;
    let got = fake.wait_for(1).await;
    let body = Mv02bFake::body_text(&got[0]);
    let doc: serde_json::Value = serde_json::from_str(&body).expect("kinesis body is JSON");
    let records = doc
        .get("Records")
        .and_then(|value| value.as_array())
        .expect("kinesis Records travel");
    let mut found = false;
    for record in records {
        if let Some(data_b64) = record.get("Data").and_then(|value| value.as_str()) {
            if let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(data_b64) {
                if String::from_utf8_lossy(&decoded).contains("\"v\":5") {
                    found = true;
                }
            }
        }
    }
    assert!(found, "the v5 payload travels: {body}");
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "gcp_pubsub")]
#[tokio::test]
async fn mv03b_gcp_pubsub_delivers_updates_and_counts_a_dead_server_through_the_broker() {
    let dir = unique_data_dir("mv03b-gcp");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv03bGrpcFake::start().await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "gcp_pubsub", "name": "mv03b-gcp",
                       "project_id": "demo-proj", "endpoint": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, stored) = server
        .get_auth("/api/v5/connectors/mv03b-gcp", &token)
        .await;
    assert_eq!(status, 200);
    assert!(
        stored.get("authentication").is_none(),
        "no auth is stored: {stored}"
    );
    assert_eq!(
        stored["type"], "gcp_pubsub",
        "the base kind is stored: {stored}"
    );
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "gcp_pubsub", "name": "mv03b-gcp-act",
                       "connector": "mv03b-gcp",
                       "parameters": {"pubsub_topic": "events"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03b-gcp-rule", "name": "mv03b-gcp-rule",
                       "sql": "SELECT * FROM \"mv03b/g\"", "actions": ["mv03b-gcp-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv03b/g", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].path.ends_with("/google.pubsub.v1.Publisher/Publish"),
        "the Publish RPC travels: {}",
        got[0].path
    );
    assert_eq!(
        got[0].topic, "projects/demo-proj/topics/events",
        "the topic travels exactly"
    );
    assert_eq!(got[0].data.len(), 1, "one event travels");
    assert_eq!(
        String::from_utf8_lossy(&got[0].data[0]),
        "{\"v\":1}",
        "the message travels exactly"
    );
    mv02b_wait_success(&server, &token, "mv03b-gcp-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03b-gcp-act",
            json!({"type": "gcp_pubsub", "name": "mv03b-gcp-act",
                       "connector": "mv03b-gcp",
                       "parameters": {"pubsub_topic": "updated"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv03b/g", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert_eq!(
        got[1].topic, "projects/demo-proj/topics/updated",
        "the update travels exactly"
    );
    mv02b_wait_success(&server, &token, "mv03b-gcp-act", 2).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03b-gcp-act",
            json!({"type": "gcp_pubsub", "name": "mv03b-gcp-act",
                       "connector": "mv03b-gcp",
                       "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a refused update keeps the old target");
    drop(fake);
    mv02b_publish(&server, &token, "mv03b/g", "{\"v\":3}").await;
    let failed = mv03b_wait_failed(&server, &token, "mv03b-gcp-act").await;
    assert!(
        failed >= 1,
        "a dead pubsub endpoint counts one failed forward"
    );
    let fake = Mv03bGrpcFake::start_on(port).await;
    // The dead forward arms a 2 s backoff; wait past it so the
    // next forward dials the restarted fake instead of failing fast.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    mv02b_publish(&server, &token, "mv03b/g", "{\"v\":4}").await;
    mv02b_wait_success(&server, &token, "mv03b-gcp-act", 3).await;
    // The restored event travels first, then the new event.
    let got = fake.wait_for(2).await;
    assert_eq!(
        got[1].topic, "projects/demo-proj/topics/updated",
        "the topic travels exactly after restart"
    );
    assert_eq!(got[1].data.len(), 1, "one event travels after restart");
    assert_eq!(
        String::from_utf8_lossy(&got[1].data[0]),
        "{\"v\":4}",
        "redelivery travels exactly after restart"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "azure_eventhubs")]
#[tokio::test]
async fn mv03b_azure_eventhubs_delivers_updates_and_counts_a_dead_server_through_the_broker() {
    use base64::Engine as _;
    let dir = unique_data_dir("mv03b-azeh");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(201, "").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "azure_eventhubs", "name": "mv03b-azeh",
                       "namespace": "ns-1", "endpoint": endpoint,
                       "shared_access_key_name": "send", "shared_access_key": "secret"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "azure_eventhubs", "name": "mv03b-azeh-act",
                       "connector": "mv03b-azeh",
                       "parameters": {"event_hub": "hub-a"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv03b-azeh-rule", "name": "mv03b-azeh-rule",
                       "sql": "SELECT * FROM \"mv03b/e\"", "actions": ["mv03b-azeh-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv03b/e", "{\"v\":21}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert!(
        got[0].path.contains("hub-a"),
        "the hub travels: {}",
        got[0].path
    );
    assert!(
        got[0]
            .authorization
            .as_deref()
            .unwrap_or("")
            .starts_with("SharedAccessSignature"),
        "the SAS travels"
    );
    assert!(
        Mv02bFake::body_text(&got[0]).contains("\"Body\""),
        "the event travels"
    );
    let wire: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[0])).expect("eventhubs body is JSON");
    let encoded = wire
        .as_array()
        .and_then(|items| items.first())
        .and_then(|item| item.get("Body"))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("eventhubs Body is base64");
    assert!(
        String::from_utf8_lossy(&decoded).contains("\"v\":21"),
        "the value 21 travels"
    );
    mv02b_wait_success(&server, &token, "mv03b-azeh-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03b-azeh-act",
            json!({"type": "azure_eventhubs", "name": "mv03b-azeh-act",
                       "connector": "mv03b-azeh",
                       "parameters": {"event_hub": "hub-b"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv03b/e", "{\"v\":22}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got[1].path.contains("hub-b"),
        "the update travels: {}",
        got[1].path
    );
    mv02b_wait_success(&server, &token, "mv03b-azeh-act", 2).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv03b-azeh-act",
            json!({"type": "azure_eventhubs", "name": "mv03b-azeh-act",
                       "connector": "mv03b-azeh",
                       "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a refused update keeps the old target");
    drop(fake);
    mv02b_publish(&server, &token, "mv03b/e", "{\"v\":23}").await;
    let failed = mv03b_wait_failed(&server, &token, "mv03b-azeh-act").await;
    assert!(failed >= 1, "a dead hub counts one failed forward");
    let fake = Mv02bFake::start_on(port, 201, "").await;
    // The dead forward arms a 2 s backoff; wait past it so the
    // next forward dials the restarted fake instead of failing fast.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    mv02b_publish(&server, &token, "mv03b/e", "{\"v\":24}").await;
    mv02b_wait_success(&server, &token, "mv03b-azeh-act", 3).await;
    let got = fake.wait_for(1).await;
    let wire: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[0])).expect("eventhubs body is JSON");
    // The restored event travels first; the new event follows it.
    let encoded = wire
        .as_array()
        .and_then(|items| items.get(1))
        .and_then(|item| item.get("Body"))
        .and_then(|value| value.as_str())
        .unwrap_or("");
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .expect("eventhubs Body is base64");
    assert!(
        String::from_utf8_lossy(&decoded).contains("\"v\":24"),
        "the value 24 travels after restart"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(any(
    feature = "kinesis",
    feature = "gcp_pubsub",
    feature = "azure_eventhubs"
))]
#[tokio::test]
async fn mv03b_tenant_gate_keeps_cloud_forwards_in_default_tenant() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // A second namespace owns the tenant-B rules below.
    let (status, _) = server
        .post_auth("/api/v5/mt/ns/mv03b-tenant", json!({}), &token)
        .await;
    assert_eq!(status, 204);
    #[cfg(any(
        feature = "kinesis",
        feature = "gcp_pubsub",
        feature = "azure_eventhubs"
    ))]
    let closed = "http://127.0.0.1:9".to_string();
    // One connector and action per ready cloud kind. A kind whose
    // Cargo feature is off refuses closed, so the gate is proved for
    // it in the full build.
    let mut proven = 0;
    // Kinesis.
    #[cfg(feature = "kinesis")]
    {
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "kinesis", "name": "mv03b-ten-kinesis",
                           "endpoint": closed, "region": "us-east-1",
                           "aws_access_key_id": "k", "aws_secret_access_key": "s"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "kinesis", "name": "mv03b-ten-kinesis-act",
                           "connector": "mv03b-ten-kinesis",
                           "parameters": {"stream_name": "telemetry",
                                          "partition_key": "${client_id}"}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": "mv03b-ten-kinesis-rule", "name": "mv03b-ten-kinesis-rule",
                           "sql": "SELECT * FROM \"mv03b/ten\"",
                           "tenant": "mv03b-tenant", "actions": ["mv03b-ten-kinesis-act"]}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "kinesis tenant forward refuses: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains("tenant"),
            "{body}"
        );
        proven += 1;
    }
    // Pub/Sub.
    #[cfg(feature = "gcp_pubsub")]
    {
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "gcp_pubsub", "name": "mv03b-ten-gcp",
                           "project_id": "demo", "endpoint": closed}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "gcp_pubsub", "name": "mv03b-ten-gcp-act",
                           "connector": "mv03b-ten-gcp",
                           "parameters": {"pubsub_topic": "events"}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": "mv03b-ten-gcp-rule", "name": "mv03b-ten-gcp-rule",
                           "sql": "SELECT * FROM \"mv03b/ten\"",
                           "tenant": "mv03b-tenant", "actions": ["mv03b-ten-gcp-act"]}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "gcp tenant forward refuses: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains("tenant"),
            "{body}"
        );
        proven += 1;
    }
    // Event Hubs.
    #[cfg(feature = "azure_eventhubs")]
    {
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "azure_eventhubs", "name": "mv03b-ten-azeh",
                           "namespace": "ns-1", "endpoint": closed,
                           "shared_access_key_name": "send", "shared_access_key": "s"}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "azure_eventhubs", "name": "mv03b-ten-azeh-act",
                           "connector": "mv03b-ten-azeh",
                           "parameters": {"event_hub": "hub-a"}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, body) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": "mv03b-ten-azeh-rule", "name": "mv03b-ten-azeh-rule",
                           "sql": "SELECT * FROM \"mv03b/ten\"",
                           "tenant": "mv03b-tenant", "actions": ["mv03b-ten-azeh-act"]}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "event hubs tenant forward refuses: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains("tenant"),
            "{body}"
        );
        proven += 1;
    }
    assert!(proven >= 1, "at least one kind proves the tenant gate");
}

#[tokio::test]
async fn mv03b_gcp_iot_refuses_at_create_update_probe_and_replays_stored_state() {
    let dir = unique_data_dir("mv03b-gcpiot");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    // Keep one live connector. Updates below must keep it.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"id": "mv03b-gcpiot-keep", "type": "webhook",
                       "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    for (kind, body) in [
        (
            "gcp_iot",
            json!({"private_key_pem": "k", "project_id": "p", "cloud_region": "r", "registry_id": "g", "device_id": "d"}),
        ),
        (
            "gcp_iot_core",
            json!({"private_key_pem": "k", "project_id": "p", "cloud_region": "r", "registry_id": "g", "device_id": "d"}),
        ),
        (
            "cloud_iot",
            json!({"private_key_pem": "k", "project_id": "p", "cloud_region": "r", "registry_id": "g", "device_id": "d"}),
        ),
    ] {
        let (status, body_out) = server
            .post_auth(
                "/api/v5/connectors",
                Value::Object(
                    json!({"type": kind, "name": format!("mv03b-ref-{kind}")})
                        .as_object()
                        .unwrap()
                        .clone()
                        .into_iter()
                        .chain(body.as_object().unwrap().clone())
                        .collect::<serde_json::Map<String, Value>>(),
                ),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} refused: {body_out}");
        assert!(
            body_out["message"]
                .as_str()
                .unwrap_or("")
                .contains("not available"),
            "{body_out}"
        );
    }
    for kind in ["gcp_iot", "cloud_iot"] {
        let (status, body_out) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": kind, "name": format!("mv03b-ref-bare-{kind}")}),
                &token,
            )
            .await;
        assert_eq!(
            status, 400,
            "a bare {kind} names the credential first: {body_out}"
        );
        assert!(
            body_out["message"]
                .as_str()
                .unwrap_or("")
                .contains("private_key_pem"),
            "{body_out}"
        );
    }
    // Update with a refused kind keeps the old record.
    let (status, body_out) = server
        .put_auth(
            "/api/v5/connectors/mv03b-gcpiot-keep",
            json!({"type": "gcp_iot", "name": "mv03b-gcpiot-keep",
                       "private_key_pem": "k", "project_id": "p",
                       "cloud_region": "r", "registry_id": "g", "device_id": "d"}),
            &token,
        )
        .await;
    assert_eq!(
        status, 400,
        "a refused update keeps the old record: {body_out}"
    );
    let (status, kept) = server
        .get_auth("/api/v5/connectors/mv03b-gcpiot-keep", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(kept["type"], json!("webhook"));
    // Update of an unknown id with a refused kind still refuses.
    let (status, body_out) = server
        .put_auth(
            "/api/v5/connectors/mv03b-no-such",
            json!({"type": "gcp_iot", "name": "mv03b-no-such"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a refused update needs no record: {body_out}");
    let (status, body_out) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "gcp_iot"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "{body_out}");
    assert_eq!(body_out["code"], "TEST_FAILED");
    // A file load that carries a refused kind refuses.
    let mut candidate = state.config.snapshot().as_ref().clone();
    candidate
        .connectors
        .connectors
        .push(broker_config::ConnectorEntry {
            id: "mv03b-gcpiot-file".to_string(),
            connector_type: "gcp_iot".to_string(),
            enable: true,
            config: "{}".to_string(),
        });
    let (status, body_out) = server
        .post_auth(
            "/api/v1/config/reload",
            json!({"snapshot": candidate, "actor": "mv03b", "summary": "refuse gcp_iot"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a file load refuses the kind: {body_out}");
    // Stored state replays: the keep record survives a restart.
    drop(server);
    drop(state);
    drop(registry);
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    // The admin password persists across the restart.
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, kept) = server
        .get_auth("/api/v5/connectors/mv03b-gcpiot-keep", &token)
        .await;
    assert_eq!(status, 200, "the keep record replays: {kept}");
    assert_eq!(kept["type"], json!("webhook"));
    let (status, body_out) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "gcp_iot", "name": "mv03b-ref-after",
                       "private_key_pem": "k", "project_id": "p",
                       "cloud_region": "r", "registry_id": "g", "device_id": "d"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "the refusal survives the restart: {body_out}");
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03b_connector_shapes_refuse_missing_fields_and_accept_aliases() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    for (kind, body, field) in [
        (
            "kinesis",
            json!({"region": "us-east-1", "aws_access_key_id": "k", "aws_secret_access_key": "s"}),
            "endpoint",
        ),
        ("gcp_pubsub", json!({}), "project_id"),
        (
            "azure_eventhubs",
            json!({"shared_access_key": "s"}),
            "namespace",
        ),
    ] {
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                Value::Object(
                    json!({"type": kind, "name": format!("mv03b-flat-{kind}")})
                        .as_object()
                        .unwrap()
                        .clone()
                        .into_iter()
                        .chain(body.as_object().unwrap().clone())
                        .collect::<serde_json::Map<String, Value>>(),
                ),
                &token,
            )
            .await;
        assert_eq!(
            status, 400,
            "{kind} refuses without {field} after the credential gate: {kind}"
        );
    }
    // Alias creates route to the same runtime.
    // Kinesis needs its Cargo feature; without it the create
    // refuses and names the feature.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "aws_kinesis", "name": "mv03b-alias-k",
                       "endpoint": "http://127.0.0.1:9", "region": "us-east-1",
                       "aws_access_key_id": "k", "aws_secret_access_key": "s"}),
            &token,
        )
        .await;
    #[cfg(feature = "kinesis")]
    assert_eq!(status, 201, "{body}");
    #[cfg(not(feature = "kinesis"))]
    assert_eq!(status, 400, "kinesis without the feature refuses: {body}");
}

#[tokio::test]
async fn mv03b_reference_only_bodies_refuse_with_driver_field() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // A body with only reference-style fields has no driver
    // identity. The create refuses with the driver field named.
    for (kind, body, field) in [
        (
            "kinesis",
            json!({"authentication": {"type": "api_key"}}),
            "endpoint",
        ),
        (
            "gcp_pubsub",
            json!({"authentication": {"type": "attached_service_account"}}),
            "project_id",
        ),
    ] {
        let (status, body_out) = server
            .post_auth(
                "/api/v5/connectors",
                Value::Object(
                    json!({"type": kind, "name": format!("mv03b-refonly-{kind}")})
                        .as_object()
                        .unwrap()
                        .clone()
                        .into_iter()
                        .chain(body.as_object().unwrap().clone())
                        .collect::<serde_json::Map<String, Value>>(),
                ),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} refuses a reference-only body");
        assert!(
            body_out["message"].as_str().unwrap_or("").contains(field),
            "{kind} names {field}: {body_out}"
        );
    }
}

#[cfg(feature = "azure_eventhubs")]
#[tokio::test]
async fn mv03b_azure_eventhubs_accepts_reference_connector_body() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body_out) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "azure_eventhubs",
                       "name": "mv03b-ref-azure",
                       "authentication": {
                           "password": "Endpoint=sb://ns-ref.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=c2VjcmV0"
                       },
                       "bootstrap_hosts": "ns-ref.servicebus.windows.net:9093",
                       "ssl": {}}),
                &token,
            )
            .await;
    assert_eq!(status, 201, "{body_out}");
    let (status, stored) = server
        .get_auth("/api/v5/connectors/mv03b-ref-azure", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(stored["namespace"], json!("ns-ref"));
    assert_eq!(stored["shared_access_key_name"], json!("send"));
    // The key value is a secret: a read shows the mask.
    assert_eq!(stored["shared_access_key"], json!("******"));
    assert!(!stored.to_string().contains("c2VjcmV0"));
}

#[tokio::test]
async fn mv03b_pending_iot_kinds_refuse_create_and_update_with_bad_request() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Keep one live connector. A refused update must keep it.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"id": "mv03b-pending-keep", "type": "webhook",
                       "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let bodies = [
        (
            "aws_iot",
            json!({"endpoint": "127.0.0.1:8883", "region": "us-east-1",
                       "client_id": "pending-1",
                       "client_cert_pem": "-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----",
                       "client_key_pem": "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----"}),
        ),
        (
            "azure_iot",
            json!({"iot_hub_name": "127.0.0.1:8883", "device_id": "edge-1",
                       "shared_access_key": "secret"}),
        ),
        (
            "oci_streaming",
            json!({"endpoint": "https://cell-1.streaming.us-east-1.oraclecloud.com",
                       "stream_pool_id": "ocid1.streampool.oc1.phx.aaa",
                       "stream_id": "ocid1.stream.oc1.phx.aaa",
                       "tenancy_ocid": "ocid1.tenancy.oc1..aaa",
                       "user_ocid": "ocid1.user.oc1..aaa",
                       "fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
                       "private_key_pem": "key"}),
        ),
    ];
    for (kind, fields) in bodies {
        let name = format!("mv03b-pending-{kind}");
        let mut body = fields.as_object().expect("object").clone();
        body.insert("type".to_string(), json!(kind));
        body.insert("name".to_string(), json!(name));
        let (status, out) = server
            .post_auth("/api/v5/connectors", Value::Object(body.clone()), &token)
            .await;
        assert_eq!(status, 400, "{kind} connector create refuses: {out}");
        assert_eq!(out["code"], "BAD_REQUEST", "{kind} create code: {out}");
        assert!(
            out["message"]
                .as_str()
                .unwrap_or("")
                .contains("not available"),
            "{kind} names the refusal: {out}"
        );
        let (status, _) = server
            .get_auth(&format!("/api/v5/connectors/{name}"), &token)
            .await;
        assert_eq!(status, 404, "{kind} create stores nothing");
        let (status, out) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": kind, "name": format!("mv03b-pending-act-{kind}")}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} action create refuses: {out}");
        assert_eq!(out["code"], "BAD_REQUEST", "{kind} action code: {out}");
        let (status, out) = server
            .put_auth(
                "/api/v5/connectors/mv03b-pending-keep",
                Value::Object(body),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} connector update refuses: {out}");
        assert_eq!(out["code"], "BAD_REQUEST", "{kind} update code: {out}");
        // Action update to a pending kind is refused too.
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "webhook", "name": format!("mv03b-pending-act-{kind}"),
                           "connector": "mv03b-pending-keep",
                           "url": "http://127.0.0.1:9"}),
                &token,
            )
            .await;
        assert_eq!(status, 201, "{kind} webhook action creates");
        let (status, out) = server
            .put_auth(
                &format!("/api/v5/actions/mv03b-pending-act-{kind}"),
                json!({"type": kind, "name": format!("mv03b-pending-act-{kind}")}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "{kind} action update refuses: {out}");
        assert_eq!(
            out["code"], "BAD_REQUEST",
            "{kind} action update code: {out}"
        );
    }
    // Pending kind refusals raise alarms.
    let (status, alarms) = server.get_auth("/api/v5/alarms", &token).await;
    assert_eq!(status, 200);
    let alarm_data = alarms["data"].as_array().expect("alarm rows");
    for kind in ["aws_iot", "azure_iot", "oci_streaming"] {
        assert!(
            alarm_data
                .iter()
                .any(|a| { a["name"].as_str().unwrap_or("").contains(kind) }),
            "{kind} raises an alarm: {alarms:?}"
        );
    }
    let (status, kept) = server
        .get_auth("/api/v5/connectors/mv03b-pending-keep", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        kept["type"],
        json!("webhook"),
        "a refused update keeps the old record"
    );
    let (status, names) = server.get_auth("/api/v5/action_types", &token).await;
    assert_eq!(status, 200);
    let names = names.as_array().expect("action type list");
    for kind in ["aws_iot", "azure_iot", "oci_streaming"] {
        assert!(
            !names.iter().any(|name| name.as_str() == Some(kind)),
            "action_types excludes {kind}: {names:?}"
        );
    }
    // A stored record of a pending kind stays readable but never starts.
    // Write it to the boot file, as an older state file would hold it.
    drop(server);
    drop(state);
    let dir = unique_data_dir("mv03b-pending-stored");
    std::fs::create_dir_all(&dir).expect("create data dir");
    let pending_state = r#"
[[connectors.connectors]]
id = "mv03b-pending-stored"
connector_type = "aws_iot"
enable = true
config = '{}'

[[actions.actions]]
id = "mv03b-pending-stored-act"
action_type = "aws_iot"
enable = true
config = '{"connector":"mv03b-pending-stored","parameters":{}}'
"#;
    std::fs::write(dir.join("state.toml"), pending_state).expect("write pending state");
    let registry =
        Arc::new(ConfigRegistry::load_for_boot(&dir).expect("pending state loads for boot"));
    let (server, state) = TestServer::start_with_registry(registry).await;
    let token = server.login_as_admin().await;
    assert!(
        state
            .engine
            .connectors()
            .get("mv03b-pending-stored")
            .is_none(),
        "a stored pending connector does not start"
    );
    let (status, stored) = server
        .get_auth("/api/v5/connectors/mv03b-pending-stored", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(stored["status"], json!("disconnected"));
    assert!(
        stored["status_reason"]
            .as_str()
            .unwrap_or("")
            .contains("not available"),
        "the stored connector names the refusal: {stored}"
    );
    // A stored pending action never starts either.
    assert!(
        state
            .engine
            .connectors()
            .get(&broker_config::v5_action_sink_key(
                "mv03b-pending-stored-act"
            ))
            .is_none(),
        "a stored pending action does not start"
    );
    let (status, stored) = server
        .get_auth("/api/v5/actions/mv03b-pending-stored-act", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(stored["status"], json!("disconnected"));
    assert!(
        stored["status_reason"]
            .as_str()
            .unwrap_or("")
            .contains("not available"),
        "the stored action names the refusal: {stored}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv03b_pending_iot_kinds_refuse_at_probe_and_replay_stored_state() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Same driver bodies as the create-refusal test.
    let bodies = [
        (
            "aws_iot",
            json!({"endpoint": "127.0.0.1:8883", "region": "us-east-1",
                       "client_id": "pending-1",
                       "client_cert_pem": "-----BEGIN CERTIFICATE-----\ncert\n-----END CERTIFICATE-----",
                       "client_key_pem": "-----BEGIN PRIVATE KEY-----\nkey\n-----END PRIVATE KEY-----"}),
        ),
        (
            "azure_iot",
            json!({"iot_hub_name": "127.0.0.1:8883", "device_id": "edge-1",
                       "shared_access_key": "secret"}),
        ),
        (
            "oci_streaming",
            json!({"endpoint": "https://cell-1.streaming.us-east-1.oraclecloud.com",
                       "stream_pool_id": "ocid1.streampool.oc1.phx.aaa",
                       "stream_id": "ocid1.stream.oc1.phx.aaa",
                       "tenancy_ocid": "ocid1.tenancy.oc1..aaa",
                       "user_ocid": "ocid1.user.oc1..aaa",
                       "fingerprint": "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99",
                       "private_key_pem": "key"}),
        ),
    ];
    for (kind, fields) in &bodies {
        let mut body = fields.as_object().expect("object").clone();
        body.insert("type".to_string(), json!(kind));
        body.insert(
            "name".to_string(),
            json!(format!("mv03b-pending-probe-{kind}")),
        );
        // Each probe entry refuses before any dial.
        for path in ["/api/v5/connectors_probe", "/api/v5/actions_probe"] {
            let (status, out) = server
                .post_auth(path, Value::Object(body.clone()), &token)
                .await;
            assert_eq!(status, 400, "{kind} probe {path} refuses: {out}");
            assert_eq!(
                out["code"], "TEST_FAILED",
                "{kind} probe {path} code: {out}"
            );
            let message = out["message"].as_str().unwrap_or("");
            assert!(
                message.contains("not available"),
                "{kind} probe {path} names the refusal: {out}"
            );
            assert!(
                message.contains(kind),
                "{kind} probe {path} names the kind: {out}"
            );
        }
    }
    // A file load that carries a pending kind refuses.
    for (kind, _) in &bodies {
        let mut candidate = state.config.snapshot().as_ref().clone();
        candidate
            .connectors
            .connectors
            .push(broker_config::ConnectorEntry {
                id: format!("mv03b-pending-file-{kind}"),
                connector_type: kind.to_string(),
                enable: true,
                config: "{}".to_string(),
            });
        let (status, out) = server
            .post_auth(
                "/api/v1/config/reload",
                json!({"snapshot": candidate, "actor": "mv03b", "summary": "refuse pending iot"}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "a file load refuses {kind}: {out}");
        let text = out.to_string();
        assert!(
            text.contains("not available"),
            "{kind} file load names the refusal: {out}"
        );
        assert!(
            text.contains(kind),
            "{kind} file load names the kind: {out}"
        );
    }
}
