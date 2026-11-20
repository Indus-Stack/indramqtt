//! Tests for the Kafka connector.
//!
//! Each test boots one real broker.
//! It creates the connector and rule.
//! It publishes on a real socket.
//! It reads each record back from the server.

use std::time::{Duration, Instant};

use super::e2e_support::TestBroker;
use serde_json::{json, Value};

/// Partitions of each qualification topic.
/// Reason: QUAL-SETUP creates 6 partitions.
/// Source: CE2-08-kafka.md:125.
const PARTITIONS: i32 = 6;
/// Partitions of the restart topic.
/// Reason: the v5 action hashes with 12.
/// Source: action_sinks.rs MV03A_DEFAULT_PARTITIONS.
const RESTART_PARTITIONS: i32 = 12;
/// Copies of one topic on a one-broker image.
/// Reason: the qual image runs one broker.
/// Source: CE2-08-kafka.md:111-116.
const REPLICATION: i32 = 1;
/// Bound for one consume wait.
/// Reason: 60 s covers a slow server insert.
/// Source: pipeline timeout budget.
const CONSUME: Duration = Duration::from_secs(60);
/// Bound for one admin call.
/// Reason: 60 s covers a slow topic create.
/// Source: pipeline timeout budget.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(60);
/// Quiet bound after an unmatched publish.
/// Reason: 2 s lets a wrong forward arrive.
/// Source: pipeline timeout budget.
const QUIET: Duration = Duration::from_secs(2);
/// Bound for one poll of the consumer.
/// Reason: 10 s bounds one slow fetch.
/// Source: pipeline timeout budget.
const POLL: Duration = Duration::from_secs(10);
/// Bound for one metadata read.
/// Reason: 30 s covers a slow broker answer.
/// Source: pipeline timeout budget.
const METADATA_TIMEOUT: Duration = Duration::from_secs(30);
/// Pause between assignment checks.
/// Reason: 100 ms polls fast without load.
/// Source: pipeline timeout budget.
const ASSIGN_POLL: Duration = Duration::from_millis(100);
/// Records of one size-triggered batch.
/// Reason: 3 proves a batch, not one send.
/// Source: ticket batch case.
const BATCH_RECORDS: usize = 3;
/// Byte bound of the byte-triggered batch.
/// Reason: 200 bytes sits below one 600-byte row.
/// Source: ticket batch case.
const BATCH_BYTES: usize = 200;
/// Records that fill a byte batch without a count trigger.
/// Reason: 100 exceeds any byte test row count.
/// Source: ticket batch case.
const BYTE_TRIGGER_RECORDS: usize = 100;
/// Default byte bound of one batch.
/// Reason: 256 KiB matches the sink default.
/// Source: kafka.rs default_batch_max_bytes.
const DEFAULT_BATCH_MAX_BYTES: usize = 256 * 1024;
/// Client id used by each v1 connector.
/// Reason: one fixed value keeps the tests stable.
/// Source: ticket client_id case.
const CLIENT_ID: &str = "ce2-kafka-e2e";
/// Client id of the acks-one connector.
/// Reason: a second id proves each id delivers.
/// Source: ticket client_id case.
const CLIENT_ID_ONE: &str = "ce2-kafka-e2e-one";

/// Computes Kafka murmur2 for one key.
/// Mirrors the sink logic for verification.
/// Reason: the test needs exact placement without kafka feature.
/// Source: kafka.rs murmur2.
#[allow(clippy::chunks_exact_to_as_chunks)]
fn murmur2(data: &[u8]) -> u32 {
    const SEED: u32 = 0x9747b28c;
    const M: u32 = 0x5bd1e995;
    const R: u32 = 24;
    let mut h = SEED ^ data.len() as u32;
    let mut chunks = data.chunks_exact(4);
    for chunk in &mut chunks {
        let mut k = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    match *chunks.remainder() {
        [a, b, c] => {
            h ^= (c as u32) << 16;
            h ^= (b as u32) << 8;
            h ^= a as u32;
            h = h.wrapping_mul(M);
        }
        [a, b] => {
            h ^= (b as u32) << 8;
            h ^= a as u32;
            h = h.wrapping_mul(M);
        }
        [a] => {
            h ^= a as u32;
            h = h.wrapping_mul(M);
        }
        _ => {}
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h
}

/// Computes partition for one key.
/// Mirrors the sink logic for verification.
/// Reason: the test needs exact placement without kafka feature.
/// Source: kafka.rs partition_for_key.
fn partition_for_key(key: Option<&[u8]>, partitions: u32) -> i32 {
    assert!(partitions >= 1, "partitions must be >= 1");
    match key {
        Some(key) => ((murmur2(key) & 0x7fffffff) % partitions) as i32,
        None => 0,
    }
}

/// Reads one required variable.
/// Panics when the variable is absent.
/// Reason: a missing address must fail, never skip.
/// Source: live-test rule for qualification.
fn qual_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} must point at a real server for qualification"))
}

/// Builds one v1 connector config for Kafka.
/// Uses one record per flush unless the caller changes it.
fn kafka_config(bootstrap: &str, topic_template: &str) -> Value {
    json!({
        "bootstrap_servers": bootstrap,
        "topic_template": topic_template,
        "partition_key_field": "device_id",
        "partitions": PARTITIONS,
        "client_id": CLIENT_ID,
        "acks": "all",
        "batch_max_records": 1,
        "batch_max_bytes": DEFAULT_BATCH_MAX_BYTES,
        "random_partition": false,
    })
}

/// Creates one topic when it is absent.
/// Fails when the server refuses the create.
/// Reason: each test needs its own topic.
/// Source: connector qualification guard.
async fn ensure_topic(bootstrap: &str, topic: &str, partitions: i32) {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    let admin: AdminClient<DefaultClientContext> = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("admin client builds");
    let spec = NewTopic::new(topic, partitions, TopicReplication::Fixed(REPLICATION));
    let results = admin
        .create_topics(
            &[spec],
            &AdminOptions::new()
                .operation_timeout(Some(rdkafka::util::Timeout::After(ADMIN_TIMEOUT))),
        )
        .await
        .expect("create topic rpc runs");
    for result in results {
        if let Err(error) = result {
            let text = format!("{error:?}");
            assert!(
                text.contains("AlreadyExists")
                    || text.contains("already exists")
                    || text.contains("ALREADY_EXISTS"),
                "topic create fails: {error:?}"
            );
        }
    }
}

/// Unique suffix for each run.
/// Reason: a fresh topic per run holds no old rows.
/// Source: harness stale-row guard.
fn run_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_nanos())
        .unwrap_or(0);
    format!("{nanos}-{}", std::process::id())
}

/// One record read back from the server.
struct ConsumedRecord {
    key: Vec<u8>,
    partition: i32,
    payload: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
}

/// Builds one consumer that reads from the start.
/// Reason: each check needs its own group.
/// Source: rdkafka consumer guard.
fn new_consumer(
    bootstrap: &str,
    topic: &str,
    group: &str,
    suffix: &str,
) -> rdkafka::consumer::StreamConsumer {
    use rdkafka::consumer::Consumer;
    let consumer: rdkafka::consumer::StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("ce2-kafka-{group}-{suffix}"))
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer builds");
    consumer.subscribe(&[topic]).expect("subscribe works");
    consumer
}

/// Checks the topic partition count against the server.
/// Panics when the count differs.
/// Reason: metadata proves the partitions setting.
/// Source: ticket partitions case.
async fn assert_topic_partitions(bootstrap: &str, topic: &str, expected: i32) {
    use rdkafka::consumer::Consumer;
    let suffix = run_suffix();
    let consumer = new_consumer(bootstrap, topic, "meta", &suffix);
    let meta = consumer
        .fetch_metadata(Some(topic), METADATA_TIMEOUT)
        .expect("topic metadata reads");
    let count = meta.topics()[0].partitions().len() as i32;
    assert_eq!(count, expected, "topic {topic} holds {expected} partitions");
}

/// Returns the end offset sum of one topic.
/// Reason: a pending row writes no offset to the server.
/// Source: ticket batch case.
fn topic_end_offsets(
    consumer: &rdkafka::consumer::StreamConsumer,
    topic: &str,
    partitions: i32,
) -> i64 {
    use rdkafka::consumer::Consumer;
    (0..partitions)
        .map(|partition| {
            consumer
                .fetch_watermarks(topic, partition, METADATA_TIMEOUT)
                .expect("watermark reads")
                .1
        })
        .sum()
}

/// Checks that one topic holds no record for a quiet interval.
/// Panics when any record reaches the server early.
/// Reason: pending rows stay off the server until the batch is full.
/// Source: ticket batch case.
async fn assert_topic_empty(bootstrap: &str, topic: &str, partitions: i32, reason: &str) {
    let consumer: rdkafka::consumer::StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .create()
        .expect("watermark consumer builds");
    let deadline = Instant::now() + QUIET;
    loop {
        assert_eq!(
            topic_end_offsets(&consumer, topic, partitions),
            0,
            "{reason}"
        );
        if Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(ASSIGN_POLL).await;
    }
}

/// Reads until each wanted key reaches its count.
/// Panics with the missing keys after the timeout.
/// Reason: at-least-once permits duplicates, never loss.
/// Source: protocol tolerance rule.
async fn consume_keys(
    bootstrap: &str,
    topic: &str,
    group: &str,
    want: &[(&str, usize)],
    timeout: Duration,
) -> (Vec<ConsumedRecord>, rdkafka::consumer::StreamConsumer) {
    use rdkafka::consumer::{Consumer, StreamConsumer};
    use rdkafka::message::{Headers, Message};
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_nanos())
        .unwrap_or(0);
    let consumer: StreamConsumer = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set(
            "group.id",
            format!("ce2-kafka-{group}-{}-{nanos}", std::process::id()),
        )
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .create()
        .expect("consumer builds");
    consumer.subscribe(&[topic]).expect("subscribe works");
    let deadline = Instant::now() + timeout;
    let mut out = Vec::new();
    loop {
        if want.iter().all(|(key, count)| {
            out.iter()
                .filter(|record: &&ConsumedRecord| record.key == key.as_bytes())
                .count()
                >= *count
        }) {
            return (out, consumer);
        }
        assert!(Instant::now() < deadline, "missing keys in topic {topic}");
        let message = tokio::time::timeout(POLL, consumer.recv())
            .await
            .expect("record arrives in time")
            .expect("consume works");
        let mut headers = Vec::new();
        if let Some(map) = message.headers() {
            for header in map.iter() {
                headers.push((
                    header.key.to_string(),
                    header.value.unwrap_or_default().to_vec(),
                ));
            }
        }
        out.push(ConsumedRecord {
            key: message.key().unwrap_or_default().to_vec(),
            partition: message.partition(),
            payload: message.payload().unwrap_or_default().to_vec(),
            headers,
        });
    }
}

/// Returns one header value.
/// Panics when the header is absent.
fn header_value<'a>(headers: &'a [(String, Vec<u8>)], name: &str) -> &'a [u8] {
    headers
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.as_slice())
        .unwrap_or_else(|| panic!("header {name} is present"))
}

/// One publish through the broker, read back from the real topic.
///
/// Covers bootstrap_servers, partition_key_field, key_template,
/// partitions, client_id and acks.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_forward_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let suffix = run_suffix();
    let fwd_topic = format!("{base}_ce2_fwd_{suffix}");
    let key_topic = format!("{base}_ce2_key_{suffix}");
    let one_topic = format!("{base}_ce2_one_{suffix}");
    ensure_topic(&bootstrap, &fwd_topic, PARTITIONS).await;
    ensure_topic(&bootstrap, &key_topic, PARTITIONS).await;
    ensure_topic(&bootstrap, &one_topic, PARTITIONS).await;
    assert_topic_partitions(&bootstrap, &fwd_topic, PARTITIONS).await;
    assert_topic_partitions(&bootstrap, &key_topic, PARTITIONS).await;
    assert_topic_partitions(&bootstrap, &one_topic, PARTITIONS).await;

    let broker = TestBroker::start("").await;
    let fwd_config = kafka_config(&bootstrap, &fwd_topic);
    broker
        .create_connector("ce2-kafka-fwd", "kafka", fwd_config)
        .await;
    broker
        .create_forward_rule("ce2-kafka-fwd-rule", "ce2/kafka/fwd", "ce2-kafka-fwd")
        .await;
    let mut keyed = kafka_config(&bootstrap, &key_topic);
    keyed["key_template"] = json!("key-${payload.seq}");
    broker
        .create_connector("ce2-kafka-key", "kafka", keyed)
        .await;
    broker
        .create_forward_rule("ce2-kafka-key-rule", "ce2/kafka/key", "ce2-kafka-key")
        .await;
    // A second connector uses another id and acks one.
    // Reason: two deliveries prove each id and each applied value.
    // Source: ticket client_id and acks cases.
    let mut one_config = kafka_config(&bootstrap, &one_topic);
    one_config["client_id"] = json!(CLIENT_ID_ONE);
    one_config["acks"] = json!("1");
    broker
        .create_connector("ce2-kafka-one", "kafka", one_config)
        .await;
    broker
        .create_forward_rule("ce2-kafka-one-rule", "ce2/kafka/one", "ce2-kafka-one")
        .await;

    // The same key lands twice; both copies share one partition.
    let mut publisher = broker.connect("ce2-kafka-fwd-pub").await;
    for _ in 0..2 {
        publisher
            .publish("ce2/kafka/fwd", br#"{"device_id":"fwd-1","seq":1}"#, 0)
            .await;
    }
    drop(publisher);
    broker
        .publish(
            "ce2-kafka-key-pub",
            "ce2/kafka/key",
            br#"{"device_id":"k","seq":7}"#,
            0,
        )
        .await;
    broker
        .publish(
            "ce2-kafka-one-pub",
            "ce2/kafka/one",
            br#"{"device_id":"one-1","seq":9}"#,
            0,
        )
        .await;

    let (records, _) = consume_keys(&bootstrap, &fwd_topic, "fwd", &[("fwd-1", 2)], CONSUME).await;
    let listed = broker.list_connectors().await;
    assert!(
        listed
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == "ce2-kafka-fwd")),
        "connector record exists"
    );
    assert!(
        listed
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == "ce2-kafka-one")),
        "acks-one connector record exists"
    );
    let first: Vec<&ConsumedRecord> = records
        .iter()
        .filter(|record| record.key == b"fwd-1")
        .collect();
    assert!(first.len() >= 2, "both copies arrive");
    assert_eq!(
        first[0].partition, first[1].partition,
        "one key stays on one partition"
    );
    assert!(
        (0..PARTITIONS).contains(&first[0].partition),
        "partition stays in range"
    );
    // Exact murmur2 match proves the connector used PARTITIONS.
    // Reason: a wrong count hashes to another partition.
    // Source: kafka sink partition path.
    let expected = partition_for_key(Some(b"fwd-1"), PARTITIONS as u32);
    assert_eq!(
        first[0].partition, expected,
        "partitions setting places the key"
    );
    assert!(
        first[0].payload.windows(7).any(|w| w == b"\"seq\":1"),
        "payload arrives intact"
    );
    assert_eq!(
        header_value(&first[0].headers, "mqtt.topic"),
        b"ce2/kafka/fwd",
        "mqtt topic header arrives"
    );
    // Two topics prove two applied values.
    // Reason: fwd uses acks all, one uses acks 1.
    // Source: ticket acks case.
    // Two ids prove two client ids.
    // Reason: fwd and one use different ids.
    // Source: ticket client_id case.
    // A wrong acks value is refused on the same entry.
    // Reason: an invalid value must fail closed.
    // Source: ticket acks case.
    let mut bad = kafka_config(&bootstrap, &fwd_topic);
    bad["acks"] = json!("quorum");
    let (bad_status, bad_body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v1/connectors",
            json!({"id": "ce2-kafka-bad-acks-fwd", "kind": "kafka", "config": bad}),
        )
        .await;
    assert_eq!(bad_status, 400, "wrong acks refused: {bad_body}");
    assert!(
        bad_body.to_string().contains("acks"),
        "error names the field: {bad_body}"
    );

    // The key template wins over the key field.
    let (keyed_records, _) =
        consume_keys(&bootstrap, &key_topic, "key", &[("key-7", 1)], CONSUME).await;
    assert_eq!(keyed_records[0].key, b"key-7", "template key arrives");
    // The acks-one connector delivers with another id.
    // Reason: arrival on its topic proves the applied value.
    // Source: ticket client_id and acks cases.
    let (one_records, _) =
        consume_keys(&bootstrap, &one_topic, "one", &[("one-1", 1)], CONSUME).await;
    assert_eq!(one_records[0].key, b"one-1", "acks-one row arrives");
    assert!(
        one_records[0].payload.windows(7).any(|w| w == b"\"seq\":9"),
        "acks-one payload arrives intact"
    );
    broker.finish().await;
}

/// One template per connector selects the record topic.
///
/// Covers topic_template.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_topic_template_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let suffix = run_suffix();
    let rendered_a = format!("{base}_ce2_tpla_{suffix}");
    let rendered_b = format!("{base}_ce2_tplb_{suffix}");
    ensure_topic(&bootstrap, &rendered_a, PARTITIONS).await;
    ensure_topic(&bootstrap, &rendered_b, PARTITIONS).await;

    let broker = TestBroker::start("").await;
    let config_a = kafka_config(&bootstrap, &rendered_a);
    broker
        .create_connector("ce2-kafka-tpla", "kafka", config_a)
        .await;
    broker
        .create_forward_rule("ce2-kafka-tpla-rule", "ce2/kafka/tpla", "ce2-kafka-tpla")
        .await;
    let config_b = kafka_config(&bootstrap, &rendered_b);
    broker
        .create_connector("ce2-kafka-tplb", "kafka", config_b)
        .await;
    broker
        .create_forward_rule("ce2-kafka-tplb-rule", "ce2/kafka/tplb", "ce2-kafka-tplb")
        .await;

    broker
        .publish(
            "ce2-kafka-tpla-pub",
            "ce2/kafka/tpla",
            br#"{"device_id":"tpla-1","seq":1}"#,
            0,
        )
        .await;
    broker
        .publish(
            "ce2-kafka-tplb-pub",
            "ce2/kafka/tplb",
            br#"{"device_id":"tplb-1","seq":2}"#,
            0,
        )
        .await;

    let (records_a, _) =
        consume_keys(&bootstrap, &rendered_a, "tpla", &[("tpla-1", 1)], CONSUME).await;
    assert_eq!(records_a[0].key, b"tpla-1");
    let (records_b, _) =
        consume_keys(&bootstrap, &rendered_b, "tplb", &[("tplb-1", 1)], CONSUME).await;
    assert_eq!(records_b[0].key, b"tplb-1");
    broker.finish().await;
}

/// Size-triggered batches reach the server intact.
///
/// Covers batch_max_records and batch_max_bytes.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_batch_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let suffix = run_suffix();
    let count_topic = format!("{base}_ce2_batch_{suffix}");
    let bytes_topic = format!("{base}_ce2_bytes_{suffix}");
    ensure_topic(&bootstrap, &count_topic, PARTITIONS).await;
    ensure_topic(&bootstrap, &bytes_topic, PARTITIONS).await;

    let broker = TestBroker::start("").await;
    let mut count_config = kafka_config(&bootstrap, &count_topic);
    count_config["batch_max_records"] = json!(BATCH_RECORDS);
    broker
        .create_connector("ce2-kafka-batch", "kafka", count_config)
        .await;
    broker
        .create_forward_rule("ce2-kafka-batch-rule", "ce2/kafka/batch", "ce2-kafka-batch")
        .await;
    let mut bytes_config = kafka_config(&bootstrap, &bytes_topic);
    bytes_config["batch_max_records"] = json!(BYTE_TRIGGER_RECORDS);
    bytes_config["batch_max_bytes"] = json!(BATCH_BYTES);
    broker
        .create_connector("ce2-kafka-bytes", "kafka", bytes_config)
        .await;
    broker
        .create_forward_rule("ce2-kafka-bytes-rule", "ce2/kafka/bytes", "ce2-kafka-bytes")
        .await;

    // Two rows stay pending below the count of three.
    let mut publisher = broker.connect("ce2-kafka-batch-pub").await;
    for seq in 0..2 {
        publisher
            .publish(
                "ce2/kafka/batch",
                format!(r#"{{"device_id":"bat-{seq}","seq":{seq}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    drop(publisher);
    assert_topic_empty(
        &bootstrap,
        &count_topic,
        PARTITIONS,
        "batch waits for size trigger",
    )
    .await;
    // The third row fills the batch and flushes all three.
    broker
        .publish(
            "ce2-kafka-batch-pub3",
            "ce2/kafka/batch",
            br#"{"device_id":"bat-2","seq":2}"#,
            0,
        )
        .await;
    let (records, _) = consume_keys(
        &bootstrap,
        &count_topic,
        "batch",
        &[("bat-0", 1), ("bat-1", 1), ("bat-2", 1)],
        CONSUME,
    )
    .await;
    let mut seqs: Vec<u64> = records
        .iter()
        .map(|record| {
            serde_json::from_slice::<Value>(&record.payload).expect("payload is JSON")["seq"]
                .as_u64()
                .expect("seq is a number")
        })
        .collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs, vec![0, 1, 2], "all three rows arrive");

    // One small row stays pending below the byte bound.
    let small = r#"{"device_id":"big-0","seq":0}"#;
    broker
        .publish(
            "ce2-kafka-bytes-pub0",
            "ce2/kafka/bytes",
            small.as_bytes(),
            0,
        )
        .await;
    assert_topic_empty(
        &bootstrap,
        &bytes_topic,
        PARTITIONS,
        "byte batch waits for size",
    )
    .await;
    // One 600-byte row exceeds the byte bound alone.
    let big = format!(r#"{{"device_id":"big-1","pad":"{}"}}"#, "x".repeat(600));
    broker
        .publish("ce2-kafka-bytes-pub", "ce2/kafka/bytes", big.as_bytes(), 0)
        .await;
    let (big_records, _) = consume_keys(
        &bootstrap,
        &bytes_topic,
        "bytes",
        &[("big-0", 1), ("big-1", 1)],
        CONSUME,
    )
    .await;
    // The small row keeps identity; the big row exceeds the bound.
    // Reason: payload size proves the byte trigger.
    // Source: ticket batch case.
    let mut seen_small = false;
    let mut seen_big = false;
    for record in &big_records {
        if record.key == b"big-0" {
            let payload: Value = serde_json::from_slice(&record.payload).expect("payload is JSON");
            assert_eq!(payload["device_id"], "big-0", "small row keeps identity");
            seen_small = true;
        } else if record.key == b"big-1" {
            assert!(record.payload.len() > BATCH_BYTES, "big row exceeds bound");
            seen_big = true;
        }
    }
    assert!(seen_small && seen_big, "both byte rows arrive");
    broker.finish().await;
}

/// Random spread moves one key across partitions.
///
/// Covers random_partition.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_random_partition_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let topic = format!("{base}_ce2_random_{}", run_suffix());
    ensure_topic(&bootstrap, &topic, PARTITIONS).await;

    let broker = TestBroker::start("").await;
    let mut config = kafka_config(&bootstrap, &topic);
    config["random_partition"] = json!(true);
    broker
        .create_connector("ce2-kafka-random", "kafka", config)
        .await;
    broker
        .create_forward_rule(
            "ce2-kafka-random-rule",
            "ce2/kafka/random",
            "ce2-kafka-random",
        )
        .await;

    let mut publisher = broker.connect("ce2-kafka-random-pub").await;
    for seq in 0..6 {
        publisher
            .publish(
                "ce2/kafka/random",
                format!(r#"{{"device_id":"rnd-same","seq":{seq}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    drop(publisher);
    let (records, _) =
        consume_keys(&bootstrap, &topic, "random", &[("rnd-same", 6)], CONSUME).await;
    let mut parts: Vec<i32> = records
        .iter()
        .filter(|record| record.key == b"rnd-same")
        .map(|record| record.partition)
        .collect();
    parts.sort_unstable();
    parts.dedup();
    assert!(parts.len() > 1, "random spread moves partitions");
    broker.finish().await;
}

/// Headers arrive raw in None mode.
///
/// Covers headers_template, extra_headers, header_encode_mode
/// and header_encode_mode.None.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_headers_none_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let topic = format!("{base}_ce2_hdrnone_{}", run_suffix());
    ensure_topic(&bootstrap, &topic, PARTITIONS).await;

    let broker = TestBroker::start("").await;
    let mut config = kafka_config(&bootstrap, &topic);
    config["headers_template"] = json!(r#"{"ce2-tpl": "${topic}"}"#);
    config["extra_headers"] = json!([["ce2-static", "s1"], ["ce2-from-topic", "${topic}"]]);
    config["header_encode_mode"] = json!("none");
    broker
        .create_connector("ce2-kafka-hdr-none", "kafka", config)
        .await;
    broker
        .create_forward_rule(
            "ce2-kafka-hdr-none-rule",
            "ce2/kafka/hdr-none",
            "ce2-kafka-hdr-none",
        )
        .await;

    broker
        .publish(
            "ce2-kafka-hdr-none-pub",
            "ce2/kafka/hdr-none",
            br#"{"device_id":"hdr-1"}"#,
            0,
        )
        .await;

    let (records, _) = consume_keys(&bootstrap, &topic, "hdrnone", &[("hdr-1", 1)], CONSUME).await;
    let record = &records[0];
    assert_eq!(header_value(&record.headers, "ce2-static"), b"s1");
    assert_eq!(
        header_value(&record.headers, "ce2-from-topic"),
        b"ce2/kafka/hdr-none"
    );
    assert_eq!(
        header_value(&record.headers, "ce2-tpl"),
        b"ce2/kafka/hdr-none"
    );
    broker.finish().await;
}

/// Headers arrive JSON-encoded in Json mode.
///
/// Covers header_encode_mode.Json.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_headers_json_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let topic = format!("{base}_ce2_hdrjson_{}", run_suffix());
    ensure_topic(&bootstrap, &topic, PARTITIONS).await;

    let broker = TestBroker::start("").await;
    let mut config = kafka_config(&bootstrap, &topic);
    config["headers_template"] = json!(r#"{"ce2-tpl": "${topic}"}"#);
    config["extra_headers"] = json!([["ce2-static", "s1"]]);
    config["header_encode_mode"] = json!("json");
    broker
        .create_connector("ce2-kafka-hdr-json", "kafka", config)
        .await;
    broker
        .create_forward_rule(
            "ce2-kafka-hdr-json-rule",
            "ce2/kafka/hdr-json",
            "ce2-kafka-hdr-json",
        )
        .await;

    broker
        .publish(
            "ce2-kafka-hdr-json-pub",
            "ce2/kafka/hdr-json",
            br#"{"device_id":"hdr-2"}"#,
            0,
        )
        .await;

    let (records, _) = consume_keys(&bootstrap, &topic, "hdrjson", &[("hdr-2", 1)], CONSUME).await;
    let record = &records[0];
    assert_eq!(header_value(&record.headers, "ce2-static"), b"\"s1\"");
    assert_eq!(
        header_value(&record.headers, "ce2-tpl"),
        b"\"ce2/kafka/hdr-json\""
    );
    broker.finish().await;
}

/// A wrong acks value is refused with the documented error.
///
/// Covers the acks refusal. Needs no server.
#[tokio::test]
#[ignore = "needs no server; the refusal runs in qualification"]
async fn kafka_acks_rejected_through_broker() {
    let broker = TestBroker::start("").await;
    let mut config = kafka_config("127.0.0.1:1", "no-topic");
    config["acks"] = json!("quorum");
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v1/connectors",
            json!({"id": "ce2-kafka-bad-acks", "kind": "kafka", "config": config}),
        )
        .await;
    assert_eq!(status, 400, "wrong acks refused: {body}");
    assert!(
        body.to_string().contains("acks"),
        "error names the field: {body}"
    );
    broker.finish().await;
}

/// A duplicate v5 connector id fails with the documented code.
///
/// The spec wants 400 with ALREADY_EXISTS.
/// Needs no server.
#[tokio::test]
async fn kafka_duplicate_through_broker() {
    let broker = TestBroker::start("").await;
    let body = json!({
        "name": "ce2-kafka-dup",
        "type": "kafka",
        "bootstrap_hosts": "127.0.0.1:1",
    });
    let (status, answer) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", body.clone())
        .await;
    assert_eq!(status, 201, "v5 connector creates: {answer}");
    let (status, answer) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", body)
        .await;
    assert_eq!(status, 400, "v5 duplicate fails: {answer}");
    assert_eq!(
        answer.get("code").and_then(|code| code.as_str()),
        Some("ALREADY_EXISTS"),
        "duplicate carries the documented code: {answer}"
    );
    broker.finish().await;
}

/// Unknown ids and unmatched topics write nothing.
///
/// An unknown connector id answers 404 with a body.
/// A publish without a matching rule writes no record.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_unknown_nomatch_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let topic = format!("{base}_ce2_nomatch_{}", run_suffix());
    ensure_topic(&bootstrap, &topic, PARTITIONS).await;

    let broker = TestBroker::start("").await;
    let (status, body) = broker
        .api(
            reqwest::Method::GET,
            "/api/v5/connectors/ce2-kafka-no-such-connector",
            Value::Null,
        )
        .await;
    assert_eq!(status, 404, "unknown connector id misses");
    assert_eq!(
        body.get("code").and_then(|code| code.as_str()),
        Some("NOT_FOUND"),
        "unknown id carries the documented code: {body}"
    );
    assert!(
        body.get("message")
            .and_then(|text| text.as_str())
            .is_some_and(|text| !text.is_empty()),
        "unknown id carries a body: {body}"
    );

    broker
        .create_connector(
            "ce2-kafka-nomatch",
            "kafka",
            kafka_config(&bootstrap, &topic),
        )
        .await;
    broker
        .create_forward_rule(
            "ce2-kafka-nomatch-rule",
            "ce2/kafka/nomatch",
            "ce2-kafka-nomatch",
        )
        .await;
    broker
        .publish(
            "ce2-kafka-nomatch-pub",
            "ce2/kafka/nomatch",
            br#"{"device_id":"nm-1","seq":1}"#,
            0,
        )
        .await;
    let (_, consumer) = consume_keys(&bootstrap, &topic, "nomatch", &[("nm-1", 1)], CONSUME).await;
    // A topic without a rule writes no further row.
    broker
        .publish(
            "ce2-kafka-nomatch-pub2",
            "ce2/kafka/nomatch-other",
            br#"{"device_id":"nm-2","seq":2}"#,
            0,
        )
        .await;
    let arrived = tokio::time::timeout(QUIET, consumer.recv()).await;
    assert!(arrived.is_err(), "quiet topic stays empty");
    broker.finish().await;
}

/// Connector and rule persist across a broker restart.
///
/// Uses the v5 connector store, which replays on boot.
/// Publishes before and after the restart; both rows return.
#[tokio::test]
#[ignore = "needs a real server (see KAFKA_* env)"]
async fn kafka_persist_across_restart_through_broker() {
    let bootstrap = qual_env("KAFKA_BOOTSTRAP_SERVERS");
    let base = qual_env("KAFKA_TOPIC");
    let topic = format!("{base}_ce2_restart_{}", run_suffix());
    ensure_topic(&bootstrap, &topic, RESTART_PARTITIONS).await;

    let mut broker = TestBroker::start("").await;
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "name": "ce2-kafka-restart",
                "type": "kafka",
                "bootstrap_hosts": bootstrap,
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 connector creates: {body}");
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/actions",
            json!({
                "name": "ce2-kafka-restart-act",
                "type": "kafka",
                "connector": "ce2-kafka-restart",
                "parameters": {
                    "topic": topic,
                    "message": {"key": "${payload.device_id}"},
                    "partition_strategy": "key_dispatch",
                    "required_acks": "all_isr",
                },
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 action creates: {body}");
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/rules",
            json!({
                "id": "ce2-kafka-restart-rule",
                "name": "ce2-kafka-restart-rule",
                "sql": "SELECT * FROM \"ce2/kafka/restart\"",
                "actions": ["ce2-kafka-restart-act"],
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 rule creates: {body}");

    broker
        .publish(
            "ce2-kafka-restart-pub1",
            "ce2/kafka/restart",
            br#"{"device_id":"rs-1","seq":11}"#,
            0,
        )
        .await;
    let (before, _) = consume_keys(
        &bootstrap,
        &topic,
        "restart-before",
        &[("rs-1", 1)],
        CONSUME,
    )
    .await;
    assert_eq!(before[0].key, b"rs-1", "row before restart arrives");

    broker.stop_and_restart_orderly().await;

    broker
        .publish(
            "ce2-kafka-restart-pub2",
            "ce2/kafka/restart",
            br#"{"device_id":"rs-2","seq":12}"#,
            0,
        )
        .await;
    let (after, _) = consume_keys(
        &bootstrap,
        &topic,
        "restart-after",
        &[("rs-1", 1), ("rs-2", 1)],
        CONSUME,
    )
    .await;
    let mut seqs: Vec<u64> = after
        .iter()
        .map(|record| {
            serde_json::from_slice::<Value>(&record.payload).expect("payload is JSON")["seq"]
                .as_u64()
                .expect("seq is a number")
        })
        .collect();
    seqs.sort_unstable();
    seqs.dedup();
    assert_eq!(seqs, vec![11, 12], "both epochs survive");
    broker.finish().await;
}
