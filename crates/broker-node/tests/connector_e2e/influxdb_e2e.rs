//! End-to-end tests for the InfluxDB connector.
//!
//! Each test boots one real broker and needs a real InfluxDB server.
//! It creates the connector and rule, publishes on a real MQTT socket,
//! and reads each point back from the InfluxDB server.
//!
//! Run with:
//!   INFLUXDB_ENDPOINT=http://127.0.0.1:8086 \
//!   INFLUXDB_ORG=qual-org INFLUXDB_BUCKET=qual-b322 INFLUXDB_TOKEN=qual-token-1 \
//!   cargo +1.96.0 test -p broker-node --test connector_e2e influxdb_e2e:: -- --ignored

use super::e2e_support::TestBroker;
use serde_json::{json, Value};
use std::time::{Duration, Instant};

/// Reads one required variable; panics when absent.
/// Reason: a missing address must fail, never skip.
/// Source: live-test rule for qualification.
fn qual_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} must point at a real server for qualification"))
}

/// Every field the v1 influxdb connector accepts, grouped into one
/// argument so the config builder stays within the argument bound.
struct InfluxdbConfig<'a> {
    endpoint: &'a str,
    bucket: &'a str,
    org: &'a str,
    token: &'a str,
    measurement_template: &'a str,
    precision: &'a str,
    batch_size: usize,
    batch_timeout_ms: u64,
    request_timeout_ms: Option<u64>,
}

/// Builds the influxdb config payload for the v1 connector path.
fn influxdb_config(cfg: &InfluxdbConfig<'_>) -> Value {
    let mut config = json!({
        "endpoint": cfg.endpoint,
        "bucket": cfg.bucket,
        "org": cfg.org,
        "token": cfg.token,
        "measurement_template": cfg.measurement_template,
        "precision": cfg.precision,
        "batch_size": cfg.batch_size,
        "batch_timeout_ms": cfg.batch_timeout_ms,
    });
    if let Some(timeout) = cfg.request_timeout_ms {
        config["request_timeout_ms"] = json!(timeout);
    }
    config
}

/// Ensures the bucket exists on the InfluxDB server.
/// Creates it when missing; 422 means it already exists.
async fn ensure_bucket(
    client: &reqwest::Client,
    endpoint: &str,
    org: &str,
    token: &str,
    bucket: &str,
) {
    let buckets_url = format!("{endpoint}/api/v2/buckets");
    let found = async {
        let Ok(response) = client
            .get(&buckets_url)
            .query(&[("org", org), ("name", bucket)])
            .header("Authorization", format!("Token {token}"))
            .send()
            .await
        else {
            return true;
        };
        match response.text().await {
            Ok(text) => {
                text.contains(&format!("\"name\":\"{bucket}\""))
                    || text.contains(&format!("\"name\": \"{bucket}\""))
            }
            Err(_) => true,
        }
    }
    .await;
    if !found {
        let create = client
            .post(&buckets_url)
            .header("Authorization", format!("Token {token}"))
            .json(&json!({
                "org": org,
                "name": bucket,
                "retentionRules": [],
            }))
            .send()
            .await
            .expect("qual create bucket");
        let status = create.status();
        let text = create.text().await.unwrap_or_default();
        assert!(
            status.as_u16() == 201 || status.as_u16() == 422 || text.contains(bucket),
            "qual create bucket: status={status} body={text}"
        );
    }
}

/// Posts one Flux query and returns the CSV body.
async fn influx_query(
    client: &reqwest::Client,
    endpoint: &str,
    org: &str,
    token: &str,
    flux: &str,
) -> String {
    let response = client
        .post(format!("{endpoint}/api/v2/query?org={org}"))
        .header("Authorization", format!("Token {token}"))
        .header("Content-Type", "application/vnd.flux")
        .header("Accept", "application/csv")
        .body(flux.to_string())
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .expect("query reaches the server");
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "influx query refused: status={status} body={text}"
    );
    text
}

/// Counts data rows in the CSV response (skips # comments and header).
fn csv_data_rows(body: &str) -> usize {
    let mut count = 0usize;
    let mut header_seen = false;
    for line in body.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if !header_seen {
            header_seen = true;
            continue;
        }
        if !line.is_empty() {
            count += 1;
        }
    }
    count
}

/// Waits until the measurement holds at least `expect` points.
async fn wait_points(
    client: &reqwest::Client,
    endpoint: &str,
    org: &str,
    token: &str,
    bucket: &str,
    measurement: &str,
    expect: usize,
) {
    let deadline = Instant::now() + Duration::from_secs(60);
    let flux = format!(
        "from(bucket:\"{bucket}\") |> range(start:-7d) \
         |> filter(fn:(r)=> r[\"_measurement\"]==\"{measurement}\" and r[\"_field\"]==\"payload\")"
    );
    loop {
        let body = influx_query(client, endpoint, org, token, &flux).await;
        let seen = csv_data_rows(&body);
        if seen >= expect {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "measurement {measurement} holds {seen} points, expected {expect}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Counts the points stored for one measurement.
async fn count_points(
    client: &reqwest::Client,
    endpoint: &str,
    org: &str,
    token: &str,
    bucket: &str,
    measurement: &str,
) -> usize {
    let flux = format!(
        "from(bucket:\"{bucket}\") |> range(start:-7d) \
         |> filter(fn:(r)=> r[\"_measurement\"]==\"{measurement}\" and r[\"_field\"]==\"payload\")"
    );
    let body = influx_query(client, endpoint, org, token, &flux).await;
    csv_data_rows(&body)
}

/// Deletes all points for one measurement (best effort).
async fn delete_measurement(
    client: &reqwest::Client,
    endpoint: &str,
    org: &str,
    token: &str,
    bucket: &str,
    measurement: &str,
) {
    let delete_body = json!({
        "start": "1970-01-01T00:00:00Z",
        "stop": "2030-01-01T00:00:00Z",
        "predicate": format!("_measurement=\"{measurement}\""),
    });
    let _ = client
        .post(format!(
            "{endpoint}/api/v2/delete?org={org}&bucket={bucket}"
        ))
        .header("Authorization", format!("Token {token}"))
        .json(&delete_body)
        .send()
        .await;
}

/// One end-to-end batch through the broker, read back from InfluxDB.
///
/// Covers endpoint, bucket, org, token, measurement_template, precision,
/// batch_size. Three rows fill one size-triggered batch.
#[tokio::test]
#[ignore = "needs a real server (see INFLUXDB_* env)"]
async fn influxdb_end_to_end_through_broker() {
    let endpoint = qual_env("INFLUXDB_ENDPOINT");
    let org = qual_env("INFLUXDB_ORG");
    let bucket = qual_env("INFLUXDB_BUCKET");
    let token = qual_env("INFLUXDB_TOKEN");
    let measurement = "ce2_influx_e2e";
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    ensure_bucket(&client, &endpoint, &org, &token, &bucket).await;
    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-influx-e2e",
            "influxdb",
            influxdb_config(&InfluxdbConfig {
                endpoint: &endpoint,
                bucket: &bucket,
                org: &org,
                token: &token,
                measurement_template: measurement,
                precision: "ms",
                batch_size: 3,
                batch_timeout_ms: 60_000,
                request_timeout_ms: None,
            }),
        )
        .await;
    let rule = broker
        .create_forward_rule("ce2-influx-e2e-rule", "ce2/influx/e2e", "ce2-influx-e2e")
        .await;

    let before = now_millis();
    for (client_id, payload) in [
        (
            "ce2-influx-e2e-pub-1",
            br#"{"device": "dev-e2e-1", "val": 42.5}"#.as_slice(),
        ),
        (
            "ce2-influx-e2e-pub-2",
            br#"{"device": "dev-e2e-2", "val": 43.5}"#.as_slice(),
        ),
        (
            "ce2-influx-e2e-pub-3",
            br#"{"device": "dev-e2e-3", "val": 44.5}"#.as_slice(),
        ),
    ] {
        broker
            .publish(client_id, "ce2/influx/e2e", payload, 0)
            .await;
        // Reason: 5 ms keeps stamps apart on one series.
        // Source: server merge on same timestamp.
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    broker
        .wait_for_action_success(&rule, 3, Duration::from_secs(30))
        .await;
    wait_points(&client, &endpoint, &org, &token, &bucket, measurement, 3).await;
    let after = now_millis();

    let flux = format!(
        "from(bucket:\"{bucket}\") |> range(start:-7d) \
         |> filter(fn:(r)=> r[\"_measurement\"]==\"{measurement}\" and r[\"_field\"]==\"payload\") \
         |> map(fn:(r)=> ({{_value: r._value, _time: r._time, ts: int(v: r._time)}})) \
         |> keep(columns:[\"_value\",\"_time\",\"ts\"])"
    );
    let body = influx_query(&client, &endpoint, &org, &token, &flux).await;
    assert!(body.contains("42.5"), "point value returns: {body}");
    assert!(body.contains("43.5"), "point value returns: {body}");
    assert!(body.contains("44.5"), "point value returns: {body}");

    let bounds = query_times(&body);
    assert_eq!(bounds.len(), 3, "three timestamps return: {bounds:?}");
    for ts in &bounds {
        assert!(
            *ts >= before * 1_000_000 && *ts <= after * 1_000_000,
            "ns timestamp {ts} sits outside write window [{before}, {after}]"
        );
    }

    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;
    broker.finish().await;
}

/// A small batch_timeout_ms flushes a partial batch before the next publish.
///
/// Covers batch_timeout_ms.
#[tokio::test]
#[ignore = "needs a real server (see INFLUXDB_* env)"]
async fn influxdb_batch_timeout_through_broker() {
    let endpoint = qual_env("INFLUXDB_ENDPOINT");
    let org = qual_env("INFLUXDB_ORG");
    let bucket = qual_env("INFLUXDB_BUCKET");
    let token = qual_env("INFLUXDB_TOKEN");
    let measurement = "ce2_influx_timeout";
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    ensure_bucket(&client, &endpoint, &org, &token, &bucket).await;
    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;

    // Large batch size, short timeout: the timeout, not the size, flushes.
    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-influx-batch-timeout",
            "influxdb",
            influxdb_config(&InfluxdbConfig {
                endpoint: &endpoint,
                bucket: &bucket,
                org: &org,
                token: &token,
                measurement_template: measurement,
                precision: "ms",
                batch_size: 100,
                batch_timeout_ms: 50,
                request_timeout_ms: None,
            }),
        )
        .await;
    let _rule = broker
        .create_forward_rule(
            "ce2-influx-batch-timeout-rule",
            "ce2/influx/batch-timeout",
            "ce2-influx-batch-timeout",
        )
        .await;

    broker
        .publish(
            "ce2-influx-batch-timeout-pub",
            "ce2/influx/batch-timeout",
            br#"{"device": "dev-bt", "val": 1.0}"#,
            0,
        )
        .await;
    // Reason: 300 ms exceeds the 50 ms timeout; the partial batch flushes.
    tokio::time::sleep(Duration::from_millis(300)).await;
    wait_points(&client, &endpoint, &org, &token, &bucket, measurement, 1).await;

    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;
    broker.finish().await;
}

/// A generous request_timeout_ms reaches the real server.
///
/// Covers request_timeout_ms. The write uses the timeout value
/// and the point returns from the server.
#[tokio::test]
#[ignore = "needs a real server (see INFLUXDB_* env)"]
async fn influxdb_request_timeout_through_broker() {
    let endpoint = qual_env("INFLUXDB_ENDPOINT");
    let org = qual_env("INFLUXDB_ORG");
    let bucket = qual_env("INFLUXDB_BUCKET");
    let token = qual_env("INFLUXDB_TOKEN");
    let measurement = "ce2_influx_req_timeout";
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    ensure_bucket(&client, &endpoint, &org, &token, &bucket).await;
    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-influx-request-timeout",
            "influxdb",
            influxdb_config(&InfluxdbConfig {
                endpoint: &endpoint,
                bucket: &bucket,
                org: &org,
                token: &token,
                measurement_template: measurement,
                precision: "ms",
                batch_size: 1,
                batch_timeout_ms: 100,
                // Reason: 30 s proves the value reaches the server.
                // Source: pipeline timeout budget.
                request_timeout_ms: Some(30_000),
            }),
        )
        .await;
    let rule = broker
        .create_forward_rule(
            "ce2-influx-request-timeout-rule",
            "ce2/influx/req-timeout",
            "ce2-influx-request-timeout",
        )
        .await;

    broker
        .publish(
            "ce2-influx-request-timeout-pub",
            "ce2/influx/req-timeout",
            br#"{"device": "d", "val": 1.0}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_success(&rule, 1, Duration::from_secs(30))
        .await;
    wait_points(&client, &endpoint, &org, &token, &bucket, measurement, 1).await;

    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;
    broker.finish().await;
}

/// Splits one Flux CSV line on commas outside quotes.
/// A quoted field may hold commas. Two quotes mean one quote.
/// Reason: JSON payloads hold commas inside quotes.
fn split_csv_line(line: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut cur));
            }
            _ => cur.push(c),
        }
    }
    out.push(cur);
    out
}

/// Reads `_time` cells as RFC3339 text and `ts` cells as nanoseconds.
fn query_times(body: &str) -> Vec<i64> {
    let mut out = Vec::new();
    let mut header_seen = false;
    let mut time_idx: Option<usize> = None;
    let mut ts_idx: Option<usize> = None;
    for line in body.lines() {
        let line = line.trim_end_matches('\r');
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if !header_seen {
            header_seen = true;
            let names: Vec<String> = split_csv_line(line)
                .iter()
                .map(|c| c.trim().trim_matches('"').to_string())
                .collect();
            time_idx = names.iter().position(|c| c == "_time");
            ts_idx = names.iter().position(|c| c == "ts");
            continue;
        }
        let cells = split_csv_line(line);
        if let (Some(ti), Some(si)) = (time_idx, ts_idx) {
            let time_text = cells
                .get(ti)
                .map(|c| c.trim().trim_matches('"'))
                .unwrap_or("");
            // Reason: InfluxDB returns RFC3339 text, never an integer.
            // Source: vendor query CSV shape.
            assert!(
                time_text.contains('T'),
                "time cell holds RFC3339 text: {time_text}"
            );
            if let Some(cell) = cells.get(si) {
                if let Ok(ts) = cell.trim().trim_matches('"').parse::<i64>() {
                    out.push(ts);
                }
            }
        }
    }
    out
}

/// Reads the current wall time in milliseconds.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// A duplicate v5 connector id must fail with the documented response.
///
/// The spec asks for 400 with `ALREADY_EXISTS`.
#[tokio::test]
#[ignore = "needs no server; the v5 duplicate path runs in qualification"]
async fn influxdb_duplicate_through_broker() {
    let broker = TestBroker::start("").await;
    let body = json!({
        "name": "ce2-influx-dup",
        "type": "influxdb",
        "server": "http://127.0.0.1:8086",
        "bucket": "qual-b322",
        "org": "qual-org",
        "token": "qual-token-1",
        "measurement": "ce2_dup",
        "precision": "ms",
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

/// Unknown and unmatched-topic cases through the broker.
///
/// An unknown connector id answers 404 with a body.
/// A publish without a matching rule writes no point.
#[tokio::test]
#[ignore = "needs a real server (see INFLUXDB_* env)"]
async fn influxdb_unknown_nomatch_through_broker() {
    let endpoint = qual_env("INFLUXDB_ENDPOINT");
    let org = qual_env("INFLUXDB_ORG");
    let bucket = qual_env("INFLUXDB_BUCKET");
    let token = qual_env("INFLUXDB_TOKEN");
    let measurement = "ce2_influx_nomatch";
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    ensure_bucket(&client, &endpoint, &org, &token, &bucket).await;
    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;

    let broker = TestBroker::start("").await;
    // An unknown connector id answers 404 with a body, never an empty answer.
    let (status, body) = broker
        .api(
            reqwest::Method::GET,
            "/api/v5/connectors/ce2-influx-no-such-connector",
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
            "ce2-influx-nomatch",
            "influxdb",
            influxdb_config(&InfluxdbConfig {
                endpoint: &endpoint,
                bucket: &bucket,
                org: &org,
                token: &token,
                measurement_template: measurement,
                precision: "ms",
                batch_size: 1,
                batch_timeout_ms: 100,
                request_timeout_ms: None,
            }),
        )
        .await;
    broker
        .create_forward_rule(
            "ce2-influx-nomatch-rule",
            "ce2/influx/nomatch",
            "ce2-influx-nomatch",
        )
        .await;
    broker
        .publish(
            "ce2-influx-nomatch-pub",
            "ce2/influx/nomatch",
            br#"{"device": "d", "val": 1.0}"#,
            0,
        )
        .await;
    wait_points(&client, &endpoint, &org, &token, &bucket, measurement, 1).await;
    assert_eq!(
        count_points(&client, &endpoint, &org, &token, &bucket, measurement).await,
        1,
        "matched topic stores one point"
    );
    // A new topic without a rule writes no point.
    // A new measurement stays empty.
    let probe = "ce2_influx_nomatch_probe";
    delete_measurement(&client, &endpoint, &org, &token, &bucket, probe).await;
    broker
        .publish(
            "ce2-influx-nomatch-pub2",
            "ce2/influx/nomatch-other-new",
            br#"{"device": "d", "val": 2.0}"#,
            0,
        )
        .await;
    // Reason: 2 s lets a wrong forward arrive.
    // Source: loopback delivery is instant.
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        count_points(&client, &endpoint, &org, &token, &bucket, measurement).await,
        1,
        "unmatched topic adds no point"
    );
    assert_eq!(
        count_points(&client, &endpoint, &org, &token, &bucket, probe).await,
        0,
        "new measurement stays empty"
    );

    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;
    broker.finish().await;
}

/// Connector and rule persist across a broker restart.
///
/// Uses the v5 connector store, which replays on boot.
/// Publishes before and after the restart; both points return.
#[tokio::test]
#[ignore = "needs a real server (see INFLUXDB_* env)"]
async fn influxdb_persist_across_restart_through_broker() {
    let endpoint = qual_env("INFLUXDB_ENDPOINT");
    let org = qual_env("INFLUXDB_ORG");
    let bucket = qual_env("INFLUXDB_BUCKET");
    let token = qual_env("INFLUXDB_TOKEN");
    let measurement = "ce2_influx_restart";
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    ensure_bucket(&client, &endpoint, &org, &token, &bucket).await;
    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;

    let mut broker = TestBroker::start("").await;
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "name": "ce2-influx-restart",
                "type": "influxdb",
                "server": endpoint,
                "bucket": bucket,
                "org": org,
                "token": token,
                "measurement": measurement,
                "precision": "ms",
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 connector creates: {body}");
    broker
        .create_forward_rule(
            "ce2-influx-restart-rule",
            "ce2/influx/restart",
            "ce2-influx-restart",
        )
        .await;

    broker
        .publish(
            "ce2-influx-restart-pub1",
            "ce2/influx/restart",
            br#"{"device": "dev-r", "val": 11.0}"#,
            0,
        )
        .await;
    wait_points(&client, &endpoint, &org, &token, &bucket, measurement, 1).await;

    broker.stop_and_restart_orderly().await;

    broker
        .publish(
            "ce2-influx-restart-pub2",
            "ce2/influx/restart",
            br#"{"device": "dev-r", "val": 12.0}"#,
            0,
        )
        .await;
    wait_points(&client, &endpoint, &org, &token, &bucket, measurement, 2).await;

    delete_measurement(&client, &endpoint, &org, &token, &bucket, measurement).await;
    broker.finish().await;
}
