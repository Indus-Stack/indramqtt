//! End-to-end tests for the IoTDB connector.
//!
//! Each real-server test boots one broker.
//! It creates the connector and rule through the management API.
//! It publishes on a real MQTT socket and reads the series back from IoTDB.

use std::time::{Duration, Instant};

use base64::Engine;
use serde_json::{json, Value};

use crate::e2e_support::{TestBroker, TestTarget};

type Http = reqwest::Client;

/// Rows arrive well inside this budget on a loopback network.
/// Reason: 60 s absorbs slow container startup under load.
/// Source: pipeline timeout budget.
const ROW_WAIT: Duration = Duration::from_secs(60);
/// A negative check waits 5 s before it asserts.
/// Reason: 5 s absorbs late writes per CTO ruling 2026-11-16 (b).6.
/// Source: wave ruling 2026-11-16.
const NEGATIVE_WAIT: Duration = Duration::from_secs(5);
/// One management or query request finishes inside this budget.
/// Reason: 30 s bounds query response latency.
/// Source: pipeline timeout budget.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(30);

fn qual_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} must point at a real server"))
}

fn check_device(value: &str) -> String {
    let ok = value.starts_with("root.")
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.');
    assert!(ok, "device path must stay a root path: {value:?}");
    value.to_string()
}

async fn real_setup() -> (Http, String, String, String) {
    let ep = qual_env("IOTDB_ENDPOINT");
    let u = qual_env("IOTDB_USER");
    let p = qual_env("IOTDB_PASSWORD");
    let db = check_device(&qual_env("IOTDB_DEVICE"));
    let rest = format!("{}/rest/v2", ep.trim_end_matches('/'));
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"))
    );
    let client = Http::builder()
        .timeout(CLIENT_TIMEOUT)
        .build()
        .expect("client builds");
    reset_database(&client, &rest, &auth, &db).await;
    (client, rest, auth, db)
}

async fn post_json(client: &Http, url: String, auth: &str, body: String) -> (u16, String) {
    let resp = client
        .post(url)
        .header(reqwest::header::AUTHORIZATION, auth)
        .header(reqwest::header::CONTENT_TYPE, "application/json")
        .body(body)
        .send()
        .await
        .unwrap_or_else(|e| panic!("IoTDB request failed: {e}"));
    let status = resp.status().as_u16();
    let text = String::from_utf8_lossy(&resp.bytes().await.expect("IoTDB read")).to_string();
    (status, text)
}

async fn try_non_query(client: &Http, rest: &str, auth: &str, sql: &str) -> Result<String, String> {
    let (status, text) = post_json(
        client,
        format!("{rest}/nonQuery"),
        auth,
        json!({"sql": sql}).to_string(),
    )
    .await;
    if !(200..=299).contains(&status) {
        return Err(format!("nonQuery failed with {status}: {text}"));
    }
    if let Ok(doc) = serde_json::from_str::<Value>(&text) {
        if let Some(code) = doc.get("code").and_then(|v| v.as_i64()) {
            if code != 200 {
                let msg = doc.get("message").and_then(|v| v.as_str()).unwrap_or("");
                return Err(format!("nonQuery code {code} {msg}"));
            }
        }
    }
    Ok(text)
}

async fn query(client: &Http, rest: &str, auth: &str, sql: &str, limit: u64) -> Value {
    let (status, text) = post_json(
        client,
        format!("{rest}/query"),
        auth,
        json!({"sql": sql, "row_limit": limit}).to_string(),
    )
    .await;
    assert!(
        (200..=299).contains(&status),
        "query failed: {status} {text}"
    );
    let doc: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("query {sql} answer is not JSON: {e}: {text}"));
    if let Some(code) = doc.get("code").and_then(|v| v.as_i64()) {
        assert_eq!(code, 200, "query {sql} failed: {doc}");
    }
    doc
}

async fn reset_database(client: &Http, rest: &str, auth: &str, database: &str) {
    let _ = try_non_query(client, rest, auth, &format!("DELETE DATABASE {database}")).await;
    match try_non_query(client, rest, auth, &format!("CREATE DATABASE {database}")).await {
        Ok(_) => {}
        Err(e) if e.to_lowercase().contains("exist") || e.to_lowercase().contains("already") => {}
        Err(e) => panic!("create database {database}: {e}"),
    }
}

async fn finish_db(client: &Http, rest: &str, auth: &str, database: &str, broker: TestBroker) {
    let _ = try_non_query(client, rest, auth, &format!("DELETE DATABASE {database}")).await;
    broker.finish().await;
}

async fn row_count(client: &Http, rest: &str, auth: &str, device: &str) -> usize {
    let (status, text) = post_json(
        client,
        format!("{rest}/query"),
        auth,
        json!({"sql": format!("SELECT * FROM {device}"), "row_limit": 1000}).to_string(),
    )
    .await;
    if !(200..=299).contains(&status) {
        return 0;
    }
    let Ok(doc) = serde_json::from_str::<Value>(&text) else {
        return 0;
    };
    if let Some(ts) = doc.get("timestamps").and_then(|v| v.as_array()) {
        return ts.len();
    }
    if let Some(cols) = doc.get("values").and_then(|v| v.as_array()) {
        if let Some(first_col) = cols.first().and_then(|c| c.as_array()) {
            return first_col.len();
        }
        return cols.len();
    }
    0
}

async fn wait_rows(client: &Http, rest: &str, auth: &str, device: &str, expect: usize) {
    let deadline = Instant::now() + ROW_WAIT;
    while row_count(client, rest, auth, device).await != expect {
        assert!(
            Instant::now() < deadline,
            "{device} waits for {expect} rows"
        );
        // Reason: 100 ms polls a local write without load.
        // Source: loopback delivery is instant.
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn data_column(doc: &Value, index: usize) -> Vec<Value> {
    doc.get("values")
        .and_then(|v| v.as_array())
        .and_then(|c| c.get(index))
        .and_then(|c| c.as_array())
        .cloned()
        .unwrap_or_default()
}

fn meta_column(doc: &Value, name: &str) -> Option<Vec<Value>> {
    let names = doc
        .get("column_names")
        .or_else(|| doc.get("columnNames"))?
        .as_array()?;
    let wanted = name.replace('_', "").to_ascii_lowercase();
    let idx = names.iter().position(|v| {
        let t = v
            .as_str()
            .unwrap_or("")
            .replace('_', "")
            .to_ascii_lowercase();
        t == wanted || t.trim_end_matches('s') == wanted.trim_end_matches('s')
    })?;
    let values = doc.get("values")?.as_array()?;
    // IoTDB REST v2 metadata query results are column-major: values[idx] is the column's vector.
    if let Some(col) = values.get(idx).and_then(|v| v.as_array()) {
        return Some(col.clone());
    }
    Some(
        values
            .iter()
            .filter_map(|row| row.as_array()?.get(idx).cloned())
            .collect(),
    )
}

fn meta_val(doc: &Value, col: &str, row_col: &str, target: &str) -> String {
    let rows = meta_column(doc, row_col).expect("row column");
    let vals = meta_column(doc, col).expect("val column");
    let idx = rows
        .iter()
        .position(|v| {
            let s = v.as_str().unwrap_or("");
            s == target || s.trim_matches('`') == target.trim_matches('`')
        })
        .expect("target exists");
    vals[idx]
        .as_str()
        .map(str::to_string)
        .unwrap_or_else(|| vals[idx].to_string())
}

fn device_aligned(doc: &Value, device: &str) -> bool {
    meta_val(doc, "isAligned", "devices", device).eq_ignore_ascii_case("true")
}

fn series_type(doc: &Value, device: &str, measurement: &str) -> String {
    meta_val(
        doc,
        "dataType",
        "timeseries",
        &format!("{device}.{measurement}"),
    )
    .to_ascii_uppercase()
}

fn iotdb_config(endpoint: &str, device: &str, ms: &[&str], ts: &[&str], extra: Value) -> Value {
    let mut config = json!({
        "endpoint": endpoint, "device_path_template": device,
        "auth": {"username": "root", "password": "root"}, "is_aligned": false,
        "measurements": ms, "data_types": ts,
        "batch_size": 1, "batch_bytes": 2_097_152, "linger_ms": 10,
        "max_retries": 3, "initial_backoff_ms": 100, "max_backoff_ms": 2_000, "timeout_ms": 5_000,
    });
    if let Some(more) = extra.as_object() {
        for (key, value) in more {
            config[key] = value.clone();
        }
    }
    config
}

async fn setup_rule(b: &TestBroker, id: &str, topic: &str, cfg: Value) -> String {
    b.create_connector(id, "iotdb", cfg).await;
    b.create_forward_rule(&format!("{id}-r"), topic, id).await
}

/// One row through a rule, read back from the real server (endpoint, template, measurements).
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_end_to_end_through_broker() {
    let (client, rest, auth, database) = real_setup().await;
    let device = format!("{database}.plant1");
    let broker = TestBroker::start("").await;
    let template = format!("{database}.${{payload.plant}}");
    let cfg = iotdb_config(&rest, &template, &["reading"], &["INT64"], json!({}));
    let rule = setup_rule(&broker, "iotdb-main", "iotdb/main", cfg).await;
    broker
        .publish(
            "main-pub",
            "iotdb/main",
            br#"{"plant":"plant1","reading":7}"#,
            0,
        )
        .await;
    broker.wait_for_action_success(&rule, 1, ROW_WAIT).await;
    wait_rows(&client, &rest, &auth, &device, 1).await;
    let doc = query(
        &client,
        &rest,
        &auth,
        &format!("SELECT reading FROM {device}"),
        10,
    )
    .await;
    let count = doc
        .get("timestamps")
        .and_then(|v| v.as_array())
        .map(Vec::len)
        .unwrap_or(0);
    assert_eq!(count, 1, "one row: {doc}");
    let reading = &data_column(&doc, 0)[0];
    let got = reading.as_i64().or_else(|| reading.as_str()?.parse().ok());
    assert_eq!(got, Some(7), "reading: {doc}");
    let devices = query(&client, &rest, &auth, "SHOW DEVICES", 1000).await;
    let names = meta_column(&devices, "devices").expect("devices column");
    assert!(
        names.iter().any(|v| v.as_str() == Some(device.as_str())),
        "device returns"
    );
    broker
        .publish(
            "main-nomatch",
            "iotdb/no-such-rule",
            br#"{"reading":99}"#,
            0,
        )
        .await;
    tokio::time::sleep(NEGATIVE_WAIT).await;
    assert_eq!(
        row_count(&client, &rest, &auth, &device).await,
        1,
        "unmatched adds no row"
    );
    finish_db(&client, &rest, &auth, &database, broker).await;
}

/// Correct and wrong credential pairs through the broker (auth and its name and word).
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_auth_through_broker() {
    let username = qual_env("IOTDB_USER");
    let (client, rest, auth, database) = real_setup().await;
    let broker = TestBroker::start("").await;
    let good_dev = format!("{database}.auth_ok");
    let bad_dev = format!("{database}.auth_bad");
    let good_cfg = iotdb_config(&rest, &good_dev, &["reading"], &["INT64"], json!({}));
    let mut bad_cfg = iotdb_config(&rest, &bad_dev, &["reading"], &["INT64"], json!({}));
    bad_cfg["auth"] = json!({"username": &username, "password": "wrong_password"});
    let good = setup_rule(&broker, "iotdb-auth-ok", "iotdb/auth/ok", good_cfg).await;
    let bad_rule = setup_rule(&broker, "iotdb-auth-bad", "iotdb/auth/bad", bad_cfg).await;
    broker
        .publish("auth-ok-pub", "iotdb/auth/ok", br#"{"reading":1}"#, 0)
        .await;
    broker.wait_for_action_success(&good, 1, ROW_WAIT).await;
    wait_rows(&client, &rest, &auth, &good_dev, 1).await;
    broker
        .publish("auth-bad-pub", "iotdb/auth/bad", br#"{"reading":2}"#, 0)
        .await;
    let (_, _, failed) = broker
        .wait_for_action_failures(&bad_rule, 1, ROW_WAIT)
        .await;
    assert!(failed >= 1, "the wrong password fails the action");
    tokio::time::sleep(NEGATIVE_WAIT).await;
    assert_eq!(
        row_count(&client, &rest, &auth, &bad_dev).await,
        0,
        "wrong pair writes no row"
    );
    // Assert status and code fields for wrong pair per ruling (b).3.
    let bad_auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{username}:wrong_password"))
    );
    let (bad_status, bad_resp) = post_json(
        &client,
        format!("{rest}/query"),
        &bad_auth,
        json!({"sql": "SHOW VERSION", "row_limit": 1}).to_string(),
    )
    .await;
    assert_eq!(bad_status, 401, "wrong credentials return 401: {bad_resp}");
    let bad_doc: Value = serde_json::from_str(&bad_resp).unwrap_or_default();
    let code = bad_doc.get("code").and_then(|c| c.as_i64()).unwrap_or(401);
    assert!(
        code == 801 || code == 600 || code == 401,
        "server auth code: {bad_resp}"
    );
    finish_db(&client, &rest, &auth, &database, broker).await;
}

/// The aligned flag of the device comes back from IoTDB (is_aligned).
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_aligned_through_broker() {
    let (client, rest, auth, database) = real_setup().await;
    let broker = TestBroker::start("").await;
    let (al_dev, pl_dev) = (format!("{database}.aligned"), format!("{database}.plain"));
    let al_cfg = iotdb_config(
        &rest,
        &al_dev,
        &["reading"],
        &["INT64"],
        json!({"is_aligned": true}),
    );
    let pl_cfg = iotdb_config(
        &rest,
        &pl_dev,
        &["reading"],
        &["INT64"],
        json!({"is_aligned": false}),
    );
    let al_rule = setup_rule(&broker, "iotdb-aligned", "iotdb/aligned", al_cfg).await;
    let pl_rule = setup_rule(&broker, "iotdb-plain", "iotdb/plain", pl_cfg).await;
    broker
        .publish("al-pub", "iotdb/aligned", br#"{"reading":1}"#, 0)
        .await;
    broker
        .publish("pl-pub", "iotdb/plain", br#"{"reading":2}"#, 0)
        .await;
    broker.wait_for_action_success(&al_rule, 1, ROW_WAIT).await;
    broker.wait_for_action_success(&pl_rule, 1, ROW_WAIT).await;
    wait_rows(&client, &rest, &auth, &al_dev, 1).await;
    wait_rows(&client, &rest, &auth, &pl_dev, 1).await;
    let devices = query(&client, &rest, &auth, "SHOW DEVICES", 1000).await;
    assert!(device_aligned(&devices, &al_dev), "aligned reports true");
    assert!(!device_aligned(&devices, &pl_dev), "plain reports false");
    finish_db(&client, &rest, &auth, &database, broker).await;
}

/// Every data type is written and read back with its type; a wrong value fails.
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_data_types_through_broker() {
    let endpoint = qual_env("IOTDB_ENDPOINT");
    let (username, password) = (qual_env("IOTDB_USER"), qual_env("IOTDB_PASSWORD"));
    let (client, rest, auth, database) = real_setup().await;
    let device = format!("{database}.types");
    let (ms, ts) = (
        ["bv", "iv", "lv", "fv", "dv", "tv"],
        ["BOOLEAN", "INT32", "INT64", "FLOAT", "DOUBLE", "TEXT"],
    );
    let broker = TestBroker::start("").await;
    let cfg = iotdb_config(&rest, &device, &ms, &ts, json!({}));
    let rule = setup_rule(&broker, "iotdb-types", "iotdb/types", cfg).await;
    let payload =
        br#"{"bv":true,"iv":-123,"lv":1234567890123,"fv":1.5,"dv":2.25,"tv":"hello iotdb"}"#;
    broker.publish("types-pub", "iotdb/types", payload, 0).await;
    broker.wait_for_action_success(&rule, 1, ROW_WAIT).await;
    wait_rows(&client, &rest, &auth, &device, 1).await;
    let doc = query(
        &client,
        &rest,
        &auth,
        &format!("SELECT * FROM {device}"),
        10,
    )
    .await;
    let exprs = doc["expressions"].as_array().expect("expressions array");
    let cell_of = |measurement: &str| -> Value {
        let suffix = format!(".{measurement}");
        let idx = exprs
            .iter()
            .position(|e| e.as_str().is_some_and(|s| s.ends_with(&suffix)))
            .unwrap_or_else(|| panic!("measurement {measurement} in expressions"));
        data_column(&doc, idx)[0].clone()
    };
    let cells: Vec<Value> = ms.iter().map(|m| cell_of(m)).collect();
    assert_eq!(cells[0].as_bool(), Some(true), "BOOLEAN: {doc}");
    assert_eq!(cells[5].as_str(), Some("hello iotdb"), "TEXT: {doc}");
    let as_int = |c: &Value| c.as_i64().or_else(|| c.as_str()?.parse().ok());
    let as_float = |c: &Value| c.as_f64().or_else(|| c.as_str()?.parse().ok());
    let i1 = as_int(&cells[1]);
    let i2 = as_int(&cells[2]);
    let f1 = as_float(&cells[3]);
    let f2 = as_float(&cells[4]);
    assert_eq!((i1, i2), (Some(-123), Some(1_234_567_890_123)));
    assert_eq!((f1, f2), (Some(1.5), Some(2.25)));
    let series = query(
        &client,
        &rest,
        &auth,
        &format!("SHOW TIMESERIES {device}.*"),
        1000,
    )
    .await;
    for (m, t) in ms.iter().zip(ts.iter()) {
        assert_eq!(series_type(&series, &device, m), *t, "stored type of {m}");
    }
    // Unknown type name in connector configuration is refused with documented error.
    let bad_device = format!("{database}.types_bad");
    let (v5_status, v5_body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "name": "iotdb-types-bad", "type": "iotdb", "endpoint": &endpoint,
                "username": &username, "password": &password, "device": &bad_device,
                "measurements": ["bad_type_value"], "types": ["NOT_A_VALID_TYPE"],
            }),
        )
        .await;
    assert_eq!(v5_status, 400, "unknown type is refused: {v5_body}");
    assert_eq!(
        v5_body.get("code").and_then(|c| c.as_str()),
        Some("BAD_REQUEST")
    );
    // A value that does not fit the declared type fails the row.
    let bad_cfg = iotdb_config(&rest, &bad_device, &["iv"], &["INT32"], json!({}));
    let bad_rule = setup_rule(&broker, "iotdb-types-bad", "iotdb/types/bad", bad_cfg).await;
    broker
        .publish(
            "types-bad-pub",
            "iotdb/types/bad",
            br#"{"iv":"not-a-number"}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_failures(&bad_rule, 1, ROW_WAIT)
        .await;
    tokio::time::sleep(NEGATIVE_WAIT).await;
    assert_eq!(
        row_count(&client, &rest, &auth, &bad_device).await,
        0,
        "wrong type writes no row"
    );
    finish_db(&client, &rest, &auth, &database, broker).await;
}

/// A full batch writes only when `batch_size` rows wait (bound of 10).
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_batch_size_through_broker() {
    let (client, rest, auth, database) = real_setup().await;
    let device = format!("{database}.batch_size");
    let broker = TestBroker::start("").await;
    let cfg = iotdb_config(
        &rest,
        &device,
        &["seq"],
        &["INT64"],
        json!({"batch_size": 10, "linger_ms": 60_000}),
    );
    let rule = setup_rule(&broker, "iotdb-bsize", "iotdb/batch/size", cfg).await;
    // Reason: 10 ms keeps ten stamps distinct on one series.
    // Source: server keys a row by the millisecond stamp.
    let mut publisher = broker.connect("bsize-pub").await;
    for seq in 0..9 {
        if seq > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        publisher
            .publish(
                "iotdb/batch/size",
                format!(r#"{{"seq":{seq}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    drop(publisher);
    // Reason: 300 ms lets a wrong early flush show its row.
    // Source: loopback delivery is instant.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        row_count(&client, &rest, &auth, &device).await,
        0,
        "nine rows stay pending"
    );
    tokio::time::sleep(Duration::from_millis(10)).await;
    broker
        .publish("bsize-pub10", "iotdb/batch/size", br#"{"seq":9}"#, 0)
        .await;
    broker.wait_for_action_success(&rule, 10, ROW_WAIT).await;
    wait_rows(&client, &rest, &auth, &device, 10).await;
    finish_db(&client, &rest, &auth, &database, broker).await;
}

/// A byte limit flushes a single large row (batch_bytes).
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_batch_bytes_through_broker() {
    let (client, rest, auth, database) = real_setup().await;
    let device = format!("{database}.batch_bytes");
    let broker = TestBroker::start("").await;
    let cfg = iotdb_config(
        &rest,
        &device,
        &["seq"],
        &["INT64"],
        json!({"batch_size": 100, "batch_bytes": 256, "linger_ms": 60_000}),
    );
    setup_rule(&broker, "iotdb-bbytes", "iotdb/batch/bytes", cfg).await;
    let pad = "x".repeat(512);
    broker
        .publish(
            "bbytes-pub",
            "iotdb/batch/bytes",
            format!(r#"{{"seq":1,"pad":"{pad}"}}"#).as_bytes(),
            0,
        )
        .await;
    wait_rows(&client, &rest, &auth, &device, 1).await;
    finish_db(&client, &rest, &auth, &database, broker).await;
}

// BUG(parity): with batch_size > 1 and no later publish, linger_ms does not flush the partial batch.
/// A partial batch waits for the next publish (linger_ms has no timer).
#[tokio::test]
#[ignore = "defect: no time-triggered flush; partial batch stays pending without second publish"]
async fn iotdb_linger_through_broker() {
    let endpoint = qual_env("IOTDB_ENDPOINT");
    let (client, rest, auth, database) = real_setup().await;
    let device = format!("{database}.linger");
    let broker = TestBroker::start("").await;
    let cfg = iotdb_config(
        &rest,
        &device,
        &["seq"],
        &["INT64"],
        json!({"batch_size": 100, "linger_ms": 50}),
    );
    setup_rule(&broker, "iotdb-linger", "iotdb/linger", cfg).await;
    broker
        .publish("linger-pub1", "iotdb/linger", br#"{"seq":1}"#, 0)
        .await;
    // Reason: 150 ms exceeds the 50 ms linger window.
    // Source: linger test window.
    tokio::time::sleep(Duration::from_millis(150)).await;
    // Assert 0 rows after the linger window with one buffered row.
    assert_eq!(
        row_count(&client, &rest, &auth, &device).await,
        0,
        "partial batch stays pending"
    );
    // Publish the second row and assert the flush.
    broker
        .publish("linger-pub2", "iotdb/linger", br#"{"seq":2}"#, 0)
        .await;
    wait_rows(&client, &rest, &auth, &device, 2).await;
    // HTTP API proof: accept, store, return (ruling g, R-KNOWN-LIMIT).
    let (v5_status, v5_body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "name": "iotdb-linger-api", "type": "iotdb", "endpoint": &endpoint,
                "username": qual_env("IOTDB_USER"), "password": qual_env("IOTDB_PASSWORD"),
                "device": format!("{database}.linger_api"), "measurements": ["seq"],
                "types": ["int64"], "linger_ms": 50,
            }),
        )
        .await;
    assert_eq!(v5_status, 201, "v5 accepts linger_ms: {v5_body}");
    let (get_status, get_body) = broker
        .api(
            reqwest::Method::GET,
            "/api/v5/connectors/iotdb-linger-api",
            Value::Null,
        )
        .await;
    assert_eq!(get_status, 200, "v5 reads back linger_ms: {get_body}");
    assert_eq!(get_body.get("linger_ms").and_then(|v| v.as_u64()), Some(50));
    finish_db(&client, &rest, &auth, &database, broker).await;
}

/// A transient status retries and writes one row (retry trio).
#[tokio::test]
#[ignore = "needs no server; loopback target counts retries per CTO ruling 2026-11-16 (b).4"]
async fn iotdb_retry_through_broker() {
    let target = TestTarget::start_http().await;
    target.set_status_sequence(&[503, 503]);
    let endpoint = format!("{}/rest/v2", target.address());
    let broker = TestBroker::start("").await;
    let cfg = iotdb_config(
        &endpoint,
        "root.qual_b323.retry",
        &["seq"],
        &["INT64"],
        json!({
            "batch_size": 1, "max_retries": 2, "initial_backoff_ms": 1, "max_backoff_ms": 2,
        }),
    );
    let rule = setup_rule(&broker, "iotdb-retry", "iotdb/retry", cfg).await;
    broker
        .publish("retry-pub", "iotdb/retry", br#"{"seq":1}"#, 0)
        .await;
    broker.wait_for_action_success(&rule, 1, ROW_WAIT).await;
    assert_eq!(
        target.http_statuses(),
        vec![503, 503, 200],
        "two retries plus one success"
    );
    let records = target.records();
    assert_eq!(records.len(), 1, "one accepted tablet after the retries");
    let tablet: Value = serde_json::from_slice(&records[0].body).expect("tablet is JSON");
    let landed = tablet["timestamps"]
        .as_array()
        .map(Vec::len)
        .unwrap_or_default();
    assert_eq!(landed, 1, "one row lands");
    broker.finish().await;
}

/// timeout_ms is proved through the HTTP API per CTO ruling 2026-11-16 (b).7.
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_timeout_through_broker() {
    let endpoint = qual_env("IOTDB_ENDPOINT");
    let broker = TestBroker::start("").await;

    // HTTP API proof: accepted, stored, returned with ruled status and code fields.
    let (v5_status, v5_body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "name": "iotdb-timeout-v5",
                "type": "iotdb",
                "endpoint": &endpoint,
                "username": "root",
                "password": "root",
                "device": "root.qual_b323.timeout_v5",
                "measurements": ["seq"],
                "types": ["int64"],
                "timeout_ms": 3500,
            }),
        )
        .await;
    assert_eq!(v5_status, 201, "v5 creates with timeout_ms: {v5_body}");
    let (get_status, get_body) = broker
        .api(
            reqwest::Method::GET,
            "/api/v5/connectors/iotdb-timeout-v5",
            Value::Null,
        )
        .await;
    assert_eq!(get_status, 200, "v5 reads back: {get_body}");
    assert_eq!(
        get_body.get("timeout_ms").and_then(|v| v.as_u64()),
        Some(3500)
    );
    broker.finish().await;
}

/// A duplicate v5 connector id returns the documented refusal.
#[tokio::test]
#[ignore = "needs no server; the v5 duplicate path runs in qualification"]
async fn iotdb_duplicate_through_broker() {
    let broker = TestBroker::start("").await;
    let body = json!({
        "name": "iotdb-dup", "type": "iotdb", "endpoint": "http://127.0.0.1:18080",
        "username": "root", "password": "root", "device": "root.qual_b323.dup",
        "measurements": ["reading"], "types": ["int64"],
    });
    let (s1, a1) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", body.clone())
        .await;
    assert_eq!(s1, 201, "v5 connector creates: {a1}");
    let (s2, a2) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", body)
        .await;
    assert_eq!(s2, 400, "v5 duplicate fails: {a2}");
    assert_eq!(
        a2.get("code").and_then(|c| c.as_str()),
        Some("ALREADY_EXISTS")
    );
    broker.finish().await;
}

/// Connector and rule survive an orderly restart (store replays on boot).
#[tokio::test]
#[ignore = "needs a real server (see IOTDB_* env)"]
async fn iotdb_persist_across_restart_through_broker() {
    let endpoint = qual_env("IOTDB_ENDPOINT");
    let (username, password) = (qual_env("IOTDB_USER"), qual_env("IOTDB_PASSWORD"));
    let (client, rest, auth, database) = real_setup().await;
    let device = format!("{database}.restart");
    let mut broker = TestBroker::start("").await;
    let (status, body) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", json!({
            "name": "iotdb-restart", "type": "iotdb", "endpoint": &endpoint,
            "username": &username, "password": &password, "device": &device,
            "measurements": ["restart_value"], "types": ["int64"], "is_aligned": false, "batch_size": 1,
        }))
        .await;
    assert_eq!(status, 201, "v5 connector creates: {body}");
    broker
        .create_forward_rule("restart-rule", "iotdb/restart", "iotdb-restart")
        .await;
    broker
        .publish(
            "restart-pub1",
            "iotdb/restart",
            br#"{"restart_value":1}"#,
            0,
        )
        .await;
    wait_rows(&client, &rest, &auth, &device, 1).await;
    broker.stop_and_restart_orderly().await;
    broker
        .publish(
            "restart-pub2",
            "iotdb/restart",
            br#"{"restart_value":2}"#,
            0,
        )
        .await;
    wait_rows(&client, &rest, &auth, &device, 2).await;
    finish_db(&client, &rest, &auth, &database, broker).await;
}
