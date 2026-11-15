//! Tests for the GreptimeDB connector.
//!
//! Each test boots one real broker.
//! It creates the connector and rule.
//! It publishes on a real socket.
//! It reads each row back from the server.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use super::e2e_support::TestBroker;
use serde_json::{json, Value};

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

/// Checks that a table name holds plain identifier characters.
/// Panics on any other input.
/// Reason: names enter SQL text directly.
/// Source: connector qualification guard.
fn check_ident(name: &str) -> String {
    let ok = !name.is_empty()
        && name
            .bytes()
            .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric());
    assert!(ok, "table name must stay a plain identifier");
    name.to_string()
}

/// Builds the HTTP API base of one endpoint.
/// Adds `/v1` only when the value lacks it.
/// Reason: stored values end at the host.
/// Source: transport base normalization.
fn api_base(endpoint: &str) -> String {
    let base = endpoint.trim_end_matches('/');
    if base.ends_with("/v1") {
        base.to_string()
    } else {
        format!("{base}/v1")
    }
}

/// Posts one SQL statement and returns the raw body.
/// Fails when the status or the envelope reports an error.
/// Reason: a refused statement must fail the test.
/// Source: connector qualification guard.
async fn sql_body(client: &reqwest::Client, sql_url: &str, sql: &str) -> String {
    let response = client
        .post(sql_url)
        .form(&[("sql", sql)])
        .send()
        .await
        .expect("statement reaches the server");
    let status = response.status();
    let text = response.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "statement refused: status={status} sql={sql} body={text}"
    );
    if let Ok(value) = serde_json::from_str::<Value>(&text) {
        if let Some(code) = value.get("code").and_then(|cell| cell.as_i64()) {
            assert_eq!(code, 0, "server envelope reports code 0: {text}");
        }
    }
    text
}

/// Finds the first `rows[0][0]` cell as `u64`.
/// Returns `None` when the shape holds no count.
fn find_count(value: &Value) -> Option<u64> {
    match value {
        Value::Object(map) => {
            if let Some(Value::Array(rows)) = map.get("rows") {
                if let Some(Value::Array(cells)) = rows.first() {
                    if let Some(cell) = cells.first() {
                        if let Some(n) = cell.as_u64() {
                            return Some(n);
                        }
                        if let Some(n) = cell.as_i64() {
                            return u64::try_from(n).ok();
                        }
                        if let Some(text) = cell.as_str() {
                            return text.trim().parse::<u64>().ok();
                        }
                    }
                }
            }
            for child in map.values() {
                if let Some(n) = find_count(child) {
                    return Some(n);
                }
            }
            None
        }
        Value::Array(items) => {
            for child in items {
                if let Some(n) = find_count(child) {
                    return Some(n);
                }
            }
            None
        }
        _ => None,
    }
}

/// Reads the count of one table.
/// Panics when the answer holds no count.
fn table_count(body: &str) -> u64 {
    let value: Value = serde_json::from_str(body).expect("count answer parses");
    find_count(&value).expect("count answer holds rows[0][0]")
}

/// Collects every numeric cell of every result row as `i64`.
/// Skips cells without a number.
fn collect_cells(value: &Value, out: &mut Vec<i64>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::Array(rows)) = map.get("rows") {
                for row in rows {
                    if let Some(cells) = row.as_array() {
                        for cell in cells {
                            if let Some(n) = cell.as_i64() {
                                out.push(n);
                            } else if let Some(n) = cell.as_u64() {
                                out.push(n.min(i64::MAX as u64) as i64);
                            } else if let Some(text) = cell.as_str() {
                                if let Ok(n) = text.trim().parse::<i64>() {
                                    out.push(n);
                                }
                            }
                        }
                    }
                }
                return;
            }
            for child in map.values() {
                collect_cells(child, out);
            }
        }
        Value::Array(items) => {
            for child in items {
                collect_cells(child, out);
            }
        }
        _ => {}
    }
}

/// Reads every numeric cell of every result row.
fn query_cells(body: &str) -> Vec<i64> {
    let value: Value = serde_json::from_str(body).expect("query answer parses");
    let mut out = Vec::new();
    collect_cells(&value, &mut out);
    out
}

/// Polls one table until it holds `expect` rows.
/// Returns the final count.
/// Reason: 60 s covers a slow server insert.
/// Source: pipeline timeout budget.
async fn wait_count(client: &reqwest::Client, sql_url: &str, table: &str, expect: u64) -> u64 {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let body = sql_body(
            client,
            sql_url,
            &format!("SELECT COUNT(*) AS n FROM {table}"),
        )
        .await;
        let seen = table_count(&body);
        if seen == expect {
            return seen;
        }
        assert!(
            Instant::now() < deadline,
            "table {table} holds {seen} rows, expected {expect}"
        );
        // Reason: 200 ms polls fast without load.
        // Source: loopback delivery is instant.
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Drops one table when it exists.
/// Fails when the server refuses the statement.
/// Reason: a stale table breaks exact counts.
/// Source: connector qualification guard.
async fn drop_table(client: &reqwest::Client, sql_url: &str, table: &str) {
    sql_body(client, sql_url, &format!("DROP TABLE IF EXISTS {table}")).await;
}

/// Creates one millisecond table for SQL-path tests.
/// Holds device, value and sequence columns.
/// Reason: fixed columns keep exact-count queries stable.
/// Source: connector qualification schema.
async fn ensure_ms_table(client: &reqwest::Client, sql_url: &str, table: &str) {
    sql_body(
        client,
        sql_url,
        &format!(
            "CREATE TABLE IF NOT EXISTS {table} \
             (ts TIMESTAMP TIME INDEX, device STRING, val DOUBLE, seq BIGINT)"
        ),
    )
    .await;
}

/// Creates one table with the timestamp type of one precision.
/// Uses TIMESTAMP(9) for ns, TIMESTAMP(6) for us, TIMESTAMP for ms, TIMESTAMP(0) for s.
/// Reason: each unit needs its own storage type.
/// Source: connector qualification schema.
async fn ensure_precision_table(
    client: &reqwest::Client,
    sql_url: &str,
    table: &str,
    suffix: &str,
) {
    let ts_type = match suffix {
        "ns" => "TIMESTAMP(9)",
        "us" => "TIMESTAMP(6)",
        "ms" => "TIMESTAMP",
        _ => "TIMESTAMP(0)",
    };
    sql_body(
        client,
        sql_url,
        &format!(
            "CREATE TABLE IF NOT EXISTS {table} \
             (ts {ts_type} TIME INDEX, device STRING, val DOUBLE, seq BIGINT)"
        ),
    )
    .await;
}

/// Reads the current wall time in milliseconds.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or(0)
}

/// Builds a v1 connector payload for GreptimeDB.
/// Uses snake-case format and precision names.
/// Reason: the v1 path parses the sink config directly.
/// Source: sink config deserialization.
fn greptime_config(
    endpoint: &str,
    database: &str,
    table_template: &str,
    format: &str,
    precision: &str,
    batch_size: u64,
    extra: Option<Value>,
) -> Value {
    let mut config = json!({
        "endpoint": endpoint,
        "database": database,
        "table_template": table_template,
        "format": format,
        "timestamp_precision": precision,
        "batch_size": batch_size,
    });
    if let Some(more) = extra {
        for (key, val) in more.as_object().expect("extra is an object") {
            config[key] = val.clone();
        }
    }
    config
}

/// One SQL insert through the broker, read back from the server.
///
/// Covers endpoint, database, format, SqlInsert and table template.
#[tokio::test]
#[ignore = "needs a real server (see GREPTIMEDB_* env)"]
async fn greptimedb_sql_insert_through_broker() {
    let endpoint = qual_env("GREPTIMEDB_ENDPOINT");
    let database = qual_env("GREPTIMEDB_DATABASE");
    let other_database = "ce2_other_db";
    let base = check_ident(&qual_env("GREPTIMEDB_TABLE"));
    let table = format!("{base}_sql");
    let other_table = format!("{base}_sql2");
    let db_table = format!("{base}_db");
    let client = reqwest::Client::builder()
        // Reason: 30 s bounds one slow query.
        // Source: pipeline timeout budget.
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    let sql_url = format!("{}/sql?db={database}", api_base(&endpoint));
    let other_sql_url = format!("{}/sql?db={other_database}", api_base(&endpoint));
    // A non-default database must exist before its table.
    sql_body(
        &client,
        &sql_url,
        &format!("CREATE DATABASE IF NOT EXISTS {other_database}"),
    )
    .await;
    drop_table(&client, &sql_url, &table).await;
    drop_table(&client, &sql_url, &other_table).await;
    drop_table(&client, &sql_url, &db_table).await;
    drop_table(&client, &other_sql_url, &db_table).await;
    ensure_ms_table(&client, &sql_url, &table).await;
    ensure_ms_table(&client, &sql_url, &other_table).await;
    ensure_ms_table(&client, &sql_url, &db_table).await;
    ensure_ms_table(&client, &other_sql_url, &db_table).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-greptime-sql",
            "greptimedb",
            greptime_config(
                &endpoint,
                &database,
                &table,
                "sql_insert",
                "millisecond",
                1,
                None,
            ),
        )
        .await;
    // A second template selects a second table for the same payload shape.
    broker
        .create_connector(
            "ce2-greptime-sql2",
            "greptimedb",
            greptime_config(
                &endpoint,
                &database,
                &other_table,
                "sql_insert",
                "millisecond",
                1,
                None,
            ),
        )
        .await;
    let rule = broker
        .create_forward_rule(
            "ce2-greptime-sql-rule",
            "ce2/greptime/sql",
            "ce2-greptime-sql",
        )
        .await;
    broker
        .create_forward_rule(
            "ce2-greptime-sql2-rule",
            "ce2/greptime/sql2",
            "ce2-greptime-sql2",
        )
        .await;

    let before = now_millis();
    broker
        .publish(
            "ce2-greptime-sql-pub",
            "ce2/greptime/sql",
            br#"{"device": "dev-sql-1", "val": 21.5, "seq": 7}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_success(&rule, 1, Duration::from_secs(30))
        .await;
    wait_count(&client, &sql_url, &table, 1).await;
    let after = now_millis();

    let body = sql_body(
        &client,
        &sql_url,
        &format!("SELECT device, val, seq FROM {table}"),
    )
    .await;
    assert!(body.contains("dev-sql-1"), "row device returns: {body}");
    assert!(body.contains('7'), "row sequence returns: {body}");

    // The stored timestamp uses millisecond units inside the write window.
    let bounds = query_cells(
        &sql_body(
            &client,
            &sql_url,
            &format!("SELECT MIN(ts) AS mn, MAX(ts) AS mx FROM {table}"),
        )
        .await,
    );
    assert_eq!(bounds.len(), 2, "min/max row returns: {bounds:?}");
    // Reason: 2 min covers clock drift between tasks.
    // Source: connector qualification window.
    for bound in &bounds {
        assert!(
            *bound >= before - 120_000 && *bound <= after + 120_000,
            "millisecond stamp {bound} sits outside [{before}, {after}]"
        );
    }

    // The template selects the table: the second template holds its own row.
    broker
        .publish(
            "ce2-greptime-sql2-pub",
            "ce2/greptime/sql2",
            br#"{"device": "dev-sql-2", "val": 22.5, "seq": 8}"#,
            0,
        )
        .await;
    wait_count(&client, &sql_url, &other_table, 1).await;
    // The first table still holds exactly one row.
    wait_count(&client, &sql_url, &table, 1).await;

    // A non-default database selects the namespace of its rows.
    broker
        .create_connector(
            "ce2-greptime-db",
            "greptimedb",
            greptime_config(
                &endpoint,
                other_database,
                &db_table,
                "sql_insert",
                "millisecond",
                1,
                None,
            ),
        )
        .await;
    let db_rule = broker
        .create_forward_rule("ce2-greptime-db-rule", "ce2/greptime/db", "ce2-greptime-db")
        .await;
    broker
        .publish(
            "ce2-greptime-db-pub",
            "ce2/greptime/db",
            br#"{"device": "dev-db-1", "val": 23.5, "seq": 9}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_success(&db_rule, 1, Duration::from_secs(30))
        .await;
    wait_count(&client, &other_sql_url, &db_table, 1).await;
    let foreign_body = sql_body(
        &client,
        &other_sql_url,
        &format!("SELECT device, seq FROM {db_table}"),
    )
    .await;
    assert!(
        foreign_body.contains("dev-db-1"),
        "row returns from the configured database: {foreign_body}"
    );
    // The default database holds no copy of this table's row.
    wait_count(&client, &sql_url, &db_table, 0).await;

    drop_table(&client, &sql_url, &table).await;
    drop_table(&client, &sql_url, &other_table).await;
    drop_table(&client, &sql_url, &db_table).await;
    drop_table(&client, &other_sql_url, &db_table).await;
    broker.finish().await;
}

/// One Influx line-protocol write through the broker, read back.
///
/// Covers the InfluxLineProtocol format at nanosecond precision.
#[tokio::test]
#[ignore = "needs a real server (see GREPTIMEDB_* env)"]
async fn greptimedb_influx_line_protocol_through_broker() {
    let endpoint = qual_env("GREPTIMEDB_ENDPOINT");
    let database = qual_env("GREPTIMEDB_DATABASE");
    let base = check_ident(&qual_env("GREPTIMEDB_TABLE"));
    let table = format!("{base}_line");
    let client = reqwest::Client::builder()
        // Reason: 30 s bounds one slow query.
        // Source: pipeline timeout budget.
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    let sql_url = format!("{}/sql?db={database}", api_base(&endpoint));
    drop_table(&client, &sql_url, &table).await;
    sql_body(
        &client,
        &sql_url,
        &format!(
            "CREATE TABLE IF NOT EXISTS {table} \
             (ts TIMESTAMP(9) TIME INDEX, device STRING, val DOUBLE, seq BIGINT)"
        ),
    )
    .await;

    // One loopback target observes the line-protocol route.
    let (fake_port, fake_state) = start_fake(String::new(), false).await;
    let fake_endpoint = format!("http://127.0.0.1:{fake_port}");

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-greptime-line-route",
            "greptimedb",
            greptime_config(
                &fake_endpoint,
                &database,
                "line_route",
                "influx_line_protocol",
                "nanosecond",
                1,
                None,
            ),
        )
        .await;
    let route_rule = broker
        .create_forward_rule(
            "ce2-greptime-line-route-rule",
            "ce2/greptime/line/route",
            "ce2-greptime-line-route",
        )
        .await;
    broker
        .publish(
            "ce2-greptime-line-route-pub",
            "ce2/greptime/line/route",
            br#"{"device": "dev-route", "val": 1.0, "seq": 1}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_success(&route_rule, 1, Duration::from_secs(30))
        .await;
    // Reason: 200 ms lets the fake record the exchange.
    // Source: loopback delivery is instant.
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let records = fake_state.records.lock().expect("route records lock");
        assert_eq!(records.len(), 1, "one line-protocol request arrives");
        assert!(
            records[0].0.contains("/influxdb/api/v2/write"),
            "line protocol uses the influx route: {}",
            records[0].0
        );
        assert!(
            records[0].0.contains("precision=ns"),
            "line protocol sends the nanosecond unit: {}",
            records[0].0
        );
    }

    broker
        .create_connector(
            "ce2-greptime-line",
            "greptimedb",
            greptime_config(
                &endpoint,
                &database,
                &table,
                "influx_line_protocol",
                "nanosecond",
                1,
                None,
            ),
        )
        .await;
    let rule = broker
        .create_forward_rule(
            "ce2-greptime-line-rule",
            "ce2/greptime/line",
            "ce2-greptime-line",
        )
        .await;

    let before_ms = now_millis();
    broker
        .publish(
            "ce2-greptime-line-pub",
            "ce2/greptime/line",
            br#"{"device": "dev-line-1", "val": 30.25, "seq": 3}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_success(&rule, 1, Duration::from_secs(30))
        .await;
    wait_count(&client, &sql_url, &table, 1).await;
    let after_ms = now_millis();

    let body = sql_body(
        &client,
        &sql_url,
        &format!("SELECT device, seq FROM {table}"),
    )
    .await;
    assert!(body.contains("dev-line-1"), "line row returns: {body}");

    // The line path stores nanoseconds inside the scaled write window.
    let bounds = query_cells(
        &sql_body(
            &client,
            &sql_url,
            &format!("SELECT MIN(ts) AS mn, MAX(ts) AS mx FROM {table}"),
        )
        .await,
    );
    assert_eq!(bounds.len(), 2, "min/max row returns: {bounds:?}");
    // Reason: 2 min covers clock drift between tasks.
    // Source: connector qualification window.
    let low = (before_ms - 120_000) * 1_000_000;
    let high = (after_ms + 120_000) * 1_000_000;
    for bound in &bounds {
        assert!(
            *bound >= low && *bound <= high,
            "nanosecond stamp {bound} sits outside [{low}, {high}]"
        );
    }

    drop_table(&client, &sql_url, &table).await;
    broker.finish().await;
}

/// Shared state of one verifying fake HTTP target.
struct FakeState {
    expected: String,
    records: Mutex<Vec<(String, Vec<u8>)>>,
}

/// Maximum fake request body kept in memory (1 MiB).
/// Reason: 1 MiB bounds a hostile body.
/// Source: common server request cap.
const MAX_FAKE_BODY_BYTES: usize = 1_048_576;

/// Serves one HTTP exchange against the expected Basic value.
/// Records the path and body only when the credential matches.
async fn serve_fake(mut stream: tokio::net::TcpStream, state: Arc<FakeState>, slow: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // Reason: 10 s bounds one fake exchange.
    // Source: pipeline timeout budget.
    let mut raw = Vec::new();
    let end = loop {
        let mut byte = [0u8; 1];
        match tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut byte)).await {
            Ok(Ok(_)) => raw.push(byte[0]),
            _ => return,
        }
        // Reason: 64 KiB caps a hostile header.
        // Source: common server header cap.
        if raw.len() > 65_536 {
            return;
        }
        if raw.ends_with(b"\r\n\r\n") {
            break raw.len();
        }
    };
    let head = String::from_utf8_lossy(&raw[..end]).to_string();
    let mut lines = head.lines();
    let path = lines
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .unwrap_or_default()
        .to_string();
    let mut auth = String::new();
    let mut length = 0usize;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("authorization") {
                auth = value.trim().to_string();
            }
            if name.trim().eq_ignore_ascii_case("content-length") {
                length = value.trim().parse().unwrap_or(0);
            }
        }
    }
    let mut body = vec![0u8; length.min(MAX_FAKE_BODY_BYTES)];
    if length > 0
        && tokio::time::timeout(Duration::from_secs(10), stream.read_exact(&mut body))
            .await
            .is_err()
    {
        return;
    }
    if slow {
        // Reason: 2000 ms exceeds the short test timeout.
        // Source: timeout test delay.
        tokio::time::sleep(Duration::from_millis(2000)).await;
    }
    let status = if auth == state.expected {
        state
            .records
            .lock()
            .expect("records lock")
            .push((path, body));
        200
    } else {
        401
    };
    let reply = format!("HTTP/1.1 {status} OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
    let _ = stream.write_all(reply.as_bytes()).await;
}

/// Starts one fake that checks the Basic credential.
/// Returns the port and the shared state.
async fn start_fake(expected: String, slow: bool) -> (u16, Arc<FakeState>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("fake binds loopback");
    let port = listener.local_addr().expect("fake address").port();
    let state = Arc::new(FakeState {
        expected,
        records: Mutex::new(Vec::new()),
    });
    let shared = state.clone();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let shared = shared.clone();
            tokio::spawn(async move { serve_fake(stream, shared, slow).await });
        }
    });
    (port, state)
}

/// Encodes one Basic credential value.
/// Uses the standard engine for the header shape.
fn basic_value(username: &str, password: &str) -> String {
    use base64::Engine;
    let raw = format!("{username}:{password}");
    format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(raw.as_bytes())
    )
}

/// Correct and wrong credential pairs through the broker.
///
/// Covers auth, username and password.
/// The fake accepts only the correct pair.
/// The wrong pair must fail the action as a terminal error.
#[tokio::test]
#[ignore = "needs no server; the loopback fake verifies the credential"]
async fn greptimedb_auth_header_through_broker() {
    let expected = basic_value("qual-user", "qual-pass");
    let (port, state) = start_fake(expected, false).await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-greptime-auth-ok",
            "greptimedb",
            json!({
                "endpoint": endpoint,
                "database": "public",
                "table_template": "auth_ok",
                "format": "sql_insert",
                "timestamp_precision": "millisecond",
                "batch_size": 1,
                "auth": {"username": "qual-user", "password": "qual-pass"},
            }),
        )
        .await;
    broker
        .create_connector(
            "ce2-greptime-auth-bad",
            "greptimedb",
            json!({
                "endpoint": endpoint,
                "database": "public",
                "table_template": "auth_bad",
                "format": "sql_insert",
                "timestamp_precision": "millisecond",
                "batch_size": 1,
                "auth": {"username": "qual-user", "password": "wrong-pass"},
            }),
        )
        .await;
    let good = broker
        .create_forward_rule(
            "ce2-greptime-auth-ok-rule",
            "ce2/greptime/auth/ok",
            "ce2-greptime-auth-ok",
        )
        .await;
    let bad = broker
        .create_forward_rule(
            "ce2-greptime-auth-bad-rule",
            "ce2/greptime/auth/bad",
            "ce2-greptime-auth-bad",
        )
        .await;

    broker
        .publish(
            "ce2-greptime-auth-ok-pub",
            "ce2/greptime/auth/ok",
            br#"{"device": "d", "val": 1.0, "seq": 1}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_success(&good, 1, Duration::from_secs(30))
        .await;
    broker
        .publish(
            "ce2-greptime-auth-bad-pub",
            "ce2/greptime/auth/bad",
            br#"{"device": "d", "val": 2.0, "seq": 2}"#,
            0,
        )
        .await;
    let (_, _, failed) = broker
        .wait_for_action_failures(&bad, 1, Duration::from_secs(30))
        .await;
    assert!(failed >= 1, "wrong password fails the action");

    // Reason: 200 ms lets the fake record the exchange.
    // Source: loopback delivery is instant.
    tokio::time::sleep(Duration::from_millis(200)).await;
    {
        let records = state.records.lock().expect("records lock");
        assert_eq!(records.len(), 1, "only the correct pair arrives");
        assert!(
            records[0].0.contains("/v1/sql"),
            "correct request hits the SQL route: {}",
            records[0].0
        );
    }
    broker.finish().await;
}

/// All four timestamp precisions through the broker, read back.
///
/// Covers timestamp_precision and each variant.
/// Each table holds one row in its own unit.
/// Each bound sits inside the write window of that unit.
#[tokio::test]
#[ignore = "needs a real server (see GREPTIMEDB_* env)"]
async fn greptimedb_timestamp_precision_through_broker() {
    let endpoint = qual_env("GREPTIMEDB_ENDPOINT");
    let database = qual_env("GREPTIMEDB_DATABASE");
    let base = check_ident(&qual_env("GREPTIMEDB_TABLE"));
    let client = reqwest::Client::builder()
        // Reason: 30 s bounds one slow query.
        // Source: pipeline timeout budget.
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    let sql_url = format!("{}/sql?db={database}", api_base(&endpoint));
    // Table suffix, precision name and window factor per variant.
    let cases = [
        ("ns", "nanosecond", 1_000_000i64),
        ("us", "microsecond", 1_000i64),
        ("ms", "millisecond", 1i64),
        ("s", "second", 0i64),
    ];
    for (suffix, _, _) in &cases {
        let table = format!("{base}_p_{suffix}");
        drop_table(&client, &sql_url, &table).await;
        ensure_precision_table(&client, &sql_url, &table, suffix).await;
    }

    let broker = TestBroker::start("").await;
    for (suffix, precision, _) in &cases {
        let table = format!("{base}_p_{suffix}");
        let id = format!("ce2-greptime-prec-{suffix}");
        broker
            .create_connector(
                &id,
                "greptimedb",
                greptime_config(
                    &endpoint,
                    &database,
                    &table,
                    "sql_insert",
                    precision,
                    1,
                    None,
                ),
            )
            .await;
        broker
            .create_forward_rule(
                &format!("{id}-rule"),
                &format!("ce2/greptime/prec/{suffix}"),
                &id,
            )
            .await;
    }

    let before_ms = now_millis();
    let mut seq = 0;
    for (suffix, _, _) in &cases {
        seq += 1;
        broker
            .publish(
                &format!("ce2-greptime-prec-pub-{suffix}"),
                &format!("ce2/greptime/prec/{suffix}"),
                format!(r#"{{"device": "dev-p-{suffix}", "val": 1.5, "seq": {seq}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    // Reason: 2 s lets the server make all rows queryable.
    // Source: slow server merge window.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after_ms = now_millis();
    for (suffix, _, factor) in &cases {
        let table = format!("{base}_p_{suffix}");
        wait_count(&client, &sql_url, &table, 1).await;
        let bounds = query_cells(
            &sql_body(
                &client,
                &sql_url,
                &format!("SELECT MIN(ts) AS mn, MAX(ts) AS mx FROM {table}"),
            )
            .await,
        );
        assert_eq!(bounds.len(), 2, "min/max row returns for {suffix}");
        // Reason: 2 min covers clock drift between tasks.
        // Source: connector qualification window.
        let skew = 120_000i64;
        let (low, high) = if *factor == 0 {
            ((before_ms - skew) / 1000, (after_ms + skew) / 1000)
        } else {
            ((before_ms - skew) * factor, (after_ms + skew) * factor)
        };
        for bound in &bounds {
            assert!(
                *bound >= low && *bound <= high,
                "precision {suffix} stamp {bound} sits outside [{low}, {high}]"
            );
        }
    }

    for (suffix, _, _) in &cases {
        drop_table(&client, &sql_url, &format!("{base}_p_{suffix}")).await;
    }
    broker.finish().await;
}

/// A size-triggered batch through the broker.
///
/// Covers batch_size.
/// Three rows reach the server.
#[tokio::test]
#[ignore = "needs a real server (see GREPTIMEDB_* env)"]
async fn greptimedb_batch_size_through_broker() {
    let endpoint = qual_env("GREPTIMEDB_ENDPOINT");
    let database = qual_env("GREPTIMEDB_DATABASE");
    let base = check_ident(&qual_env("GREPTIMEDB_TABLE"));
    let table = format!("{base}_batch");
    let client = reqwest::Client::builder()
        // Reason: 30 s bounds one slow query.
        // Source: pipeline timeout budget.
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    let sql_url = format!("{}/sql?db={database}", api_base(&endpoint));
    drop_table(&client, &sql_url, &table).await;
    ensure_ms_table(&client, &sql_url, &table).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-greptime-batch",
            "greptimedb",
            greptime_config(
                &endpoint,
                &database,
                &table,
                "sql_insert",
                "millisecond",
                3,
                None,
            ),
        )
        .await;
    broker
        .create_forward_rule(
            "ce2-greptime-batch-rule",
            "ce2/greptime/batch",
            "ce2-greptime-batch",
        )
        .await;

    // Publish the batch rows on one socket.
    // Reason: 2 ms keeps the three stamps in different milliseconds.
    // Source: server last-row merge on one series.
    const ROW_SPACING: Duration = Duration::from_millis(2);
    // Reason: 25 ms is below the 50 ms linger, so only a size flush passes.
    // Source: GREPTIME_BATCH_LINGER in the sink.
    const SIZE_FLUSH_DEADLINE: Duration = Duration::from_millis(25);
    let mut publisher = broker.connect("ce2-greptime-batch-pub").await;
    for seq in 0..3 {
        if seq > 0 {
            tokio::time::sleep(ROW_SPACING).await;
        }
        publisher
            .publish(
                "ce2/greptime/batch",
                format!(r#"{{"device": "dev-b", "val": 1.0, "seq": {seq}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    drop(publisher);
    // The third row fills the batch well inside the linger window.
    // The row must arrive before the linger could flush it.
    let deadline = Instant::now() + SIZE_FLUSH_DEADLINE;
    loop {
        let body = sql_body(
            &client,
            &sql_url,
            &format!("SELECT COUNT(*) AS n FROM {table}"),
        )
        .await;
        let seen = table_count(&body);
        if seen == 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "size-triggered batch does not arrive before 25 ms: holds {seen} rows"
        );
        // Reason: 2 ms polls fast without load.
        // Source: loopback delivery is instant.
        tokio::time::sleep(Duration::from_millis(2)).await;
    }
    let seqs = query_cells(
        &sql_body(
            &client,
            &sql_url,
            &format!("SELECT seq FROM {table} ORDER BY seq"),
        )
        .await,
    );
    assert_eq!(seqs, vec![0, 1, 2], "all three rows arrive: {seqs:?}");

    drop_table(&client, &sql_url, &table).await;
    broker.finish().await;
}

/// A full buffer fails closed through the broker.
///
/// Covers buffer_capacity.
/// Six rapid rows exceed a capacity of two.
/// Four actions fail; two rows drain on an orderly stop.
#[tokio::test]
#[ignore = "needs a real server (see GREPTIMEDB_* env)"]
async fn greptimedb_buffer_capacity_fails_closed_through_broker() {
    let endpoint = qual_env("GREPTIMEDB_ENDPOINT");
    let database = qual_env("GREPTIMEDB_DATABASE");
    let base = check_ident(&qual_env("GREPTIMEDB_TABLE"));
    let table = format!("{base}_buf");
    let client = reqwest::Client::builder()
        // Reason: 30 s bounds one slow query.
        // Source: pipeline timeout budget.
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    let sql_url = format!("{}/sql?db={database}", api_base(&endpoint));
    drop_table(&client, &sql_url, &table).await;
    ensure_ms_table(&client, &sql_url, &table).await;

    let mut broker = TestBroker::start("").await;
    // Reason: a batch of 100 keeps the buffer bound visible first.
    // Source: task buffer case.
    // Reason: 2 rows fit in memory for the test.
    // Source: task buffer case.
    broker
        .create_connector(
            "ce2-greptime-buf",
            "greptimedb",
            greptime_config(
                &endpoint,
                &database,
                &table,
                "sql_insert",
                "millisecond",
                100,
                Some(json!({"buffer_capacity": 2})),
            ),
        )
        .await;
    let listed = broker.list_connectors().await;
    assert!(
        listed
            .as_array()
            .is_some_and(|rows| rows.iter().any(|row| row["id"] == "ce2-greptime-buf")),
        "connector record exists"
    );
    let rule = broker
        .create_forward_rule(
            "ce2-greptime-buf-rule",
            "ce2/greptime/buf",
            "ce2-greptime-buf",
        )
        .await;
    // Publish all six rows on one socket.
    // The first two rows fill the buffer, so the linger sweep skips it.
    // Reason: 2 ms keeps the two buffered stamps in different milliseconds.
    // Source: server last-row merge on one series.
    const ROW_SPACING: Duration = Duration::from_millis(2);
    let mut publisher = broker.connect("ce2-greptime-buf-pub").await;
    for value in 1..=6 {
        if value > 1 {
            tokio::time::sleep(ROW_SPACING).await;
        }
        publisher
            .publish(
                "ce2/greptime/buf",
                format!(r#"{{"device": "dev-f", "val": 1.0, "seq": {value}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    drop(publisher);
    let (_, _, failed) = broker
        .wait_for_action_failures(&rule, 4, Duration::from_secs(30))
        .await;
    assert_eq!(failed, 4, "four rows fail while the buffer is full");
    let body = sql_body(
        &client,
        &sql_url,
        &format!("SELECT COUNT(*) AS n FROM {table}"),
    )
    .await;
    assert_eq!(table_count(&body), 0, "no row leaves before the drain");

    // Reason: an orderly stop drains the two buffered rows.
    // Source: task orderly-stop case.
    broker.stop_and_restart_orderly().await;
    wait_count(&client, &sql_url, &table, 2).await;
    drop_table(&client, &sql_url, &table).await;
    broker.finish().await;
}

/// A slow server trips the request timeout through the broker.
///
/// Covers timeout_ms.
/// The delayed fake answers after 2000 ms.
/// A 300 ms timeout fails the action before the reply.
#[tokio::test]
#[ignore = "needs no server; the loopback fake delays the reply"]
async fn greptimedb_timeout_applied_through_broker() {
    // No credential on either side: the empty value matches.
    let (port, _state) = start_fake(String::new(), true).await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-greptime-timeout",
            "greptimedb",
            json!({
                "endpoint": endpoint,
                "database": "public",
                "table_template": "timeout_t",
                "format": "sql_insert",
                "timestamp_precision": "millisecond",
                "batch_size": 1,
                // Reason: 300 ms fails before the 2000 ms reply.
                // Source: timeout test delay.
                "timeout_ms": 300,
            }),
        )
        .await;
    let rule = broker
        .create_forward_rule(
            "ce2-greptime-timeout-rule",
            "ce2/greptime/timeout",
            "ce2-greptime-timeout",
        )
        .await;
    broker
        .publish(
            "ce2-greptime-timeout-pub",
            "ce2/greptime/timeout",
            br#"{"device": "d", "val": 1.0, "seq": 1}"#,
            0,
        )
        .await;
    let (total, success, failed) = broker
        .wait_for_action_failures(&rule, 1, Duration::from_secs(30))
        .await;
    assert!(failed >= 1, "slow server fails the action");
    // The broker counts the attempt and stores no row.
    assert!(total >= 1, "broker counts the timed-out action");
    assert_eq!(success, 0, "slow server stores no row");
    broker.finish().await;
}

/// A duplicate v5 connector id must fail with the documented conflict.
///
/// The spec asks for 409 with `ALREADY_EXISTS`.
#[tokio::test]
#[ignore = "needs no server; the v5 duplicate path runs in qualification"]
async fn greptimedb_duplicate_through_broker() {
    let broker = TestBroker::start("").await;
    let body = json!({
        "name": "ce2-greptime-dup",
        "type": "greptimedb",
        "server": "http://127.0.0.1:1",
        "database": "public",
        "table": "ce2_dup",
        "format": "sql",
        "precision": "ms",
    });
    let (status, answer) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", body.clone())
        .await;
    assert_eq!(status, 201, "v5 connector creates: {answer}");
    let (status, answer) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", body)
        .await;
    assert_eq!(status, 409, "v5 duplicate fails: {answer}");
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
/// A publish without a matching rule writes no row.
#[tokio::test]
#[ignore = "needs a real server (see GREPTIMEDB_* env)"]
async fn greptimedb_unknown_nomatch_through_broker() {
    let endpoint = qual_env("GREPTIMEDB_ENDPOINT");
    let database = qual_env("GREPTIMEDB_DATABASE");
    let base = check_ident(&qual_env("GREPTIMEDB_TABLE"));
    let table = format!("{base}_nomatch");
    let client = reqwest::Client::builder()
        // Reason: 30 s bounds one slow query.
        // Source: pipeline timeout budget.
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    let sql_url = format!("{}/sql?db={database}", api_base(&endpoint));
    drop_table(&client, &sql_url, &table).await;
    ensure_ms_table(&client, &sql_url, &table).await;

    let broker = TestBroker::start("").await;
    // An unknown connector id answers 404 with a body, never an empty answer.
    let (status, body) = broker
        .api(
            reqwest::Method::GET,
            "/api/v5/connectors/ce2-greptime-no-such-connector",
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
            "ce2-greptime-nomatch",
            "greptimedb",
            greptime_config(
                &endpoint,
                &database,
                &table,
                "sql_insert",
                "millisecond",
                1,
                None,
            ),
        )
        .await;
    broker
        .create_forward_rule(
            "ce2-greptime-nomatch-rule",
            "ce2/greptime/nomatch",
            "ce2-greptime-nomatch",
        )
        .await;
    broker
        .publish(
            "ce2-greptime-nomatch-pub",
            "ce2/greptime/nomatch",
            br#"{"device": "d", "val": 1.0, "seq": 1}"#,
            0,
        )
        .await;
    wait_count(&client, &sql_url, &table, 1).await;
    // A topic without a rule writes no further row.
    broker
        .publish(
            "ce2-greptime-nomatch-pub2",
            "ce2/greptime/nomatch-other",
            br#"{"device": "d", "val": 2.0, "seq": 2}"#,
            0,
        )
        .await;
    // Reason: 2 s lets a wrong forward arrive.
    // Source: loopback delivery is instant.
    tokio::time::sleep(Duration::from_secs(2)).await;
    wait_count(&client, &sql_url, &table, 1).await;

    drop_table(&client, &sql_url, &table).await;
    broker.finish().await;
}

/// Connector and rule persist across a broker restart.
///
/// Uses the v5 connector store, which replays on boot.
/// Publishes before and after the restart; both rows return.
#[tokio::test]
#[ignore = "needs a real server (see GREPTIMEDB_* env)"]
async fn greptimedb_persist_across_restart_through_broker() {
    let endpoint = qual_env("GREPTIMEDB_ENDPOINT");
    let database = qual_env("GREPTIMEDB_DATABASE");
    let base = check_ident(&qual_env("GREPTIMEDB_TABLE"));
    let table = format!("{base}_restart");
    let client = reqwest::Client::builder()
        // Reason: 30 s bounds one slow query.
        // Source: pipeline timeout budget.
        .timeout(Duration::from_secs(30))
        .build()
        .expect("query client builds");
    let sql_url = format!("{}/sql?db={database}", api_base(&endpoint));
    drop_table(&client, &sql_url, &table).await;
    ensure_ms_table(&client, &sql_url, &table).await;

    let mut broker = TestBroker::start("").await;
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "name": "ce2-greptime-restart",
                "type": "greptimedb",
                "server": endpoint,
                "database": database,
                "table": table,
                "format": "sql",
                "precision": "ms",
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 connector creates: {body}");
    broker
        .create_forward_rule(
            "ce2-greptime-restart-rule",
            "ce2/greptime/restart",
            "ce2-greptime-restart",
        )
        .await;

    broker
        .publish(
            "ce2-greptime-restart-pub1",
            "ce2/greptime/restart",
            br#"{"device": "dev-r", "val": 1.0, "seq": 11}"#,
            0,
        )
        .await;
    wait_count(&client, &sql_url, &table, 1).await;

    broker.stop_and_restart_orderly().await;

    broker
        .publish(
            "ce2-greptime-restart-pub2",
            "ce2/greptime/restart",
            br#"{"device": "dev-r", "val": 2.0, "seq": 12}"#,
            0,
        )
        .await;
    wait_count(&client, &sql_url, &table, 2).await;
    let seqs = query_cells(
        &sql_body(
            &client,
            &sql_url,
            &format!("SELECT seq FROM {table} ORDER BY seq"),
        )
        .await,
    );
    assert_eq!(seqs, vec![11, 12], "both epochs survive: {seqs:?}");

    drop_table(&client, &sql_url, &table).await;
    broker.finish().await;
}
