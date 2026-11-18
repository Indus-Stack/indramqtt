//! Tests for the MySQL connector.
//!
//! Each test boots one real broker.
//! It creates the connector and rule.
//! It publishes on a real socket.
//! It reads each row back from the server.

use std::time::{Duration, Instant};

use super::e2e_support::TestBroker;
use mysql_async::prelude::Queryable;
use serde_json::{json, Value};

/// Bound for one server round trip.
/// Reason: 60 s covers a slow server insert.
/// Source: pipeline timeout budget.
const ROW_WAIT: Duration = Duration::from_secs(60);
/// Pause between server polls.
/// Reason: 200 ms polls fast without load.
/// Source: loopback delivery is instant.
const POLL_WAIT: Duration = Duration::from_millis(200);
/// Quiet bound after an unmatched publish.
/// Reason: 2 s lets a wrong forward arrive.
/// Source: loopback delivery is instant.
const QUIET_WAIT: Duration = Duration::from_secs(2);
/// Rows of one size-triggered batch.
/// Reason: 3 proves a batch, not one send.
/// Source: ticket batch case.
const BATCH_ROWS: usize = 3;
/// Linger of the time-triggered test.
/// Reason: 200 ms flushes fast but stays above scheduling noise.
/// Source: pipeline timeout budget.
const TIMEOUT_MS: u64 = 200;
/// Pool bound of the pool test.
/// Reason: 2 bounds server sessions per the ticket.
/// Source: ticket pool case.
const POOL_SIZE: usize = 2;
/// Rows of the concurrent pool load.
/// Reason: 20 exceeds one batch and exercises the pool.
/// Source: ticket pool case.
const POOL_ROWS: usize = 20;
/// Rows of the forward and auth checks.
/// Reason: 1 row proves the template binds each column.
/// Source: ticket forward case.
const ONE_ROW: u64 = 1;
/// Stall bound before a size batch may flush.
/// Reason: 500 ms proves two rows stay pending without a trigger.
/// Source: pipeline timeout budget.
const PENDING_WAIT: Duration = Duration::from_millis(500);
/// Pause after one publish before a quiet read.
/// Reason: 300 ms lets the linger task flush the row.
/// Source: loopback delivery is instant.
const FLUSH_WAIT: Duration = Duration::from_millis(300);

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

/// Checks that a value stays a plain identifier.
/// Panics on any other input.
/// Reason: names enter SQL text directly.
/// Source: connector qualification guard.
fn check_ident(name: &str, value: &str) -> String {
    assert!(
        !value.is_empty()
            && value
                .bytes()
                .all(|b| b == b'_' || b.is_ascii_alphanumeric()),
        "{name} must stay a plain identifier"
    );
    value.to_string()
}

/// Unique suffix for each run.
/// Reason: a fresh table per run holds no old rows.
/// Source: harness stale-row guard.
fn run_suffix() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|span| span.as_nanos())
        .unwrap_or(0);
    format!("{nanos}_{}", std::process::id())
}

/// Builds one connection URL.
/// Reason: one helper keeps user and password in one place.
fn mysql_url(host: &str, port: u16, user: &str, password: &str, database: &str) -> String {
    format!("mysql://{user}:{password}@{host}:{port}/{database}")
}

/// Opens one observer pool with a single session.
/// Reason: one session bounds the observer to one connection.
/// Source: driver pool-constraint guard.
async fn open_pool(url: &str) -> mysql_async::Pool {
    let opts = mysql_async::Opts::from_url(url).expect("url parses");
    let constraints = mysql_async::PoolConstraints::new(0, 1).expect("pool bounds");
    let pool_opts = mysql_async::PoolOpts::default().with_constraints(constraints);
    let builder = mysql_async::OptsBuilder::from_opts(opts).pool_opts(pool_opts);
    mysql_async::Pool::new(mysql_async::Opts::from(builder))
}

/// Drops one table when it exists.
async fn drop_table(pool: &mysql_async::Pool, table: &str) {
    let mut conn = pool.get_conn().await.expect("pool connects");
    conn.query_drop(format!("DROP TABLE IF EXISTS `{table}`"))
        .await
        .expect("drop runs");
}

/// Creates one narrow event table.
async fn ensure_table(pool: &mysql_async::Pool, table: &str) {
    let mut conn = pool.get_conn().await.expect("pool connects");
    conn.query_drop(format!(
        "CREATE TABLE IF NOT EXISTS `{table}` \
        (`topic` VARCHAR(256) NOT NULL, \
        `qos` TINYINT NOT NULL, \
        `payload` VARCHAR(512) NOT NULL)"
    ))
    .await
    .expect("create runs");
}

/// Reads the row count of one table.
async fn count_rows(pool: &mysql_async::Pool, table: &str) -> u64 {
    let mut conn = pool.get_conn().await.expect("pool connects");
    let count: Option<u64> = conn
        .query_first(format!("SELECT COUNT(*) FROM `{table}`"))
        .await
        .expect("count runs");
    count.unwrap_or(0)
}

/// Waits until one table holds the expected rows.
async fn wait_rows(pool: &mysql_async::Pool, table: &str, expect: u64) {
    let deadline = Instant::now() + ROW_WAIT;
    loop {
        let seen = count_rows(pool, table).await;
        if seen == expect {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "table {table} holds {seen}, want {expect}"
        );
        tokio::time::sleep(POLL_WAIT).await;
    }
}

/// Reads one row back as text triple.
async fn read_first_row(pool: &mysql_async::Pool, table: &str) -> (String, String, String) {
    let mut conn = pool.get_conn().await.expect("pool connects");
    let row: Option<(String, u8, String)> = conn
        .query_first(format!(
            "SELECT `topic`, `qos`, `payload` FROM `{table}` LIMIT 1"
        ))
        .await
        .expect("row reads");
    let (topic, qos, payload) = row.expect("one row exists");
    (topic, qos.to_string(), payload)
}

/// Counts server sessions of one user.
/// Reason: the pool test bounds open sessions.
/// Source: ticket pool case.
async fn session_count(pool: &mysql_async::Pool, user: &str, database: &str) -> u64 {
    let user = check_ident("user", user);
    let database = check_ident("database", database);
    let mut conn = pool.get_conn().await.expect("pool connects");
    let count: Option<u64> = conn
        .query_first(format!(
            "SELECT COUNT(*) FROM information_schema.processlist \
            WHERE `USER` = '{user}' AND `DB` = '{database}'"
        ))
        .await
        .expect("processlist reads");
    count.unwrap_or(0)
}

/// Builds one v1 connector config.
/// Reason: one helper keeps the field names in one place.
fn mysql_config(url: &str, table: &str, extra: Value) -> Value {
    let mut config = json!({
        "connection_url": url,
        "sql_template": format!("INSERT INTO `{table}` (`topic`, `qos`, `payload`) VALUES (?, ?, ?)"),
    });
    for (key, val) in extra.as_object().expect("extra is an object") {
        config[key] = val.clone();
    }
    config
}

/// One forward write through the broker, read back from the server.
///
/// Covers sql_template. The config omits pool and batch fields, so the
/// broker applies the defaults (pool 10, batch 100, timeout 50 ms).
#[tokio::test]
#[ignore = "needs a real server (see MYSQL_* env)"]
async fn mysql_forward_through_broker() {
    let host = qual_env("MYSQL_HOST");
    let port: u16 = qual_env("MYSQL_PORT").parse().expect("port parses");
    let database = check_ident("database", &qual_env("MYSQL_DATABASE"));
    let user = qual_env("MYSQL_USER");
    let password = qual_env("MYSQL_PASSWORD");
    let base = check_ident("table", &qual_env("MYSQL_TABLE"));
    let table = format!("{base}_fwd_{}", run_suffix());
    check_ident("table", &table);
    let url = mysql_url(&host, port, &user, &password, &database);

    let pool = open_pool(&url).await;
    drop_table(&pool, &table).await;
    ensure_table(&pool, &table).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-mysql-fwd",
            "mysql",
            mysql_config(&url, &table, json!({})),
        )
        .await;
    let rule = broker
        .create_forward_rule("ce2-mysql-fwd-rule", "ce2/mysql/fwd", "ce2-mysql-fwd")
        .await;

    broker
        .publish("ce2-mysql-fwd-pub", "ce2/mysql/fwd", br#"{"seq":1}"#, 0)
        .await;
    broker
        .wait_for_action_success(&rule, ONE_ROW, ROW_WAIT)
        .await;
    wait_rows(&pool, &table, ONE_ROW).await;
    let (topic, qos, payload) = read_first_row(&pool, &table).await;
    assert_eq!(topic, "ce2/mysql/fwd", "template binds the topic");
    assert_eq!(qos, "0", "template binds the QoS");
    assert!(payload.contains("\"seq\":1"), "payload arrives: {payload}");

    drop_table(&pool, &table).await;
    broker.finish().await;
}

/// Both password plugins deliver; a wrong password fails closed.
///
/// Covers connection_url. The wrong-password rule counts a failure and
/// writes no row.
#[tokio::test]
#[ignore = "needs a real server (see MYSQL_* env)"]
async fn mysql_auth_through_broker() {
    let host = qual_env("MYSQL_HOST");
    let port: u16 = qual_env("MYSQL_PORT").parse().expect("port parses");
    let database = check_ident("database", &qual_env("MYSQL_DATABASE"));
    let user = qual_env("MYSQL_USER");
    let native_user = qual_env("MYSQL_NATIVE_USER");
    let password = qual_env("MYSQL_PASSWORD");
    let base = check_ident("table", &qual_env("MYSQL_TABLE"));
    let suffix = run_suffix();
    let native_table = format!("{base}_authn_{suffix}");
    let sha2_table = format!("{base}_auths_{suffix}");
    let bad_table = format!("{base}_authb_{suffix}");
    check_ident("table", &native_table);
    check_ident("table", &sha2_table);
    check_ident("table", &bad_table);
    let native_url = mysql_url(&host, port, &native_user, &password, &database);
    let sha2_url = mysql_url(&host, port, &user, &password, &database);
    let bad_url = mysql_url(&host, port, &native_user, "wrongpass1", &database);

    let pool = open_pool(&native_url).await;
    for table in [&native_table, &sha2_table, &bad_table] {
        drop_table(&pool, table).await;
        ensure_table(&pool, table).await;
    }

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-mysql-native",
            "mysql",
            mysql_config(&native_url, &native_table, json!({"batch_size": 1})),
        )
        .await;
    broker
        .create_connector(
            "ce2-mysql-sha2",
            "mysql",
            mysql_config(&sha2_url, &sha2_table, json!({"batch_size": 1})),
        )
        .await;
    broker
        .create_connector(
            "ce2-mysql-bad",
            "mysql",
            mysql_config(&bad_url, &bad_table, json!({"batch_size": 1})),
        )
        .await;
    let native_rule = broker
        .create_forward_rule(
            "ce2-mysql-native-rule",
            "ce2/mysql/auth/native",
            "ce2-mysql-native",
        )
        .await;
    let sha2_rule = broker
        .create_forward_rule(
            "ce2-mysql-sha2-rule",
            "ce2/mysql/auth/sha2",
            "ce2-mysql-sha2",
        )
        .await;
    let bad_rule = broker
        .create_forward_rule("ce2-mysql-bad-rule", "ce2/mysql/auth/bad", "ce2-mysql-bad")
        .await;

    broker
        .publish(
            "ce2-mysql-authn-pub",
            "ce2/mysql/auth/native",
            br#"{"seq":1}"#,
            0,
        )
        .await;
    broker
        .publish(
            "ce2-mysql-auths-pub",
            "ce2/mysql/auth/sha2",
            br#"{"seq":2}"#,
            0,
        )
        .await;
    broker
        .wait_for_action_success(&native_rule, ONE_ROW, ROW_WAIT)
        .await;
    broker
        .wait_for_action_success(&sha2_rule, ONE_ROW, ROW_WAIT)
        .await;
    wait_rows(&pool, &native_table, ONE_ROW).await;
    wait_rows(&pool, &sha2_table, ONE_ROW).await;

    broker
        .publish(
            "ce2-mysql-authb-pub",
            "ce2/mysql/auth/bad",
            br#"{"seq":3}"#,
            0,
        )
        .await;
    let (_, _, failed) = broker
        .wait_for_action_failures(&bad_rule, ONE_ROW, ROW_WAIT)
        .await;
    assert!(failed >= ONE_ROW, "wrong password fails the action");
    tokio::time::sleep(FLUSH_WAIT).await;
    assert_eq!(
        count_rows(&pool, &bad_table).await,
        0,
        "wrong password writes no row"
    );

    for table in [&native_table, &sha2_table, &bad_table] {
        drop_table(&pool, table).await;
    }
    broker.finish().await;
}

/// A template without exactly three markers fails at create time.
///
/// Needs no server. The create answers 400 and stores nothing.
#[tokio::test]
#[ignore = "needs no server; the refusal runs in qualification"]
async fn mysql_sql_template_rejected_through_broker() {
    let broker = TestBroker::start("").await;
    for (tag, template) in [
        ("two", "INSERT INTO t (`topic`, `qos`) VALUES (?, ?)"),
        (
            "four",
            "INSERT INTO t (`a`, `b`, `c`, `d`) VALUES (?, ?, ?, ?)",
        ),
    ] {
        let id = format!("ce2-mysql-bad-tpl-{tag}");
        let (status, body) = broker
            .api(
                reqwest::Method::POST,
                "/api/v1/connectors",
                json!({
                    "id": id,
                    "kind": "mysql",
                    "config": {
                        "connection_url": "mysql://u:p@127.0.0.1:3306/db",
                        "sql_template": template,
                    },
                }),
            )
            .await;
        assert_eq!(status, 400, "wrong marker count refuses: {body}");
        assert!(
            body.to_string().contains("must carry exactly 3 ? markers"),
            "error names the marker rule: {body}"
        );
        let listed = broker.list_connectors().await;
        assert!(
            !listed
                .as_array()
                .is_some_and(|rows| rows.iter().any(|row| row["id"] == id)),
            "refused connector stores nothing"
        );
    }
    broker.finish().await;
}

/// A small pool bounds open sessions under concurrent load.
///
/// Covers pool_size. The observer holds one session, so the read stays
/// at or below three while the sink holds at most two.
#[tokio::test]
#[ignore = "needs a real server (see MYSQL_* env)"]
async fn mysql_pool_size_through_broker() {
    let host = qual_env("MYSQL_HOST");
    let port: u16 = qual_env("MYSQL_PORT").parse().expect("port parses");
    let database = check_ident("database", &qual_env("MYSQL_DATABASE"));
    let user = qual_env("MYSQL_USER");
    let password = qual_env("MYSQL_PASSWORD");
    let base = check_ident("table", &qual_env("MYSQL_TABLE"));
    let table = format!("{base}_pool_{}", run_suffix());
    check_ident("table", &table);
    let url = mysql_url(&host, port, &user, &password, &database);

    let pool = open_pool(&url).await;
    drop_table(&pool, &table).await;
    ensure_table(&pool, &table).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-mysql-pool",
            "mysql",
            mysql_config(
                &url,
                &table,
                json!({"pool_size": POOL_SIZE, "batch_size": 5, "batch_timeout_ms": 60_000}),
            ),
        )
        .await;
    let rule = broker
        .create_forward_rule("ce2-mysql-pool-rule", "ce2/mysql/pool", "ce2-mysql-pool")
        .await;

    let ids: Vec<String> = (0..POOL_ROWS)
        .map(|seq| format!("ce2-mysql-pool-pub-{seq}"))
        .collect();
    let payloads: Vec<Vec<u8>> = (0..POOL_ROWS)
        .map(|seq| format!(r#"{{"seq":{seq}}}"#).into_bytes())
        .collect();
    futures::future::join_all(
        ids.iter()
            .zip(payloads.iter())
            .map(|(id, payload)| broker.publish(id, "ce2/mysql/pool", payload, 0)),
    )
    .await;
    let mut max_sessions = 0u64;
    let deadline = Instant::now() + ROW_WAIT;
    loop {
        let sessions = session_count(&pool, &user, &database).await;
        max_sessions = max_sessions.max(sessions);
        if count_rows(&pool, &table).await == POOL_ROWS as u64 {
            break;
        }
        assert!(Instant::now() < deadline, "pool rows arrive late");
        tokio::time::sleep(POLL_WAIT).await;
    }
    broker
        .wait_for_action_success(&rule, POOL_ROWS as u64, ROW_WAIT)
        .await;
    assert!(
        max_sessions <= (POOL_SIZE as u64) + ONE_ROW,
        "sink holds at most {POOL_SIZE} plus the observer: saw {max_sessions}"
    );

    drop_table(&pool, &table).await;
    broker.finish().await;
}

/// A full batch flushes as one batch.
///
/// Covers batch_size. Two rows stay pending; the third row flushes all
/// three with no timer involved.
#[tokio::test]
#[ignore = "needs a real server (see MYSQL_* env)"]
async fn mysql_batch_size_through_broker() {
    let host = qual_env("MYSQL_HOST");
    let port: u16 = qual_env("MYSQL_PORT").parse().expect("port parses");
    let database = check_ident("database", &qual_env("MYSQL_DATABASE"));
    let user = qual_env("MYSQL_USER");
    let password = qual_env("MYSQL_PASSWORD");
    let base = check_ident("table", &qual_env("MYSQL_TABLE"));
    let table = format!("{base}_batch_{}", run_suffix());
    check_ident("table", &table);
    let url = mysql_url(&host, port, &user, &password, &database);

    let pool = open_pool(&url).await;
    drop_table(&pool, &table).await;
    ensure_table(&pool, &table).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-mysql-batch",
            "mysql",
            mysql_config(
                &url,
                &table,
                json!({"batch_size": BATCH_ROWS, "batch_timeout_ms": 60_000}),
            ),
        )
        .await;
    broker
        .create_forward_rule("ce2-mysql-batch-rule", "ce2/mysql/batch", "ce2-mysql-batch")
        .await;

    let mut publisher = broker.connect("ce2-mysql-batch-pub").await;
    for seq in 0..2 {
        publisher
            .publish(
                "ce2/mysql/batch",
                format!(r#"{{"seq":{seq}}}"#).as_bytes(),
                0,
            )
            .await;
    }
    drop(publisher);
    tokio::time::sleep(PENDING_WAIT).await;
    assert_eq!(
        count_rows(&pool, &table).await,
        0,
        "two rows stay pending below the batch size"
    );
    broker
        .publish(
            "ce2-mysql-batch-pub3",
            "ce2/mysql/batch",
            br#"{"seq":2}"#,
            0,
        )
        .await;
    wait_rows(&pool, &table, BATCH_ROWS as u64).await;

    drop_table(&pool, &table).await;
    broker.finish().await;
}

/// A stale row flushes on its own after the timeout.
///
/// Covers batch_timeout_ms. One row arrives with no second publish.
#[tokio::test]
#[ignore = "needs a real server (see MYSQL_* env)"]
async fn mysql_batch_timeout_through_broker() {
    let host = qual_env("MYSQL_HOST");
    let port: u16 = qual_env("MYSQL_PORT").parse().expect("port parses");
    let database = check_ident("database", &qual_env("MYSQL_DATABASE"));
    let user = qual_env("MYSQL_USER");
    let password = qual_env("MYSQL_PASSWORD");
    let base = check_ident("table", &qual_env("MYSQL_TABLE"));
    let table = format!("{base}_linger_{}", run_suffix());
    check_ident("table", &table);
    let url = mysql_url(&host, port, &user, &password, &database);

    let pool = open_pool(&url).await;
    drop_table(&pool, &table).await;
    ensure_table(&pool, &table).await;

    let broker = TestBroker::start("").await;
    broker
        .create_connector(
            "ce2-mysql-linger",
            "mysql",
            mysql_config(
                &url,
                &table,
                json!({"batch_size": 100, "batch_timeout_ms": TIMEOUT_MS}),
            ),
        )
        .await;
    broker
        .create_forward_rule(
            "ce2-mysql-linger-rule",
            "ce2/mysql/linger",
            "ce2-mysql-linger",
        )
        .await;

    broker
        .publish(
            "ce2-mysql-linger-pub",
            "ce2/mysql/linger",
            br#"{"seq":1}"#,
            0,
        )
        .await;
    wait_rows(&pool, &table, ONE_ROW).await;

    drop_table(&pool, &table).await;
    broker.finish().await;
}

/// A duplicate v5 connector id fails with the documented code.
///
/// Needs no server.
#[tokio::test]
#[ignore = "needs no server; the v5 duplicate path runs in qualification"]
async fn mysql_duplicate_through_broker() {
    let broker = TestBroker::start("").await;
    let body = json!({
        "name": "ce2-mysql-dup",
        "type": "mysql",
        "server": "127.0.0.1:3306",
        "database": "qual",
        "username": "root",
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

/// Unknown ids and unmatched topics through the broker.
///
/// An unknown connector id answers 404 with a body.
/// A publish without a matching rule writes no row.
#[tokio::test]
#[ignore = "needs a real server (see MYSQL_* env)"]
async fn mysql_unknown_nomatch_through_broker() {
    let host = qual_env("MYSQL_HOST");
    let port: u16 = qual_env("MYSQL_PORT").parse().expect("port parses");
    let database = check_ident("database", &qual_env("MYSQL_DATABASE"));
    let user = qual_env("MYSQL_USER");
    let password = qual_env("MYSQL_PASSWORD");
    let base = check_ident("table", &qual_env("MYSQL_TABLE"));
    let table = format!("{base}_nomatch_{}", run_suffix());
    check_ident("table", &table);
    let url = mysql_url(&host, port, &user, &password, &database);

    let pool = open_pool(&url).await;
    drop_table(&pool, &table).await;
    ensure_table(&pool, &table).await;

    let broker = TestBroker::start("").await;
    let (status, body) = broker
        .api(
            reqwest::Method::GET,
            "/api/v5/connectors/ce2-mysql-no-such-connector",
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
            "ce2-mysql-nomatch",
            "mysql",
            mysql_config(&url, &table, json!({"batch_size": 1})),
        )
        .await;
    broker
        .create_forward_rule(
            "ce2-mysql-nomatch-rule",
            "ce2/mysql/nomatch",
            "ce2-mysql-nomatch",
        )
        .await;
    broker
        .publish(
            "ce2-mysql-nomatch-pub",
            "ce2/mysql/nomatch",
            br#"{"seq":1}"#,
            0,
        )
        .await;
    wait_rows(&pool, &table, ONE_ROW).await;
    broker
        .publish(
            "ce2-mysql-nomatch-pub2",
            "ce2/mysql/nomatch-other",
            br#"{"seq":2}"#,
            0,
        )
        .await;
    tokio::time::sleep(QUIET_WAIT).await;
    assert_eq!(
        count_rows(&pool, &table).await,
        ONE_ROW,
        "unmatched topic adds no row"
    );

    drop_table(&pool, &table).await;
    broker.finish().await;
}

/// Connector and rule persist across a broker restart.
///
/// Uses the v5 connector store, which replays on boot.
/// Publishes before and after the restart; both rows return.
#[tokio::test]
#[ignore = "needs a real server (see MYSQL_* env)"]
async fn mysql_persist_across_restart_through_broker() {
    let host = qual_env("MYSQL_HOST");
    let port: u16 = qual_env("MYSQL_PORT").parse().expect("port parses");
    let database = check_ident("database", &qual_env("MYSQL_DATABASE"));
    let user = qual_env("MYSQL_USER");
    let password = qual_env("MYSQL_PASSWORD");
    let base = check_ident("table", &qual_env("MYSQL_TABLE"));
    let table = format!("{base}_restart_{}", run_suffix());
    check_ident("table", &table);
    let url = mysql_url(&host, port, &user, &password, &database);

    let pool = open_pool(&url).await;
    drop_table(&pool, &table).await;
    ensure_table(&pool, &table).await;
    let template = format!("INSERT INTO `{table}` (`topic`, `qos`, `payload`) VALUES (?, ?, ?)");

    let mut broker = TestBroker::start("").await;
    let server = format!("{host}:{port}");
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/connectors",
            json!({
                "name": "ce2-mysql-restart",
                "type": "mysql",
                "server": server,
                "database": database,
                "username": user,
                "password": password,
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 connector creates: {body}");
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/actions",
            json!({
                "name": "ce2-mysql-restart-act",
                "type": "mysql",
                "connector": "ce2-mysql-restart",
                "parameters": {"sql": template},
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 action creates: {body}");
    let (status, body) = broker
        .api(
            reqwest::Method::POST,
            "/api/v5/rules",
            json!({
                "id": "ce2-mysql-restart-rule",
                "name": "ce2-mysql-restart-rule",
                "sql": "SELECT * FROM \"ce2/mysql/restart\"",
                "actions": ["ce2-mysql-restart-act"],
            }),
        )
        .await;
    assert_eq!(status, 201, "v5 rule creates: {body}");

    broker
        .publish(
            "ce2-mysql-restart-pub1",
            "ce2/mysql/restart",
            br#"{"seq":11}"#,
            0,
        )
        .await;
    wait_rows(&pool, &table, ONE_ROW).await;

    broker.stop_and_restart_orderly().await;

    broker
        .publish(
            "ce2-mysql-restart-pub2",
            "ce2/mysql/restart",
            br#"{"seq":12}"#,
            0,
        )
        .await;
    wait_rows(&pool, &table, 2).await;

    drop_table(&pool, &table).await;
    broker.finish().await;
}
