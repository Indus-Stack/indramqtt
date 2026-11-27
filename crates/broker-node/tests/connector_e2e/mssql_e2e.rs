//! End-to-end tests for the Microsoft SQL Server connector.
//!
//! Each test boots one broker.
//! It creates the connector and forward rule.
//! It publishes on a real MQTT socket.
//! It reads each row back from the SQL Server database.

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::compat::TokioAsyncWriteCompatExt;

use super::e2e_support::{MqttClient, TestBroker};

/// Bound for one server round trip.
/// Reason: 30 s covers a slow server insert.
/// Source: pipeline timeout budget.
const ROW_WAIT: Duration = Duration::from_secs(30);

/// Pause between server polls.
/// Reason: 200 ms polls fast without load.
/// Source: loopback delivery is instant.
const POLL_WAIT: Duration = Duration::from_millis(200);

/// Bound for a quiet negative check.
/// Reason: 5 s absorbs late writes per CTO ruling 2026-11-16 (b).6.
/// Source: wave ruling 2026-11-16.
const NEGATIVE_WAIT: Duration = Duration::from_secs(5);

/// Timeout for the server outage recovery.
/// Reason: 240 s covers container restart under load.
/// Source: ticket outage case.
const OUTAGE_WAIT: Duration = Duration::from_secs(240);

/// Timeout for post-outage message delivery.
/// Reason: 120 s allows retry queues to drain.
/// Source: ticket outage case.
const POST_OUTAGE_WAIT: Duration = Duration::from_secs(120);

/// Pause between outage messages.
/// Reason: 500 ms paces message rate during outage.
/// Source: ticket outage case.
const OUTAGE_POLL: Duration = Duration::from_millis(500);

/// Pause between post-outage messages.
/// Reason: 2 s paces message rate after outage.
/// Source: ticket outage case.
const POST_OUTAGE_POLL: Duration = Duration::from_secs(2);

type MssqlClient = tiberius::Client<tokio_util::compat::Compat<tokio::net::TcpStream>>;

struct EnvConfig {
    host: String,
    port: u16,
    database: String,
    table: String,
    user: String,
    password: String,
}

fn qual_env(name: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| panic!("{name} must point at a real server for qualification"))
}

fn read_env() -> EnvConfig {
    let port = qual_env("MSSQL_PORT")
        .parse()
        .expect("MSSQL_PORT must be a port");
    EnvConfig {
        host: qual_env("MSSQL_HOST"),
        port,
        database: qual_env("MSSQL_DATABASE"),
        table: qual_env("MSSQL_TABLE"),
        user: qual_env("MSSQL_USER"),
        password: qual_env("MSSQL_PASSWORD"),
    }
}

async fn connect_raw(
    host: &str,
    port: u16,
    user: &str,
    password: &str,
    database: &str,
) -> Result<MssqlClient, String> {
    let mut config = tiberius::Config::new();
    config.host(host);
    config.port(port);
    config.database(database);
    config.authentication(tiberius::AuthMethod::sql_server(user, password));
    config.trust_cert();
    config.encryption(tiberius::EncryptionLevel::NotSupported);
    let tcp = tokio::time::timeout(ROW_WAIT, tokio::net::TcpStream::connect(config.get_addr()))
        .await
        .map_err(|_| "tcp connect timed out".to_string())?
        .map_err(|e| format!("tcp connect failed: {e}"))?;
    tcp.set_nodelay(true)
        .map_err(|e| format!("tcp nodelay failed: {e}"))?;
    tokio::time::timeout(
        ROW_WAIT,
        tiberius::Client::connect(config, tcp.compat_write()),
    )
    .await
    .map_err(|_| "mssql login timed out".to_string())?
    .map_err(|e| format!("mssql login failed: {e}"))
}

async fn connect_client(env: &EnvConfig) -> Result<MssqlClient, String> {
    connect_raw(&env.host, env.port, &env.user, &env.password, &env.database).await
}

async fn exec_query(client: &mut MssqlClient, sql: impl Into<String>) {
    client
        .simple_query(sql.into())
        .await
        .expect("query")
        .into_first_result()
        .await
        .expect("drain");
}

async fn drop_table(client: &mut MssqlClient, table: &str) {
    exec_query(client, format!("DROP TABLE IF EXISTS [{table}]")).await;
}

async fn drop_and_create_table(client: &mut MssqlClient, table: &str) {
    let sql = format!(
        "DROP TABLE IF EXISTS [{table}]; \
         CREATE TABLE [{table}] (\
         event_time DATETIMEOFFSET NOT NULL, topic NVARCHAR(256) NOT NULL, \
         client_id NVARCHAR(256) NOT NULL, qos BIGINT NOT NULL, payload NVARCHAR(MAX) NOT NULL)"
    );
    exec_query(client, sql).await;
}

async fn count_rows(client: &mut MssqlClient, table: &str) -> i32 {
    let rows = client
        .simple_query(format!("SELECT COUNT(*) FROM [{table}]"))
        .await
        .expect("count query")
        .into_first_result()
        .await
        .expect("count rows");
    rows.first().and_then(|r| r.get(0)).unwrap_or(0)
}

async fn wait_rows(client: &mut MssqlClient, table: &str, expected: i32, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        let seen = count_rows(client, table).await;
        if seen == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "table {table} holds {seen}, want {expected}"
        );
        tokio::time::sleep(POLL_WAIT).await;
    }
}

async fn read_first_row(client: &mut MssqlClient, table: &str) -> (String, String, String) {
    let rows = client
        .simple_query(format!("SELECT topic, client_id, payload FROM [{table}]"))
        .await
        .expect("select row")
        .into_first_result()
        .await
        .expect("select rows");
    let row = rows.first().expect("table must hold at least one row");
    (
        row.get::<&str, _>(0).expect("topic").to_string(),
        row.get::<&str, _>(1).expect("client_id").to_string(),
        row.get::<&str, _>(2).expect("payload").to_string(),
    )
}

fn mssql_config(env: &EnvConfig, table: &str) -> Value {
    json!({
        "host": env.host, "port": env.port, "database": env.database,
        "table_template": table,
        "auth": {"type": "sqlpassword", "username": env.user, "password": env.password},
        "batch_size": 1,
    })
}

fn outage_config(env: &EnvConfig, table: &str, retries: usize) -> Value {
    let mut cfg = mssql_config(env, table);
    cfg["timeout_ms"] = json!(500);
    cfg["initial_backoff_ms"] = json!(50);
    cfg["max_backoff_ms"] = json!(200);
    cfg["batch_size"] = json!(1);
    cfg["max_retries"] = json!(retries);
    cfg
}

async fn setup_forward(broker: &TestBroker, name: &str, topic: &str, cfg: Value) -> String {
    let conn = format!("{name}-conn");
    broker.create_connector(&conn, "mssql", cfg).await;
    broker
        .create_forward_rule(&format!("{name}-rule"), topic, &conn)
        .await
}

async fn setup_test(case: &str) -> (EnvConfig, String, MssqlClient, TestBroker) {
    let env = read_env();
    let table = format!("{}_{case}", env.table);
    let mut client = connect_client(&env).await.expect("connect client");
    drop_and_create_table(&mut client, &table).await;
    let broker = TestBroker::start("").await;
    (env, table, client, broker)
}

async fn finish_test(mut client: MssqlClient, table: &str, broker: TestBroker) {
    drop_table(&mut client, table).await;
    broker.finish().await;
}

async fn assert_create_err(broker: &TestBroker, id: &str, cfg: Value, expect: &str) {
    let (s, b) = broker
        .api(
            reqwest::Method::POST,
            "/api/v1/connectors",
            json!({"id": id, "kind": "mssql", "config": cfg}),
        )
        .await;
    assert_eq!(s, 400, "{id} answers 400");
    assert!(
        b.to_string().contains(expect),
        "body text contains {expect:?}: {b}"
    );
}

async fn post_v5_201(broker: &TestBroker, path: &str, body: Value) {
    let (s, b) = broker.api(reqwest::Method::POST, path, body).await;
    assert_eq!(s, 201, "{path} answers 201: {b}");
}

/// Case 1: verifies standard end-to-end publish, topic templates, and config errors.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_end_to_end_through_broker() {
    let (env, table, mut client, broker) = setup_test("end_to_end").await;
    drop_and_create_table(&mut client, "ce2_mssql_tt").await;

    let mut cfg1 = mssql_config(&env, "${topic}");
    cfg1["query_mode"] = json!({"mode": "insertjson"});
    setup_forward(&broker, "ce2-tt", "ce2/mssql/tt", cfg1).await;

    let payload = json!({"client_id": "cid-e2e-1", "test": "payload"}).to_string();
    broker
        .publish("pub-e2e", "ce2/mssql/tt", payload.as_bytes(), 1)
        .await;
    wait_rows(&mut client, "ce2_mssql_tt", 1, ROW_WAIT).await;
    let (topic, cid, pl) = read_first_row(&mut client, "ce2_mssql_tt").await;
    assert_eq!(topic, "ce2/mssql/tt");
    assert_eq!(cid, "cid-e2e-1");
    assert_eq!(pl, payload);

    let mut cfg_bad_pw = mssql_config(&env, &table);
    cfg_bad_pw["auth"] =
        json!({"type": "sqlpassword", "username": env.user, "password": "wrong-password"});
    let r_bad_pw = setup_forward(&broker, "ce2-bad-pw", "ce2/mssql/bad_pw", cfg_bad_pw).await;
    broker
        .publish("pub-bad-pw", "ce2/mssql/bad_pw", payload.as_bytes(), 1)
        .await;
    let (_, _, f_pw) = broker
        .wait_for_action_failures(&r_bad_pw, 1, ROW_WAIT)
        .await;
    assert!(f_pw >= 1, "wrong password increments failed counter");
    assert_eq!(
        count_rows(&mut client, &table).await,
        0,
        "wrong password writes no row"
    );

    let mut cfg_bad_db = mssql_config(&env, &table);
    cfg_bad_db["database"] = json!("nonexistent_db_ce2");
    let r_bad_db = setup_forward(&broker, "ce2-bad-db", "ce2/mssql/bad_db", cfg_bad_db).await;
    broker
        .publish("pub-bad-db", "ce2/mssql/bad_db", payload.as_bytes(), 1)
        .await;
    let (_, _, f_db) = broker
        .wait_for_action_failures(&r_bad_db, 1, ROW_WAIT)
        .await;
    assert!(f_db >= 1, "missing database increments failed counter");
    assert_eq!(
        count_rows(&mut client, &table).await,
        0,
        "missing database writes no row"
    );

    let mut c_host = mssql_config(&env, &table);
    c_host["host"] = json!("");
    assert_create_err(&broker, "e1", c_host, "mssql host must not be empty").await;

    let mut c_port = mssql_config(&env, &table);
    c_port["port"] = json!(0);
    assert_create_err(&broker, "e2", c_port, "mssql port must be 1..=65535").await;

    let mut c_user = mssql_config(&env, &table);
    c_user["auth"] = json!({"type": "sqlpassword", "username": "", "password": env.password});
    assert_create_err(&broker, "e3", c_user, "mssql username must not be empty").await;

    drop_table(&mut client, "ce2_mssql_tt").await;
    finish_test(client, &table, broker).await;
}

/// Case 2: verifies active directory password auth and refusal of integrated auth.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_auth_variants_through_broker() {
    let (env, table, mut client, broker) = setup_test("auth_variants").await;

    let mut cfg_ad = mssql_config(&env, &table);
    cfg_ad["auth"] =
        json!({"type": "activedirectorypassword", "username": env.user, "password": env.password});
    setup_forward(&broker, "ce2-ad", "ce2/mssql/ad", cfg_ad).await;

    let payload = json!({"client_id": "cid-ad-1", "mode": "activedirectorypassword"}).to_string();
    broker
        .publish("pub-ad", "ce2/mssql/ad", payload.as_bytes(), 1)
        .await;
    wait_rows(&mut client, &table, 1, ROW_WAIT).await;
    let (topic, cid, pl) = read_first_row(&mut client, &table).await;
    assert_eq!(topic, "ce2/mssql/ad");
    assert_eq!(cid, "cid-ad-1");
    assert_eq!(pl, payload);

    let mut cfg_int = mssql_config(&env, &table);
    cfg_int.as_object_mut().expect("object").remove("auth");
    assert_create_err(
        &broker,
        "c-int",
        cfg_int,
        "mssql integrated auth is not supported by the driver transport; use sql_password",
    )
    .await;

    finish_test(client, &table, broker).await;
}

/// Case 3: verifies custom upsert query templates and parameter validation.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_custom_upsert_through_broker() {
    let (env, table, mut client, broker) = setup_test("custom_upsert").await;
    let table_c = format!("{table}_c");
    exec_query(
        &mut client,
        format!("DROP TABLE IF EXISTS [{table_c}]; CREATE TABLE [{table_c}] (topic NVARCHAR(256) NOT NULL, payload NVARCHAR(MAX) NOT NULL)"),
    )
    .await;

    let mut cfg = mssql_config(&env, &table);
    cfg["query_mode"] = json!({"mode": "customupsert", "sql_template": format!("INSERT INTO {table_c} ([topic],[payload]) VALUES ($2, $5)")});
    setup_forward(&broker, "ce2-upsert", "ce2/mssql/custom", cfg).await;

    let payload = json!({"client_id": "cid-custom", "data": "custom-val"}).to_string();
    broker
        .publish("pub-custom", "ce2/mssql/custom", payload.as_bytes(), 1)
        .await;
    wait_rows(&mut client, &table_c, 1, ROW_WAIT).await;

    let rows = client
        .simple_query(format!("SELECT topic, payload FROM [{table_c}]"))
        .await
        .expect("select c")
        .into_first_result()
        .await
        .expect("rows c");
    let row = rows.first().expect("row in c");
    assert_eq!(row.get::<&str, _>(0).expect("topic"), "ce2/mssql/custom");
    assert_eq!(row.get::<&str, _>(1).expect("payload"), payload);
    assert_eq!(
        count_rows(&mut client, &table).await,
        0,
        "standard table stays empty"
    );

    let mut cfg_bad = mssql_config(&env, &table);
    cfg_bad["query_mode"] = json!({"mode": "customupsert", "sql_template": "SELECT 1"});
    assert_create_err(
        &broker,
        "c-bad-sql",
        cfg_bad,
        "mssql sql_template must reference $1..$5",
    )
    .await;

    drop_table(&mut client, &table_c).await;
    finish_test(client, &table, broker).await;
}

/// Case 4: verifies trust_server_certificate acceptance on true and false.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_trust_server_certificate_through_broker() {
    let (env, table, mut client, broker) = setup_test("trust_cert").await;

    let mut cfg_true = mssql_config(&env, &table);
    cfg_true["trust_server_certificate"] = json!(true);
    setup_forward(&broker, "c-trust-t", "ce2/mssql/trust_t", cfg_true).await;
    let pl1 = json!({"client_id": "c-t", "val": 1}).to_string();
    broker
        .publish("pub-t", "ce2/mssql/trust_t", pl1.as_bytes(), 1)
        .await;
    wait_rows(&mut client, &table, 1, ROW_WAIT).await;

    let mut cfg_false = mssql_config(&env, &table);
    cfg_false["trust_server_certificate"] = json!(false);
    setup_forward(&broker, "c-trust-f", "ce2/mssql/trust_f", cfg_false).await;
    let pl2 = json!({"client_id": "c-f", "val": 2}).to_string();
    broker
        .publish("pub-f", "ce2/mssql/trust_f", pl2.as_bytes(), 1)
        .await;
    wait_rows(&mut client, &table, 2, ROW_WAIT).await;

    finish_test(client, &table, broker).await;
}

/// Case 5: verifies batch_size flushing on exact threshold count.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_batch_size_through_broker() {
    let (env, table, mut client, broker) = setup_test("batch_size").await;

    let mut cfg = mssql_config(&env, &table);
    cfg["batch_size"] = json!(10);
    cfg["linger_ms"] = json!(600000);
    setup_forward(&broker, "c-bs", "ce2/mssql/bs", cfg).await;

    let mut mqtt = MqttClient::connect(broker.mqtt_port, "pub-bs").await;
    for i in 0..9 {
        let pl = json!({"client_id": format!("bs-{i}"), "seq": i}).to_string();
        mqtt.publish("ce2/mssql/bs", pl.as_bytes(), 1).await;
    }
    tokio::time::sleep(NEGATIVE_WAIT).await;
    assert_eq!(
        count_rows(&mut client, &table).await,
        0,
        "table holds 0 rows after 9 publishes and 5s"
    );

    let pl10 = json!({"client_id": "bs-9", "seq": 9}).to_string();
    mqtt.publish("ce2/mssql/bs", pl10.as_bytes(), 1).await;
    wait_rows(&mut client, &table, 10, Duration::from_secs(10)).await;

    finish_test(client, &table, broker).await;
}

/// Case 6: verifies batch_bytes flushing when byte threshold is exceeded.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_batch_bytes_through_broker() {
    let (env, table, mut client, broker) = setup_test("batch_bytes").await;

    let mut cfg = mssql_config(&env, &table);
    cfg["batch_size"] = json!(1000);
    cfg["linger_ms"] = json!(600000);
    cfg["batch_bytes"] = json!(3000);
    setup_forward(&broker, "c-bb", "ce2/mssql/bb", cfg).await;

    let pad = "x".repeat(1000);
    let mut mqtt = MqttClient::connect(broker.mqtt_port, "pub-bb").await;
    for i in 0..2 {
        let pl = json!({"client_id": format!("bb-{i}"), "pad": &pad}).to_string();
        mqtt.publish("ce2/mssql/bb", pl.as_bytes(), 1).await;
    }
    tokio::time::sleep(NEGATIVE_WAIT).await;
    assert_eq!(
        count_rows(&mut client, &table).await,
        0,
        "table holds 0 rows after 2 publishes and 5s"
    );

    let pl3 = json!({"client_id": "bb-2", "pad": &pad}).to_string();
    mqtt.publish("ce2/mssql/bb", pl3.as_bytes(), 1).await;
    wait_rows(&mut client, &table, 3, Duration::from_secs(10)).await;

    finish_test(client, &table, broker).await;
}

/// Case 7: verifies linger_ms flushing upon next publish when oldest row is stale.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_linger_through_broker() {
    let (env, table, mut client, broker) = setup_test("linger").await;

    let mut cfg = mssql_config(&env, &table);
    cfg["batch_size"] = json!(100);
    cfg["linger_ms"] = json!(1000);
    setup_forward(&broker, "c-linger", "ce2/mssql/linger", cfg).await;

    let mut mqtt = MqttClient::connect(broker.mqtt_port, "pub-linger").await;
    let pl_a = json!({"client_id": "ling-A", "msg": "A"}).to_string();
    let pl_b = json!({"client_id": "ling-B", "msg": "B"}).to_string();
    mqtt.publish("ce2/mssql/linger", pl_a.as_bytes(), 1).await;
    mqtt.publish("ce2/mssql/linger", pl_b.as_bytes(), 1).await;

    tokio::time::sleep(NEGATIVE_WAIT).await;
    assert_eq!(
        count_rows(&mut client, &table).await,
        0,
        "table holds 0 rows after A, B and 5s"
    );

    let pl_c = json!({"client_id": "ling-C", "msg": "C"}).to_string();
    mqtt.publish("ce2/mssql/linger", pl_c.as_bytes(), 1).await;
    wait_rows(&mut client, &table, 3, Duration::from_secs(10)).await;

    finish_test(client, &table, broker).await;
}

/// Case 8: verifies timeout_ms failure on a non-responsive endpoint within 3 seconds.
#[tokio::test]
#[ignore = "needs no server; loopback listener measures timeout"]
async fn mssql_timeout_through_broker() {
    let (_env, table, client, broker) = setup_test("timeout").await;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let port = listener.local_addr().expect("port").port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _s = stream;
                tokio::time::sleep(Duration::from_secs(60)).await;
            });
        }
    });

    let cfg = json!({
        "host": "127.0.0.1", "port": port, "database": "qual", "table_template": table,
        "auth": {"type": "sqlpassword", "username": "sa", "password": "any"},
        "timeout_ms": 500, "max_retries": 0, "batch_size": 1,
    });
    let rule_id = setup_forward(&broker, "c-timeout", "ce2/mssql/timeout", cfg).await;

    let pl = json!({"client_id": "cid-timeout", "msg": "timeout-test"}).to_string();
    broker
        .publish("pub-timeout", "ce2/mssql/timeout", pl.as_bytes(), 1)
        .await;
    let (_, _, failed) = broker
        .wait_for_action_failures(&rule_id, 1, Duration::from_secs(3))
        .await;
    assert!(
        failed >= 1,
        "action failed counter is at least 1 within 3 seconds: {failed}"
    );

    finish_test(client, &table, broker).await;
}

fn mqtt_string(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 2);
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    out
}

fn mqtt_packet(header: u8, body: &[u8]) -> Vec<u8> {
    let mut pkt = vec![header];
    let mut len = body.len();
    loop {
        let mut b = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            b |= 0x80;
        }
        pkt.push(b);
        if len == 0 {
            break;
        }
    }
    pkt.extend_from_slice(body);
    pkt
}

fn mqtt_connect_packet(client_id: &str) -> Vec<u8> {
    let mut rest = mqtt_string("MQTT");
    rest.extend_from_slice(&[4, 0x02]);
    rest.extend_from_slice(&60u16.to_be_bytes());
    rest.extend_from_slice(&mqtt_string(client_id));
    mqtt_packet(0x10, &rest)
}

fn mqtt_publish_packet(topic: &str, packet_id: u16, payload: &[u8]) -> Vec<u8> {
    let mut rest = mqtt_string(topic);
    rest.extend_from_slice(&packet_id.to_be_bytes());
    rest.extend_from_slice(payload);
    mqtt_packet(0x32, &rest)
}

async fn read_mqtt_packet<R: AsyncReadExt + Unpin>(
    stream: &mut R,
) -> std::io::Result<(u8, Vec<u8>)> {
    let mut head = [0u8; 1];
    stream.read_exact(&mut head).await?;
    let (mut rem, mut mul) = (0usize, 1usize);
    loop {
        let mut b = [0u8; 1];
        stream.read_exact(&mut b).await?;
        rem += ((b[0] & 0x7f) as usize) * mul;
        if b[0] & 0x80 == 0 {
            break;
        }
        mul *= 128;
    }
    let mut body = vec![0u8; rem];
    stream.read_exact(&mut body).await?;
    Ok((head[0], body))
}

async fn query_table_payloads(client: &mut MssqlClient, table: &str) -> Option<Vec<tiberius::Row>> {
    let stream = client
        .simple_query(format!("SELECT payload FROM [{table}]"))
        .await
        .ok()?;
    stream.into_first_result().await.ok()
}

async fn collect_outage_seqs(
    client: &mut MssqlClient,
    env: &EnvConfig,
    table: &str,
) -> Option<HashSet<usize>> {
    let Some(rows) = query_table_payloads(client, table).await else {
        if let Ok(new_client) =
            connect_raw(&env.host, env.port, &env.user, &env.password, &env.database).await
        {
            *client = new_client;
        }
        return None;
    };
    let mut seen = HashSet::new();
    for r in &rows {
        if let Some(s) = r
            .get::<&str, _>(0)
            .and_then(|p| serde_json::from_str::<Value>(p).ok())
            .and_then(|v| v.get("seq")?.as_u64())
        {
            seen.insert(s as usize);
        }
    }
    Some(seen)
}

async fn check_outage_tables(
    client: &mut MssqlClient,
    env: &EnvConfig,
    table_r600: &str,
    table_r0: &str,
    n: usize,
) -> (bool, bool) {
    let Some(seen_600) = collect_outage_seqs(client, env, table_r600).await else {
        return (false, false);
    };
    let Some(seen_0) = collect_outage_seqs(client, env, table_r0).await else {
        return (false, false);
    };
    (
        (0..n).all(|i| seen_600.contains(&i)),
        (0..n).any(|i| !seen_0.contains(&i)),
    )
}

/// Case 9: verifies retry behavior across a simulated server outage.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_retry_through_outage() {
    let env = read_env();
    let table_r0 = format!("{}_r0", env.table);
    let table_r600 = format!("{}_r600", env.table);
    let mut client = connect_client(&env).await.expect("connect client");
    drop_and_create_table(&mut client, &table_r0).await;
    drop_and_create_table(&mut client, &table_r600).await;

    let broker = TestBroker::start("").await;
    let rule_r0 = setup_forward(
        &broker,
        "ce2-r0",
        "ce2/mssql/r0",
        outage_config(&env, &table_r0, 0),
    )
    .await;
    let _rule_r600 = setup_forward(
        &broker,
        "ce2-r600",
        "ce2/mssql/r600",
        outage_config(&env, &table_r600, 600),
    )
    .await;

    let mut stream = TcpStream::connect(("127.0.0.1", broker.mqtt_port))
        .await
        .expect("connect to broker");
    stream
        .write_all(&mqtt_connect_packet("client-outage"))
        .await
        .expect("write CONNECT packet");
    let (head, body) = read_mqtt_packet(&mut stream).await.expect("read CONNACK");
    assert_eq!(head & 0xF0, 0x20, "server answers CONNACK");
    assert_eq!(body.get(1).copied().unwrap_or(1), 0, "CONNECT accepted");

    let (mut reader, mut writer) = stream.into_split();
    let reader_task =
        tokio::spawn(async move { while (read_mqtt_packet(&mut reader).await).is_ok() {} });

    let fault_file_var = std::env::var("QUAL_FAULT_FILE")
        .unwrap_or_else(|_| panic!("QUAL_FAULT_FILE must be set for outage test"));
    let fault_path = PathBuf::from(fault_file_var);
    std::fs::write(&fault_path, b"restart").expect("write QUAL_FAULT_FILE");

    let mut seq = 0usize;
    let mut packet_id = 1u16;
    let deadline_outage = Instant::now() + OUTAGE_WAIT;
    while fault_path.exists() {
        assert!(
            Instant::now() < deadline_outage,
            "fault file not cleared within 240 seconds"
        );
        let pl_r0 = json!({"client_id": format!("r0-{seq}"), "seq": seq}).to_string();
        let pl_r600 = json!({"client_id": format!("r600-{seq}"), "seq": seq}).to_string();
        let pkt0 = mqtt_publish_packet("ce2/mssql/r0", packet_id, pl_r0.as_bytes());
        packet_id = packet_id.wrapping_add(1).max(1);
        let pkt600 = mqtt_publish_packet("ce2/mssql/r600", packet_id, pl_r600.as_bytes());
        packet_id = packet_id.wrapping_add(1).max(1);

        writer.write_all(&pkt0).await.expect("publish R0");
        writer.write_all(&pkt600).await.expect("publish R600");
        seq += 1;
        tokio::time::sleep(OUTAGE_POLL).await;
    }
    let n = seq;

    let post_deadline = Instant::now() + POST_OUTAGE_WAIT;
    loop {
        let (r600_ok, r0_lacks) =
            check_outage_tables(&mut client, &env, &table_r600, &table_r0, n).await;
        let (_, _, r0_failed) = broker.rule_actions(&rule_r0).await;
        if r600_ok && r0_failed >= 1 && r0_lacks {
            break;
        }
        assert!(
            Instant::now() < post_deadline,
            "outage check did not hold within 120s"
        );
        let pl_r0 = json!({"client_id": format!("r0-{seq}"), "seq": seq}).to_string();
        let pl_r600 = json!({"client_id": format!("r600-{seq}"), "seq": seq}).to_string();
        let pkt0 = mqtt_publish_packet("ce2/mssql/r0", packet_id, pl_r0.as_bytes());
        packet_id = packet_id.wrapping_add(1).max(1);
        let pkt600 = mqtt_publish_packet("ce2/mssql/r600", packet_id, pl_r600.as_bytes());
        packet_id = packet_id.wrapping_add(1).max(1);

        writer.write_all(&pkt0).await.expect("publish R0");
        writer.write_all(&pkt600).await.expect("publish R600");
        seq += 1;
        tokio::time::sleep(POST_OUTAGE_POLL).await;
    }

    let _ = writer.write_all(&[0xE0, 0x00]).await;
    let _ = writer.shutdown().await;
    reader_task.abort();

    drop_table(&mut client, &table_r0).await;
    drop_table(&mut client, &table_r600).await;
    broker.finish().await;
}

/// Case 10: verifies rejection of duplicate v5 connector identifiers with ALREADY_EXISTS.
#[tokio::test]
#[ignore = "needs no server; v5 duplicate rejection runs in qualification"]
async fn mssql_duplicate_through_broker() {
    let broker = TestBroker::start("").await;
    let body = json!({
        "name": "ce2-mssql-dup", "type": "mssql", "server": "127.0.0.1:1433",
        "database": "qual", "username": "sa"
    });
    post_v5_201(&broker, "/api/v5/connectors", body.clone()).await;

    let (s2, a2) = broker
        .api(reqwest::Method::POST, "/api/v5/connectors", body)
        .await;
    assert_eq!(s2, 400, "duplicate connector create answers 400: {a2}");
    assert_eq!(
        a2.get("code").and_then(|c| c.as_str()),
        Some("ALREADY_EXISTS"),
        "error code is ALREADY_EXISTS: {a2}"
    );

    broker.finish().await;
}

/// Case 11: verifies that unmatched topics deliver no messages to the database.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_unmatched_topic_through_broker() {
    let (env, table, mut client, broker) = setup_test("unmatched").await;

    setup_forward(
        &broker,
        "ce2-unmatched",
        "ce2/mssql/rule",
        mssql_config(&env, &table),
    )
    .await;

    let payload = json!({"client_id": "cid-nomatch", "msg": "other"}).to_string();
    broker
        .publish("pub-unmatched", "ce2/mssql/other", payload.as_bytes(), 1)
        .await;
    tokio::time::sleep(NEGATIVE_WAIT).await;
    assert_eq!(
        count_rows(&mut client, &table).await,
        0,
        "table holds 0 rows for unmatched publish"
    );

    finish_test(client, &table, broker).await;
}

/// Case 12: verifies connector and rule persistence across an orderly broker restart.
#[tokio::test]
#[ignore = "needs a real server (see MSSQL_* env)"]
async fn mssql_persist_across_restart_through_broker() {
    let (env, table, mut client, mut broker) = setup_test("persist").await;

    let server = format!("{}:{}", env.host, env.port);
    let conn = json!({
        "name": "ce2-mssql-restart", "type": "mssql", "server": server,
        "database": env.database, "username": env.user, "password": env.password
    });
    post_v5_201(&broker, "/api/v5/connectors", conn).await;

    let sql = format!(
        "INSERT INTO {table} ([event_time],[topic],[client_id],[qos],[payload]) VALUES ($1, $2, $3, $4, $5)"
    );
    let act = json!({"name": "ce2-mssql-restart-act", "type": "mssql", "connector": "ce2-mssql-restart", "parameters": {"sql": sql}});
    post_v5_201(&broker, "/api/v5/actions", act).await;

    let rule = json!({"id": "ce2-mssql-restart-rule", "name": "ce2-mssql-restart-rule", "sql": "SELECT * FROM \"ce2/mssql/restart\"", "actions": ["ce2-mssql-restart-act"]});
    post_v5_201(&broker, "/api/v5/rules", rule).await;

    let pl1 = json!({"client_id": "rst-1", "seq": 1}).to_string();
    broker
        .publish("pub-rst-1", "ce2/mssql/restart", pl1.as_bytes(), 1)
        .await;
    wait_rows(&mut client, &table, 1, ROW_WAIT).await;

    broker.stop_and_restart_orderly().await;

    let pl2 = json!({"client_id": "rst-2", "seq": 2}).to_string();
    broker
        .publish("pub-rst-2", "ce2/mssql/restart", pl2.as_bytes(), 1)
        .await;
    wait_rows(&mut client, &table, 2, ROW_WAIT).await;

    finish_test(client, &table, broker).await;
}
