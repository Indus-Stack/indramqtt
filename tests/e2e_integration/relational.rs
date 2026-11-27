use super::*;

/// MV-02a is accepted: the six relational kinds are `ready`, so the
/// probe no longer refuses them as pending. A bare probe carries no
/// target, and a ready non-document kind reads 204.
#[tokio::test]
async fn mv02a_ready_kinds_probe_without_target_reads_no_content() {
    let kinds = [
        "mysql",
        "alloydb",
        "cockroachdb",
        "mssql",
        "oracle",
        "redshift",
    ];
    for kind in kinds {
        let kernel = spawn_kernel("").await;
        let (status, _) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors_probe",
            serde_json::json!({ "type": kind }),
        )
        .await;
        assert_eq!(status, 204, "{kind} bare probe reads 204, got {status}");
        kernel.finish().await;
    }
}

/// A ready kind dials its target: a dead target fails the probe with
/// 400 TEST_FAILED. A pending kind would fail with the refusal text
/// instead of a dial error.
#[tokio::test]
async fn mv02a_ready_kinds_probe_dead_target_fails() {
    let kinds = [
        "mysql",
        "alloydb",
        "cockroachdb",
        "mssql",
        "oracle",
        "redshift",
    ];
    for kind in kinds {
        let kernel = spawn_kernel("").await;
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors_probe",
            serde_json::json!({ "type": kind, "server": "127.0.0.1:9" }),
        )
        .await;
        assert_eq!(status, 400, "{kind} dead-target probe: {body}");
        assert_eq!(body["code"], "TEST_FAILED", "{kind} probe code: {body}");
        assert!(
            !body["message"]
                .as_str()
                .unwrap_or("")
                .contains("not available in this broker"),
            "{kind} probe dialled instead of refusing: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a is accepted: the six relational kinds are `ready`, so a
/// complete connector create stores (201) and reads back. The sink
/// registers lazily, so a dead server still stores.
#[tokio::test]
async fn mv02a_ready_kinds_create_stores() {
    let kinds = [
        ("mysql", "mysql"),
        ("alloydb", "alloydb"),
        ("cockroachdb", "cockroachdb"),
        ("mssql", "sqlserver"),
        ("oracle", "oracle"),
        ("redshift", "redshift"),
    ];
    for (kind, connector_type) in kinds {
        let kernel = spawn_kernel("").await;
        let name = format!("mv02a-up-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            serde_json::json!({
                "name": name,
                "type": connector_type,
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/connectors/{name}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} connector read: {body}");
        assert_eq!(
            body["type"],
            serde_json::json!(connector_type),
            "{kind} type round-trips: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a is accepted: the six relational kinds are `ready`, so an
/// action on a stored connector stages its owned sink and stores
/// (201). Each statement uses the placeholders of its driver.
#[tokio::test]
async fn mv02a_ready_kinds_action_create_stores() {
    let kinds = [
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
        ),
        (
            "alloydb",
            "alloydb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "username": "u",
                "service_name": "XE",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
    ];
    for (kind, action_type, fields, sql) in kinds {
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-up-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": action_type,
        });
        for (field, value) in fields.as_object().expect("connector fields") {
            connector_body[field] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let name = format!("mv02a-act-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": name,
                "type": action_type,
                "connector": connector,
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{name}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} action read: {body}");
        assert_eq!(
            body["type"],
            serde_json::json!(action_type),
            "{kind} type round-trips: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a is accepted: the kinds are `ready`, so option validation
/// runs in the action builder. Each combination below reaches the
/// builder and fails with the field named. The connector create
/// stores (201); the action create answers 400 and stores nothing.
#[tokio::test]
async fn mv02a_ready_kinds_action_builders_refuse_bad_options() {
    let cases = [
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:5439",
                "database": "db",
                "username": "u",
                "disable_prepared_statements": true,
            }),
            "INSERT INTO t VALUES ($1, $2, $3)",
            "disable_prepared_statements",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:26257",
                "database": "db",
                "username": "u",
                "application_name": "app",
            }),
            "INSERT INTO t VALUES ($1, $2, $3)",
            "application_name",
        ),
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:3306",
                "database": "db",
                "ssl": { "enable": true },
            }),
            "INSERT INTO t VALUES (?, ?, ?)",
            "ssl.enable",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1521",
                "username": "u",
            }),
            "INSERT INTO t VALUES (:1)",
            "service_name",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1433",
                "database": "db",
                "driver": "FreeTDS",
            }),
            "INSERT INTO t VALUES ($2)",
            "driver",
        ),
    ];
    for (kind, action_type, fields, sql, field) in cases {
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-opt-conn-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": action_type,
        });
        for (key, value) in fields.as_object().expect("connector fields") {
            connector_body[key] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": format!("mv02a-opt-act-{kind}"),
                "type": action_type,
                "connector": connector,
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 400, "{kind} bad option refused: {body}");
        assert_eq!(
            body["code"], "ALREADY_EXISTS",
            "{kind} refusal code: {body}"
        );
        assert!(
            body["message"].as_str().unwrap_or("").contains(field),
            "{kind} refusal names {field}: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a N1: the pg-family kinds deliver the engine row through the broker.
/// Each action fires on its own publish. The fake backend captures the
/// statement and the bound row. A quote in the payload stays bound.
#[tokio::test]
async fn mv02a_pg_family_delivers_bound_rows_through_broker() {
    let kinds = [
        ("alloydb", "alloydb"),
        ("cockroachdb", "cockroachdb"),
        ("redshift", "redshift"),
    ];
    for (kind, action_type) in kinds {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("pg fake binds");
        let address = listener.local_addr().expect("pg fake address");
        let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
        let fake = {
            let captured = Arc::clone(&captured);
            tokio::spawn(async move {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 1).await;
            })
        };
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-pg-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            serde_json::json!({
                "name": connector,
                "type": action_type,
                "server": format!("127.0.0.1:{}", address.port()),
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let action = format!("mv02a-pg-act-{kind}");
        let sentinel = format!("MV02A-{kind}-SQL");
        let sql = format!("{sentinel} INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": action,
                "type": action_type,
                "connector": connector,
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let rule = format!("mv02a-pg-rule-{kind}");
        let topic = format!("mv02a/in/pg-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule,
                "sql": format!("SELECT * FROM \"{topic}\""),
                "actions": [action.clone()],
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} rule create: {body}");
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-pg-pub-{kind}")).await;
        let payload = br#"{"v":"a\"b"}"#;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, payload),
        )
        .await;
        drop(publisher);
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let done = captured.lock().expect("capture lock").len() == 1;
            assert!(
                std::time::Instant::now() < deadline || done,
                "{kind} pg fake got one batch"
            );
            if done {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        fake.await.expect("pg fake task");
        let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
            .expect("capture lock");
        assert_eq!(captured.len(), 1, "{kind} one batch");
        let (statement, rows) = &captured[0];
        assert!(
            statement.contains(&sentinel),
            "{kind} statement carries sentinel: {statement}"
        );
        assert!(
            statement.contains("$1") && statement.contains("$3"),
            "{kind} markers stay: {statement}"
        );
        assert_eq!(rows.len(), 1, "{kind} one row");
        assert_eq!(rows[0].len(), 3, "{kind} topic, qos, payload");
        assert_eq!(rows[0][0], topic.as_bytes(), "{kind} topic bound");
        assert_eq!(rows[0][1], b"0", "{kind} qos bound");
        assert_eq!(rows[0][2], payload, "{kind} quote stays bound");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} metrics: {body}");
        assert_eq!(
            body["metrics"]["success"],
            serde_json::json!(1),
            "{kind} success: {body}"
        );
        assert_eq!(
            body["metrics"]["failed"],
            serde_json::json!(0),
            "{kind} failed: {body}"
        );
        kernel.finish().await;
    }
}

/// Reads one MySQL packet (seq plus body) from the fake server stream.
/// Returns `None` for a reachability probe that connects and closes
/// without a byte, so the fake skips it and serves the driver.
async fn mv02a_mysql_read_packet(stream: &mut tokio::net::TcpStream) -> Option<Vec<u8>> {
    use tokio::io::AsyncReadExt;
    let mut header = [0_u8; 4];
    if stream.read_exact(&mut header).await.is_err() {
        return None;
    }
    let len = u32::from_le_bytes([header[0], header[1], header[2], 0]) as usize;
    let mut body = vec![0_u8; 1 + len];
    body[0] = header[3];
    if stream.read_exact(&mut body[1..]).await.is_err() {
        return None;
    }
    Some(body)
}

/// Writes one MySQL packet with the given seq to the fake server stream.
/// Returns false when the peer closed, so the caller skips it.
async fn mv02a_mysql_write_packet(
    stream: &mut tokio::net::TcpStream,
    seq: u8,
    body: &[u8],
) -> bool {
    use tokio::io::AsyncWriteExt;
    let len = body.len() as u32;
    let mut frame = vec![
        (len & 0xFF) as u8,
        ((len >> 8) & 0xFF) as u8,
        ((len >> 16) & 0xFF) as u8,
        seq,
    ];
    frame.extend_from_slice(body);
    stream.write_all(&frame).await.is_ok()
}

/// Decodes one length-encoded integer from the fake server cursor.
fn mv02a_mysql_decode_lenenc(cursor: &[u8]) -> (usize, &[u8]) {
    match cursor[0] {
        0xFC => (
            u16::from_le_bytes([cursor[1], cursor[2]]) as usize,
            &cursor[3..],
        ),
        0xFD => (
            u32::from_le_bytes([cursor[1], cursor[2], cursor[3], 0]) as usize,
            &cursor[4..],
        ),
        0xFE => (
            u64::from_le_bytes([
                cursor[1], cursor[2], cursor[3], cursor[4], cursor[5], cursor[6], cursor[7],
                cursor[8],
            ]) as usize,
            &cursor[9..],
        ),
        byte => (byte as usize, &cursor[1..]),
    }
}

/// Writes one MySQL column definition with catalog `def`.
/// It matches the real wire the driver parses.
async fn mv02a_mysql_write_column_def(stream: &mut tokio::net::TcpStream, seq: u8, name: &[u8]) {
    let mut def = vec![0x03, b'd', b'e', b'f'];
    def.push(0);
    def.push(0);
    def.push(0);
    def.push(name.len() as u8);
    def.extend_from_slice(name);
    def.push(0);
    def.push(0x0C);
    def.extend_from_slice(&33_u16.to_le_bytes());
    def.extend_from_slice(&1024_u32.to_le_bytes());
    def.push(0xFD);
    def.extend_from_slice(&0_u16.to_le_bytes());
    def.push(0);
    def.extend_from_slice(&[0, 0]);
    mv02a_mysql_write_packet(stream, seq, &def).await;
}

/// Answers one driver settings probe (`COM_QUERY`, for example
/// `SELECT @@socket`). Each selected column gets one NULL value.
/// The driver only reads the values by position.
async fn mv02a_mysql_answer_settings_query(stream: &mut tokio::net::TcpStream, sql: &[u8]) {
    let columns = sql.iter().filter(|&&b| b == b',').count() + 1;
    let columns = columns.min(8) as u8;
    mv02a_mysql_write_packet(stream, 1, &[columns]).await;
    for index in 0..columns {
        mv02a_mysql_write_column_def(stream, 2 + index, b"").await;
    }
    mv02a_mysql_write_packet(stream, 2 + columns, &[0xFE, 0, 0, 2, 0]).await;
    mv02a_mysql_write_packet(stream, 3 + columns, &vec![0xFB; columns as usize]).await;
    mv02a_mysql_write_packet(stream, 4 + columns, &[0xFE, 0, 0, 2, 0]).await;
}

/// One fake MySQL backend: greeting, accept-any handshake, driver
/// settings probe, one prepare capture, one execute capture. It
/// proves delivery, not auth.
async fn mv02a_mysql_serve_one(
    mut stream: tokio::net::TcpStream,
    captured_sql: Arc<Mutex<String>>,
    captured_rows: Arc<Mutex<Vec<Vec<Vec<u8>>>>>,
) {
    let seed_part1 = b"12345678";
    let seed_part2 = b"ABCDEFGHJKLM";
    let mut greeting = vec![10_u8];
    greeting.extend_from_slice(b"8.0.36-test\0");
    greeting.extend_from_slice(&42_u32.to_le_bytes());
    greeting.extend_from_slice(seed_part1);
    greeting.push(0);
    // No SSL bit: the fake speaks plaintext only, so it clears
    // CLIENT_SSL (0x0800) from the low caps. It advertises
    // CLIENT_PROTOCOL_41 and CLIENT_SECURE_CONNECTION, and
    // CLIENT_PLUGIN_AUTH in the high caps for mysql_native_password.
    greeting.extend_from_slice(&0xF7FF_u16.to_le_bytes());
    greeting.push(33);
    greeting.extend_from_slice(&0x0002_u16.to_le_bytes());
    greeting.extend_from_slice(&0x0008_u16.to_le_bytes());
    greeting.push(21);
    greeting.extend_from_slice(&[0_u8; 10]);
    greeting.extend_from_slice(seed_part2);
    greeting.push(0);
    greeting.extend_from_slice(b"mysql_native_password\0");
    if !mv02a_mysql_write_packet(&mut stream, 0, &greeting).await {
        return;
    }
    let Some(_handshake) = mv02a_mysql_read_packet(&mut stream).await else {
        return;
    };
    if !mv02a_mysql_write_packet(&mut stream, 2, &[0x00, 0, 0, 0, 0, 0, 0]).await {
        return;
    }
    let request = loop {
        let Some(request) = mv02a_mysql_read_packet(&mut stream).await else {
            return;
        };
        if request[1] == 0x03 {
            mv02a_mysql_answer_settings_query(&mut stream, &request[2..]).await;
            continue;
        }
        break request;
    };
    assert_eq!(request[1], 0x16, "prepare parses the action statement");
    captured_sql
        .lock()
        .expect("sql lock")
        .push_str(std::str::from_utf8(&request[2..]).expect("sql"));
    let mut prepare_ok = vec![0x00];
    prepare_ok.extend_from_slice(&7_u32.to_le_bytes());
    prepare_ok.extend_from_slice(&0_u16.to_le_bytes());
    prepare_ok.extend_from_slice(&3_u16.to_le_bytes());
    prepare_ok.extend_from_slice(&0_u16.to_le_bytes());
    prepare_ok.extend_from_slice(&[0_u8; 3]);
    mv02a_mysql_write_packet(&mut stream, 1, &prepare_ok).await;
    for seq in [2_u8, 3, 4] {
        mv02a_mysql_write_column_def(&mut stream, seq, b"").await;
    }
    mv02a_mysql_write_packet(&mut stream, 5, &[0xFE, 0, 0, 0, 0]).await;
    let Some(request) = mv02a_mysql_read_packet(&mut stream).await else {
        return;
    };
    assert_eq!(request[1], 0x17, "execute binds one engine row");
    let stmt_id = u32::from_le_bytes([request[2], request[3], request[4], request[5]]);
    assert_eq!(stmt_id, 7, "execute uses the prepared statement");
    let mut cursor = &request[6 + 1 + 4 + 1 + 1..];
    cursor = &cursor[6..];
    let mut row = Vec::new();
    for _ in 0..3 {
        let (len, rest) = mv02a_mysql_decode_lenenc(cursor);
        row.push(rest[..len].to_vec());
        cursor = &rest[len..];
    }
    captured_rows.lock().expect("rows lock").push(row);
    mv02a_mysql_write_packet(&mut stream, 1, &[0x00, 1, 0, 0, 0, 0, 0]).await;
}

/// MV-02a N1: MySQL delivers the engine row through the broker. The fake
/// backend captures the statement and the bound row. A quote stays bound.
#[tokio::test]
async fn mv02a_mysql_delivers_bound_rows_through_broker() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mysql fake binds");
    let address = listener.local_addr().expect("mysql fake address");
    let captured_sql = Arc::new(Mutex::new(String::new()));
    let captured_rows: Arc<Mutex<Vec<Vec<Vec<u8>>>>> = Arc::new(Mutex::new(Vec::new()));
    let fake = {
        let captured_sql = Arc::clone(&captured_sql);
        let captured_rows = Arc::clone(&captured_rows);
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.expect("mysql backend accepted");
                mv02a_mysql_serve_one(
                    stream,
                    Arc::clone(&captured_sql),
                    Arc::clone(&captured_rows),
                )
                .await;
                let done = !captured_sql.lock().expect("sql lock").is_empty()
                    && !captured_rows.lock().expect("rows lock").is_empty();
                if done {
                    break;
                }
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv02a-my",
            "type": "mysql",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let sentinel = "MV02A-MYSQL-SQL";
    let sql = format!("{sentinel} INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv02a-my-act",
            "type": "mysql",
            "connector": "mv02a-my",
            "parameters": {"sql": sql},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let topic = "mv02a/in/mysql";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv02a-my-rule",
            "sql": format!("SELECT * FROM \"{topic}\""),
            "actions": ["mv02a-my-act"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv02a-my-pub").await;
    let payload = br#"{"v":"a\"b"}"#;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(topic, 0, 0, false, payload),
    )
    .await;
    drop(publisher);
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let done = captured_rows.lock().expect("rows lock").len() == 1;
        assert!(
            std::time::Instant::now() < deadline || done,
            "mysql fake got one row"
        );
        if done {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("mysql fake task");
    let statement = captured_sql.lock().expect("sql lock").clone();
    assert!(
        statement.contains(sentinel),
        "statement carries sentinel: {statement}"
    );
    assert!(statement.contains("?, ?, ?"), "markers stay: {statement}");
    let rows = captured_rows.lock().expect("rows lock").clone();
    assert_eq!(rows.len(), 1, "one row");
    assert_eq!(rows[0].len(), 3, "topic, qos, payload");
    assert_eq!(rows[0][0], topic.as_bytes(), "topic bound");
    assert_eq!(rows[0][1], b"0", "qos bound");
    assert_eq!(rows[0][2], payload, "quote stays bound");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-my-act/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics: {body}");
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(1),
        "success: {body}"
    );
    assert_eq!(
        body["metrics"]["failed"],
        serde_json::json!(0),
        "failed: {body}"
    );
    kernel.finish().await;
}

/// Encodes text as UCS-2 (UTF-16LE) for the MSSQL fake assertions.
fn mv02a_ucs2(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for unit in text.encode_utf16() {
        out.extend_from_slice(&unit.to_le_bytes());
    }
    out
}

/// Reads one TDS packet body from the fake MSSQL server stream.
async fn mv02a_mssql_read_packet(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    let mut header = [0_u8; 8];
    stream.read_exact(&mut header).await.expect("tds head");
    let length = u16::from_be_bytes([header[2], header[3]]) as usize;
    let mut body = vec![0_u8; length - 8];
    stream.read_exact(&mut body).await.expect("tds body");
    body
}

/// Writes one TDS reply packet from the fake MSSQL server stream.
async fn mv02a_mssql_write_reply(stream: &mut tokio::net::TcpStream, payload: &[u8]) {
    use tokio::io::AsyncWriteExt;
    let mut packet = vec![0x04_u8, 0x01];
    packet.extend_from_slice(&((8 + payload.len()) as u16).to_be_bytes());
    packet.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    packet.extend_from_slice(payload);
    stream.write_all(&packet).await.expect("tds reply");
}

/// One fake MSSQL backend: prelogin, login, one RPC capture. It proves
/// delivery, not auth. The RPC bytes carry the bound values.
async fn mv02a_mssql_serve_one(mut stream: tokio::net::TcpStream, captured: Arc<Mutex<Vec<u8>>>) {
    use tokio::io::AsyncWriteExt;
    let request = mv02a_mssql_read_packet(&mut stream).await;
    assert!(!request.is_empty(), "prelogin arrives");
    // Full prelogin reply: VERSION, ENCRYPTION (NOT_SUP, plaintext
    // only), INSTOPT (empty default instance), THREADID and MARS,
    // so a maintained driver accepts it. The delivery and auth
    // checks below are unchanged.
    let mut reply = Vec::new();
    let mut offset: u16 = 5 * 5 + 1;
    for (id, data_len) in [
        (0x00_u8, 6_usize),
        (0x01, 1),
        (0x02, 1),
        (0x03, 4),
        (0x04, 1),
    ] {
        reply.push(id);
        reply.extend_from_slice(&offset.to_be_bytes());
        reply.extend_from_slice(&(data_len as u16).to_be_bytes());
        offset += data_len as u16;
    }
    reply.push(0xFF);
    reply.extend_from_slice(&[0x0F, 0x00, 0x00, 0x00, 0x00, 0x00]);
    reply.push(0x02);
    reply.push(0x00);
    reply.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    reply.push(0x00);
    mv02a_mssql_write_reply(&mut stream, &reply).await;
    let login = mv02a_mssql_read_packet(&mut stream).await;
    assert!(login.len() > 64, "login arrives");
    let needle = mv02a_ucs2("sa");
    assert!(
        login.windows(needle.len()).any(|w| w == needle.as_slice()),
        "login carries the user"
    );
    let mut loginack = vec![0xAD_u8];
    // Wire-valid LOGINACK the driver parses: interface, TDS
    // version, program name, server version.
    let mut login_body = vec![0x01_u8];
    login_body.extend_from_slice(&0x74000004_u32.to_be_bytes());
    login_body.push(0x05);
    for unit in "mssql".encode_utf16() {
        login_body.extend_from_slice(&unit.to_le_bytes());
    }
    login_body.extend_from_slice(&0_u32.to_le_bytes());
    loginack.extend_from_slice(&(login_body.len() as u16).to_le_bytes());
    loginack.extend_from_slice(&login_body);
    let mut done = vec![0xFD_u8];
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&0_u64.to_le_bytes());
    let mut payload = loginack;
    payload.extend_from_slice(&done);
    mv02a_mssql_write_reply(&mut stream, &payload).await;
    let batch = mv02a_mssql_read_packet(&mut stream).await;
    assert!(batch.len() > 16, "rpc arrives");
    *captured.lock().expect("capture lock") = batch;
    let mut done = vec![0xFD_u8];
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&0_u16.to_le_bytes());
    done.extend_from_slice(&1_u64.to_le_bytes());
    mv02a_mssql_write_reply(&mut stream, &done).await;
    let _ = stream.shutdown().await;
}

/// MV-02a N1: MSSQL delivers the engine row through the broker. The fake
/// backend captures the RPC. Topic and payload ride as bound params.
#[tokio::test]
async fn mv02a_mssql_delivers_bound_rows_through_broker() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("mssql fake binds");
    let address = listener.local_addr().expect("mssql fake address");
    let captured: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(Vec::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            let stream = mv01_pg_accept(&listener).await;
            mv02a_mssql_serve_one(stream, Arc::clone(&captured)).await;
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv02a-ms",
            "type": "mssql",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "sa",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let sentinel = "MV02A-MSSQL-SQL";
    let sql = format!("{sentinel} INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv02a-ms-act",
            "type": "mssql",
            "connector": "mv02a-ms",
            "parameters": {"sql": sql},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let topic = "mv02a/in/mssql";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv02a-ms-rule",
            "sql": format!("SELECT * FROM \"{topic}\""),
            "actions": ["mv02a-ms-act"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv02a-ms-pub").await;
    let payload = br#"{"v":"a\"b"}"#;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(topic, 0, 0, false, payload),
    )
    .await;
    drop(publisher);
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let done = !captured.lock().expect("capture lock").is_empty();
        assert!(
            std::time::Instant::now() < deadline || done,
            "mssql fake got one rpc"
        );
        if done {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("mssql fake task");
    let batch = captured.lock().expect("capture lock").clone();
    let topic_ucs2 = mv02a_ucs2(topic);
    assert!(
        batch
            .windows(topic_ucs2.len())
            .any(|w| w == topic_ucs2.as_slice()),
        "rpc carries the topic bound"
    );
    let payload_text = String::from_utf8_lossy(payload).into_owned();
    let payload_ucs2 = mv02a_ucs2(&payload_text);
    assert!(
        batch
            .windows(payload_ucs2.len())
            .any(|w| w == payload_ucs2.as_slice()),
        "rpc carries the payload with the quote bound"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-ms-act/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics: {body}");
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(1),
        "success: {body}"
    );
    assert_eq!(
        body["metrics"]["failed"],
        serde_json::json!(0),
        "failed: {body}"
    );
    kernel.finish().await;
}

/// MV-02a N2: an Oracle action against a dead server counts one failed
/// forward. The broker keeps serving. The payload carries a quote.
#[tokio::test]
async fn mv02a_oracle_down_server_counts_failure_and_keeps_state() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv02a-ora-down",
            "type": "oracle",
            "server": "127.0.0.1:1",
            "username": "u",
            "password": "pwd",
            "service_name": "XE",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let sql = "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv02a-ora-down-act",
            "type": "oracle",
            "connector": "mv02a-ora-down",
            "parameters": {"sql": sql},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-ora-down-act",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "action read: {body}");
    assert_eq!(body["parameters"]["sql"], serde_json::json!(sql), "{body}");
    let topic = "mv02a/in/ora-down";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv02a-ora-down-rule",
            "sql": format!("SELECT * FROM \"{topic}\""),
            "actions": ["mv02a-ora-down-act"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv02a-ora-down-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(topic, 0, 0, false, br#"{"v":"a\"b"}"#),
    )
    .await;
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let (_, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/actions/mv02a-ora-down-act/metrics",
            serde_json::Value::Null,
        )
        .await;
        if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the dead-target fire failed in time: {body}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv02a-ora-down-act/metrics",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics: {body}");
    assert_eq!(
        body["metrics"]["success"],
        serde_json::json!(0),
        "success stays zero: {body}"
    );
    assert_eq!(
        body["metrics"]["failed"],
        serde_json::json!(1),
        "one fire counts one failed: {body}"
    );
    kernel.finish().await;
}

/// MV-02a N2: every relational kind counts a dead-server fire as failed.
/// The broker keeps serving. This proves the Limits claim per kind.
#[tokio::test]
async fn mv02a_relational_dead_server_counts_failed_for_all_kinds() {
    let kinds: &[(&str, &str, serde_json::Value, &str)] = &[
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
        ),
        (
            "alloydb",
            "alloydb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "username": "u",
                "service_name": "XE",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
    ];
    for (kind, action_type, fields, sql) in kinds {
        let kernel = spawn_kernel("").await;
        let connector = format!("mv02a-dead-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": *action_type,
        });
        for (key, value) in fields.as_object().expect("connector fields") {
            connector_body[key] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let action = format!("mv02a-dead-act-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {"sql": *sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let topic = format!("mv02a/in/dead-{kind}");
        let rule = format!("mv02a-dead-rule-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule,
                "sql": format!("SELECT * FROM \"{topic}\""),
                "actions": [action.clone()],
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} rule create: {body}");
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-dead-pub-{kind}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"v":"a\"b"}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} dead-target fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} metrics: {body}");
        assert_eq!(
            body["metrics"]["failed"],
            serde_json::json!(1),
            "{kind} one fire counts one failed: {body}"
        );
        kernel.finish().await;
    }
}

/// MV-02a N3: an accepted action update swaps the statement, a refused
/// update keeps the old delivery, and a restart keeps the state. This
/// runs for all six kinds against a dead server through the broker.
#[tokio::test]
async fn mv02a_relational_update_and_restart_for_all_kinds() {
    let kinds: &[(&str, &str, serde_json::Value, &str, &str)] = &[
        (
            "mysql",
            "mysql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (?, ?, ?)",
            "INSERT INTO t2(topic, qos, payload) VALUES (?, ?, ?)",
        ),
        (
            "alloydb",
            "alloydb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "cockroachdb",
            "cockroachdb",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
                "password": "pwd",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
        (
            "mssql",
            "mssql",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($2, $4, $5)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($2, $4, $5)",
        ),
        (
            "oracle",
            "oracle",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "username": "u",
                "service_name": "XE",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES (:1, :2, :3)",
            "INSERT INTO t2(topic, qos, payload) VALUES (:1, :2, :3)",
        ),
        (
            "redshift",
            "redshift",
            serde_json::json!({
                "server": "127.0.0.1:1",
                "database": "db",
                "username": "u",
            }),
            "INSERT INTO t(topic, qos, payload) VALUES ($1, $2, $3)",
            "INSERT INTO t2(topic, qos, payload) VALUES ($1, $2, $3)",
        ),
    ];
    for (kind, action_type, fields, sql_v1, sql_v2) in kinds {
        let mut kernel = spawn_kernel("").await;
        let connector = format!("mv02a-upd-{kind}");
        let mut connector_body = serde_json::json!({
            "name": connector,
            "type": *action_type,
        });
        for (key, value) in fields.as_object().expect("connector fields") {
            connector_body[key] = value.clone();
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/connectors",
            connector_body,
        )
        .await;
        assert_eq!(status, 201, "{kind} connector create: {body}");
        let action = format!("mv02a-upd-act-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {"sql": *sql_v1},
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} action create: {body}");
        let topic = format!("mv02a/in/upd-{kind}");
        let rule = format!("mv02a-upd-rule-{kind}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule,
                "sql": format!("SELECT * FROM \"{topic}\""),
                "actions": [action.clone()],
            }),
        )
        .await;
        assert_eq!(status, 201, "{kind} rule create: {body}");
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-upd-pub-{kind}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":1}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} first fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::PUT,
            &format!("/api/v5/actions/{action}"),
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {"sql": *sql_v2},
            }),
        )
        .await;
        assert_eq!(status, 200, "{kind} accepted update: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} action read: {body}");
        assert_eq!(
            body["parameters"]["sql"],
            serde_json::json!(*sql_v2),
            "{body}"
        );
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":2}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 2 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} updated fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::PUT,
            &format!("/api/v5/actions/{action}"),
            serde_json::json!({
                "name": action,
                "type": *action_type,
                "connector": connector,
                "parameters": {},
            }),
        )
        .await;
        assert_eq!(status, 400, "{kind} missing sql refuses: {body}");
        assert!(
            body["message"].as_str().unwrap_or("").contains(&action)
                && body["message"]
                    .as_str()
                    .unwrap_or("")
                    .contains("parameters.sql"),
            "{kind} refusal names the action and the field: {body}"
        );
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":3}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 3 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} refused-update fire kept delivery: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        mv00_restart(&mut kernel).await;
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/{action}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "{kind} action survives restart: {body}");
        assert_eq!(
            body["parameters"]["sql"],
            serde_json::json!(*sql_v2),
            "{body}"
        );
        let mut publisher = mv00_connect(&kernel, &format!("mv02a-upd-pub2-{kind}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&topic, 0, 0, false, br#"{"u":4}"#),
        )
        .await;
        let deadline = std::time::Instant::now() + STEP;
        loop {
            let (_, body) = mv00_api(
                &kernel,
                reqwest::Method::GET,
                &format!("/api/v5/actions/{action}/metrics"),
                serde_json::Value::Null,
            )
            .await;
            if body["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{kind} post-restart fire failed in time: {body}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        kernel.finish().await;
    }
}
