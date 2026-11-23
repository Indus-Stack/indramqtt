use super::*;

/// Two `pgsql` actions on one connector own separate statement shapes: each
/// delivers the engine row (topic, QoS, payload) under its own Parameters,
/// and the per-action counters attribute the fires.
#[tokio::test]
async fn mv01_pgsql_action_delivers_engine_rows_to_connector_settings() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            for _ in 0..2 {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 1).await;
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    for (suffix, sentinel) in [("a", "MV01-A-SQL"), ("b", "MV01-B-SQL")] {
        let id = format!("mv01-pg-{suffix}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": id,
                "type": "pgsql",
                "connector": "mv01-pg",
                "parameters": {"sql": format!("{sentinel} INSERT $1, $2, $3")},
            }),
        )
        .await;
        assert_eq!(status, 201, "action {id} create: {body}");
        assert_eq!(body["status"], "connected", "action {id}: {body}");
        let rule_id = format!("mv01-pg-rule-{suffix}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": rule_id,
                "sql": format!("SELECT * FROM \"mv01/in/{suffix}\""),
                "actions": [id],
            }),
        )
        .await;
        assert_eq!(status, 201, "rule {rule_id} create: {body}");
    }
    // One publish per action fires its own rule; each action's pool dials
    // the fake backend and flushes its own statement with the engine row.
    for (suffix, payload) in [("a", &b"{\"v\":1}"[..]), ("b", &b"{\"v\":2}"[..])] {
        let mut publisher = mv00_connect(&kernel, &format!("mv01-pg-pub-{suffix}")).await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv01/in/{suffix}"), 0, 0, false, payload),
        )
        .await;
        drop(publisher);
    }
    // The sink flusher outside this test drives both pools; bound the wait.
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let arrived = captured.lock().expect("capture lock").len() == 2;
        assert!(
            std::time::Instant::now() < deadline || arrived,
            "both pgsql actions flushed in time"
        );
        if arrived {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    // Per-action counters attributed one success each.
    for suffix in ["a", "b"] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/actions/mv01-pg-{suffix}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "action {suffix} metrics: {body}");
        assert_eq!(body["metrics"]["success"], serde_json::json!(1), "{body}");
        assert_eq!(body["metrics"]["failed"], serde_json::json!(0), "{body}");
    }
    // Both actions delivered under their own statements with the engine
    // row (topic, QoS, payload) bound by the sink.
    assert_eq!(captured.len(), 2);
    for (statement, rows) in &captured {
        assert!(rows.len() == 1, "one row per statement: {rows:?}");
        let row = &rows[0];
        assert_eq!(row.len(), 3, "topic, qos, payload bound: {row:?}");
        assert_eq!(row[1], &b"0"[..]);
        let (topic, payload) = if statement.contains("MV01-A-SQL") {
            ("mv01/in/a", "{\"v\":1}".as_bytes())
        } else if statement.contains("MV01-B-SQL") {
            ("mv01/in/b", "{\"v\":2}".as_bytes())
        } else {
            unreachable!("each connection binds its action's statement: {statement:?}");
        };
        assert_eq!(row[0], topic.as_bytes());
        assert_eq!(row[2], payload);
    }
    kernel.finish().await;
}

/// An action update swaps the statement live (new publishes use it); a
/// refused update (missing `sql`) answers 400 naming the field and the
/// old statement keeps delivering.
#[tokio::test]
async fn mv01_pgsql_action_update_swaps_statement_and_refusal_keeps_old() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured = Arc::new(Mutex::new(Vec::<(String, Vec<Vec<Vec<u8>>>)>::new()));
    let fake = {
        let captured = Arc::clone(&captured);
        tokio::spawn(async move {
            for _ in 0..3 {
                let stream = mv01_pg_accept(&listener).await;
                mv01_pg_serve_one(stream, Arc::clone(&captured), "u", "pwd", "db", 1).await;
            }
        })
    };
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "pgsql",
            "connector": "mv01-pg-up",
            "parameters": {"sql": "MV01-PGUP-V1 INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-up-rule",
            "sql": "SELECT * FROM \"mv01/in/up\"",
            "actions": ["mv01-pg-up"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-up-pub").await;
    // The first publish flushes the created statement.
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/up", 0, 0, false, br#"{"u":1}"#),
    )
    .await;
    // An update that drops `sql` is refused with the field named; the
    // stored action and the delivering statement are untouched.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-up",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "pgsql",
            "connector": "mv01-pg-up",
            "parameters": {},
        }),
    )
    .await;
    assert_eq!(status, 400, "missing sql refuses: {body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("mv01-pg-up") && message.contains("parameters.sql"),
        "the refusal names the action and the field: {message}"
    );
    // The old generation kept delivering (a fresh backend connection).
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/up", 0, 0, false, br#"{"u":2}"#),
    )
    .await;
    // Wait for the refused-update publish to flush on the old sink before
    // the replacement swaps it. A publish still in the broker task would
    // otherwise land on the new statement.
    mv01_wait_pg_batches(&captured, 2).await;
    // A full replacement swaps the statement in for the next publish.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-up",
        serde_json::json!({
            "name": "mv01-pg-up",
            "type": "pgsql",
            "connector": "mv01-pg-up",
            "parameters": {"sql": "MV01-PGUP-V2 INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 200, "action update: {body}");
    assert_eq!(body["status"], "connected", "{body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/up", 0, 0, false, br#"{"u":3}"#),
    )
    .await;
    // Bound the wait for all three flushes.
    let deadline = std::time::Instant::now() + STEP;
    loop {
        let arrived = captured.lock().expect("capture lock").len() == 3;
        assert!(
            std::time::Instant::now() < deadline || arrived,
            "all three statements flushed in time"
        );
        if arrived {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    fake.await.expect("pg fake task");
    let captured = std::sync::Mutex::into_inner(Arc::try_unwrap(captured).expect("sole owner"))
        .expect("capture lock");
    // Flush order follows connection order: created, refused-update
    // still old, updated new. The prepared statement carries the
    // driver's generated name; the sql follows after the separator.
    let sql_of = |index: usize| -> String {
        captured[index]
            .0
            .split_once('|')
            .expect("statement capture")
            .1
            .to_string()
    };
    assert_eq!(sql_of(0), "MV01-PGUP-V1 INSERT $1, $2, $3");
    assert_eq!(sql_of(1), "MV01-PGUP-V1 INSERT $1, $2, $3");
    assert_eq!(sql_of(2), "MV01-PGUP-V2 INSERT $1, $2, $3");
    // Every flush carries the engine row (topic, QoS, payload) bound by
    // the action to its own publish.
    let payloads: [&[u8]; 3] = [br#"{"u":1}"#, br#"{"u":2}"#, br#"{"u":3}"#];
    for (index, expected_payload) in payloads.iter().enumerate() {
        let rows = &captured[index].1;
        assert!(rows.len() == 1, "one row per flush: {rows:?}");
        let row = &rows[0];
        assert_eq!(row.len(), 3, "topic, qos, payload bound: {row:?}");
        assert_eq!(row[0], &b"mv01/in/up"[..]);
        assert_eq!(row[1], &b"0"[..]);
        assert_eq!(row[2], *expected_payload);
    }
    kernel.finish().await;
}

/// Wait for one more captured PostgreSQL batch. The fake flushes one
/// batch per connection, so the count is the number of completed sends.
async fn mv01_wait_pg_batches(captured: &PgCaptured, count: usize) {
    let deadline = std::time::Instant::now() + STEP;
    loop {
        if captured.lock().expect("capture lock").len() >= count {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the pgsql batches arrived in time: wanted {count}, got {}",
            captured.lock().expect("capture lock").len()
        );
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

/// A pgsql action stops on disable, resumes on enable and stops on delete.
#[tokio::test]
async fn mv01_pgsql_action_disable_enable_delete() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-life",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-life",
            "type": "pgsql",
            "connector": "mv01-pg-life",
            "parameters": {"sql": "MV01-LIFE INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-life-rule",
            "sql": "SELECT * FROM \"mv01/in/life\"",
            "actions": ["mv01-pg-life"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-life-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-life/enable/false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "disable: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":2}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        captured.lock().expect("capture lock").len(),
        1,
        "a disabled action delivers nothing"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/actions/mv01-pg-life/enable/true",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "enable: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":3}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 2).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/mv01-pg-life-rule",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "rule delete: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/actions/mv01-pg-life",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "action delete: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/life", 0, 0, false, br#"{"n":4}"#),
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    assert_eq!(
        captured.lock().expect("capture lock").len(),
        2,
        "a deleted action delivers nothing"
    );
    fake.abort();
    kernel.finish().await;
}

/// A pgsql action stores while its server is down, reports `disconnected`
/// from the real attempt, and delivers after the server starts.
#[tokio::test]
async fn mv01_pgsql_down_server_reports_disconnected_and_delivers_when_up() {
    let reserve = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a port");
    let address = reserve.local_addr().expect("reserved address");
    drop(reserve);
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-down",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-down",
            "type": "pgsql",
            "connector": "mv01-pg-down",
            "parameters": {"sql": "MV01-DOWN INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "a down target still stores: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-down-rule",
            "sql": "SELECT * FROM \"mv01/in/pgdown\"",
            "actions": ["mv01-pg-down"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("the server starts on the reserved port");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let mut publisher = mv00_connect(&kernel, "mv01-pg-down-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgdown", 0, 0, false, br#"{"up":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    fake.abort();
    kernel.finish().await;
}

/// A pgsql action delivers after a broker restart from the stored
/// connector and action.
#[tokio::test]
async fn mv01_pgsql_action_survives_restart() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let mut kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-restart",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-restart",
            "type": "pgsql",
            "connector": "mv01-pg-restart",
            "parameters": {"sql": "MV01-RESTART INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-restart-rule",
            "sql": "SELECT * FROM \"mv01/in/pgrestart\"",
            "actions": ["mv01-pg-restart"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-restart-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrestart", 0, 0, false, br#"{"r":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-pg-restart",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(body["status"], "connected", "the replay rebuilt it: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-restart-pub2").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrestart", 0, 0, false, br#"{"r":2}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 2).await;
    {
        let captured = captured.lock().expect("capture lock");
        assert!(captured[0].0.contains("MV01-RESTART"));
        assert!(captured[1].0.contains("MV01-RESTART"));
    }
    fake.abort();
    kernel.finish().await;
}

/// A connector update refuses when one dependent pgsql action cannot use
/// the new settings; the old statement keeps delivering and the refused
/// attempt leaks no connection or task.
#[tokio::test]
async fn mv01_pgsql_refused_connector_update_keeps_old_and_leaks_nothing() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let accepted = Arc::new(AtomicUsize::new(0));
    let fake_captured = Arc::clone(&captured);
    let fake_accepted = Arc::clone(&accepted);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            fake_accepted.fetch_add(1, Ordering::SeqCst);
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-refuse",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-refuse",
            "type": "pgsql",
            "connector": "mv01-pg-refuse",
            "parameters": {"sql": "MV01-REFUSE INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-refuse-rule",
            "sql": "SELECT * FROM \"mv01/in/pgrefuse\"",
            "actions": ["mv01-pg-refuse"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-refuse-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrefuse", 0, 0, false, br#"{"v":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    let accepted_before = accepted.load(Ordering::SeqCst);
    let tasks_before = tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks();
    // The new body drops `database`, so the dependent action cannot build.
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-pg-refuse",
        serde_json::json!({
            "name": "mv01-pg-refuse",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "username": "u",
            "password": "pwd",
        }),
    )
    .await;
    assert_eq!(status, 400, "the dependent build refuses: {body}");
    let message = body["message"].as_str().expect("refusal message");
    assert!(
        message.contains("mv01-pg-refuse") && message.contains("database"),
        "the refusal names the action and the field: {message}"
    );
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        accepted_before,
        "the refused update opens no connection"
    );
    let deadline = std::time::Instant::now() + STEP;
    let tasks_after = loop {
        let alive = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();
        if alive <= tasks_before || std::time::Instant::now() >= deadline {
            break alive;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    };
    assert!(
        tasks_after <= tasks_before,
        "the refused update leaks no task: before {tasks_before}, after {tasks_after}"
    );
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/pgrefuse", 0, 0, false, br#"{"v":2}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 2).await;
    {
        let captured = captured.lock().expect("capture lock");
        assert!(captured[1].0.contains("MV01-REFUSE"));
    }
    fake.abort();
    kernel.finish().await;
}

/// A successful connector update keeps two dependent actions on their own
/// statements.
#[tokio::test]
async fn mv01_pgsql_connector_update_keeps_two_action_statements() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake_captured), "u", "pwd", "db", 1).await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-two",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
            "pool_size": 2,
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    for (id, sql) in [
        ("mv01-pg-two-a", "MV01-TWO-A INSERT $1, $2, $3"),
        ("mv01-pg-two-b", "MV01-TWO-B INSERT $1, $2, $3"),
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/actions",
            serde_json::json!({
                "name": id,
                "type": "pgsql",
                "connector": "mv01-pg-two",
                "parameters": {"sql": sql},
            }),
        )
        .await;
        assert_eq!(status, 201, "action {id}: {body}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            serde_json::json!({
                "id": format!("{id}-rule"),
                "sql": format!("SELECT * FROM \"mv01/in/two/{id}\""),
                "actions": [id],
            }),
        )
        .await;
        assert_eq!(status, 201, "rule {id}: {body}");
    }
    let mut publisher = mv00_connect(&kernel, "mv01-pg-two-pub").await;
    for id in ["mv01-pg-two-a", "mv01-pg-two-b"] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv01/in/two/{id}"), 0, 0, false, id.as_bytes()),
        )
        .await;
    }
    mv01_wait_pg_batches(&captured, 2).await;
    // A second server observes the rebuild: after the update every row
    // must reach the new target. A no-op connector update fails here.
    let listener2 = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("second pg fake binds");
    let address2 = listener2.local_addr().expect("second pg fake address");
    let captured2: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake2_captured = Arc::clone(&captured2);
    let fake2 = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener2).await;
            mv01_pg_serve_one(stream, Arc::clone(&fake2_captured), "u", "pwd", "db", 1).await;
        }
    });
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-pg-two",
        serde_json::json!({
            "name": "mv01-pg-two",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address2.port()),
            "database": "db",
            "username": "u",
            "password": "pwd",
            "pool_size": 4,
        }),
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    for id in ["mv01-pg-two-a", "mv01-pg-two-b"] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv01/in/two/{id}"), 0, 0, false, id.as_bytes()),
        )
        .await;
    }
    mv01_wait_pg_batches(&captured2, 2).await;
    {
        let captured2 = captured2.lock().expect("capture lock");
        let statements: Vec<&str> = captured2
            .iter()
            .map(|(statement, _)| {
                statement
                    .split_once('|')
                    .map(|(_, sql)| sql)
                    .unwrap_or(statement.as_str())
            })
            .collect();
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.contains("MV01-TWO-A"))
                .count(),
            1,
            "action A rebuilt onto the new server: {statements:?}"
        );
        assert_eq!(
            statements
                .iter()
                .filter(|sql| sql.contains("MV01-TWO-B"))
                .count(),
            1,
            "action B rebuilt onto the new server: {statements:?}"
        );
    }
    fake.abort();
    fake2.abort();
    kernel.finish().await;
}

/// A stored pgsql action whose password reference disappears reports the
/// error state and raises the build alarm at boot.
#[tokio::test]
async fn mv01_pgsql_failed_boot_wiring_reports_error_and_alarm() {
    let mut kernel = spawn_kernel("").await;
    let secret = kernel.scratch.join("mv01-pg-password");
    std::fs::write(&secret, "pwd").expect("secret file");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-boot",
            "type": "postgres",
            "server": "127.0.0.1:9",
            "database": "db",
            "username": "u",
            "password": format!("file:{}", secret.display()),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-boot",
            "type": "pgsql",
            "connector": "mv01-pg-boot",
            "parameters": {"sql": "MV01-BOOT INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    std::fs::remove_file(&secret).expect("remove the secret");
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/actions/mv01-pg-boot",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "the failed action stays readable: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    let reason = body["status_reason"].as_str().expect("status reason");
    assert!(
        reason.contains("password"),
        "the reason names the field: {reason}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/alarms",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "alarms read: {body}");
    let alarm = body["data"]
        .as_array()
        .expect("alarm rows")
        .iter()
        .find(|row| row["name"] == "action_build_failed:mv01-pg-boot")
        .expect("the boot failure raised its alarm");
    assert!(
        alarm["message"]
            .as_str()
            .is_some_and(|text| text.contains("password")),
        "the alarm carries the build reason: {alarm}"
    );
    kernel.finish().await;
}

/// A password and a database that contain URL delimiters reach the
/// driver as values, not as a hand-joined URL.
#[tokio::test]
async fn mv01_pgsql_special_password_and_database_reach_driver() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(
                stream,
                Arc::clone(&fake_captured),
                "u",
                "p@ss/w?rd#1",
                "db@x/y?z#w",
                1,
            )
            .await;
        }
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-special",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db@x/y?z#w",
            "username": "u",
            "password": "p@ss/w?rd#1",
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-special",
            "type": "pgsql",
            "connector": "mv01-pg-special",
            "parameters": {"sql": "MV01-SPECIAL INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-special-rule",
            "sql": "SELECT * FROM \"mv01/in/special\"",
            "actions": ["mv01-pg-special"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-special-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/special", 0, 0, false, br#"{"s":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    fake.abort();
    kernel.finish().await;
}

/// The pgsql transport applies `ssl.verify = verify_peer` and refuses
/// `verify_none` instead of accepting a value it cannot apply.
#[tokio::test]
async fn mv01_pgsql_tls_verify_is_applied_or_refused() {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "postgres",
            "server": "127.0.0.1:9",
            "database": "db",
            "username": "u",
            "password": "pwd",
            "ssl": {"enable": true, "verify": "verify_none"},
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "pgsql",
            "connector": "mv01-pg-tls",
            "parameters": {"sql": "MV01-TLS INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 400, "verify_none is refused: {body}");
    assert_eq!(body["code"], "ALREADY_EXISTS", "{body}");
    let message = body["message"].as_str().expect("error message");
    assert!(
        message.contains("ssl.verify") && message.contains("verify_none"),
        "the refusal names the setting: {message}"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/connectors/mv01-pg-tls",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "postgres",
            "server": "127.0.0.1:9",
            "database": "db",
            "username": "u",
            "password": "pwd",
            "ssl": {"enable": true, "verify": "verify_peer"},
        }),
    )
    .await;
    assert_eq!(status, 200, "connector update: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-tls",
            "type": "pgsql",
            "connector": "mv01-pg-tls",
            "parameters": {"sql": "MV01-TLS INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "verify_peer builds: {body}");
    assert_eq!(body["status"], "disconnected", "{body}");
    kernel.finish().await;
}

/// A `file:` password reference resolves before the driver dials, and the
/// fake server checks the real password.
#[tokio::test]
async fn mv01_pgsql_secret_password_reaches_driver() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("pg fake binds");
    let address = listener.local_addr().expect("pg fake address");
    let captured: PgCaptured = Arc::new(Mutex::new(Vec::new()));
    let fake_captured = Arc::clone(&captured);
    let fake = tokio::spawn(async move {
        loop {
            let stream = mv01_pg_accept(&listener).await;
            mv01_pg_serve_one(
                stream,
                Arc::clone(&fake_captured),
                "u",
                "pwd-from-file",
                "db",
                1,
            )
            .await;
        }
    });
    let kernel = spawn_kernel("").await;
    let secret = kernel.scratch.join("mv01-pg-password");
    std::fs::write(&secret, "pwd-from-file").expect("secret file");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/connectors",
        serde_json::json!({
            "name": "mv01-pg-secret",
            "type": "postgres",
            "server": format!("127.0.0.1:{}", address.port()),
            "database": "db",
            "username": "u",
            "password": format!("file:{}", secret.display()),
        }),
    )
    .await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/actions",
        serde_json::json!({
            "name": "mv01-pg-secret",
            "type": "pgsql",
            "connector": "mv01-pg-secret",
            "parameters": {"sql": "MV01-SECRET INSERT $1, $2, $3"},
        }),
    )
    .await;
    assert_eq!(status, 201, "action create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "mv01-pg-secret-rule",
            "sql": "SELECT * FROM \"mv01/in/secret\"",
            "actions": ["mv01-pg-secret"],
        }),
    )
    .await;
    assert_eq!(status, 201, "rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv01-pg-secret-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311("mv01/in/secret", 0, 0, false, br#"{"secret":1}"#),
    )
    .await;
    mv01_wait_pg_batches(&captured, 1).await;
    fake.abort();
    kernel.finish().await;
}
