use super::*;

#[cfg(feature = "clickhouse")]
#[tokio::test]
async fn mv02b_clickhouse_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-clickhouse");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "Ok.").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "clickhouse", "name": "mv02b-ch",
                       "url": endpoint, "database": "db", "table": "events"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "clickhouse", "name": "mv02b-ch-act",
                       "connector": "mv02b-ch", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-ch-rule", "name": "mv02b-ch-rule",
                       "sql": "SELECT * FROM \"mv02b/ch\"", "actions": ["mv02b-ch-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/ch", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert_eq!(got[0].path, "/");
    assert_eq!(
        got[0].query.replace('+', " "),
        "query=INSERT INTO db.events FORMAT JSONEachRow",
        "query: {}",
        got[0].query
    );
    let row: serde_json::Value =
        serde_json::from_str(Mv02bFake::body_text(&got[0]).trim()).expect("row is JSON");
    assert_eq!(
        row,
        json!({"topic": "mv02b/ch", "qos": 0, "payload": "{\"v\":1}"}),
        "row: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-ch-act", 1).await;

    // The action override moves the target table.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-ch-act",
            json!({"type": "clickhouse", "name": "mv02b-ch-act",
                       "connector": "mv02b-ch", "parameters": {"table": "events2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/ch", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert!(got[1].query.contains("events2"), "query: {}", got[1].query);
    mv02b_wait_success(&server, &token, "mv02b-ch-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-ch",
            json!({"type": "clickhouse", "name": "mv02b-ch",
                       "url": endpoint, "database": "db"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped table refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("table"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/ch", "{\"v\":3}").await;
    let got = fake.wait_for(3).await;
    assert!(got[2].query.contains("events2"), "query: {}", got[2].query);
    mv02b_wait_success(&server, &token, "mv02b-ch-act", 3).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/ch", "{\"v\":4}").await;
    let (status, _) = server
        .get_auth("/api/v5/actions/mv02b-ch-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    // The failure may still be in flight; poll for it.
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-ch-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-ch-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server.get_auth("/api/v5/connectors/mv02b-ch", &token).await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 200, "Ok.").await;
    mv02b_publish(&server, &token, "mv02b/ch", "{\"v\":5}").await;
    let got = fake.wait_for(1).await;
    assert!(got[0].query.contains("events2"), "query: {}", got[0].query);
    mv02b_wait_success(&server, &token, "mv02b-ch-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_influxdb_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-influxdb");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(204, "").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "influxdb", "name": "mv02b-influx",
                       "server": endpoint, "bucket": "b", "org": "o",
                       "token": "t", "measurement": "m"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "influxdb", "name": "mv02b-influx-act",
                       "connector": "mv02b-influx", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-influx-rule", "name": "mv02b-influx-rule",
                       "sql": "SELECT * FROM \"mv02b/influx\"", "actions": ["mv02b-influx-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/influx", "{\"temp\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/api/v2/write");
    assert!(
        got[0].query.contains("precision=ms"),
        "query: {}",
        got[0].query
    );
    assert!(got[0].query.contains("bucket=b"), "query: {}", got[0].query);
    assert!(
        Mv02bFake::body_text(&got[0]).starts_with("m,topic=mv02b/influx "),
        "line: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-influx-act", 1).await;

    // The action override moves the measurement and the precision.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-influx-act",
            json!({"type": "influxdb", "name": "mv02b-influx-act",
                       "connector": "mv02b-influx",
                       "parameters": {"measurement": "m2", "precision": "s"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/influx", "{\"temp\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got[1].query.contains("precision=s"),
        "query: {}",
        got[1].query
    );
    assert!(
        Mv02bFake::body_text(&got[1]).starts_with("m2,topic=mv02b/influx "),
        "line: {}",
        Mv02bFake::body_text(&got[1])
    );
    mv02b_wait_success(&server, &token, "mv02b-influx-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-influx",
            json!({"type": "influxdb", "name": "mv02b-influx",
                       "server": endpoint, "org": "o", "token": "t", "measurement": "m"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped bucket refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("bucket"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/influx", "{\"temp\": 23.5}").await;
    let got = fake.wait_for(3).await;
    assert!(
        Mv02bFake::body_text(&got[2]).starts_with("m2,topic=mv02b/influx "),
        "line: {}",
        Mv02bFake::body_text(&got[2])
    );
    mv02b_wait_success(&server, &token, "mv02b-influx-act", 3).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/influx", "{\"temp\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-influx-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-influx-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server
        .get_auth("/api/v5/connectors/mv02b-influx", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 204, "").await;
    mv02b_publish(&server, &token, "mv02b/influx", "{\"temp\": 25.5}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).starts_with("m2,topic=mv02b/influx "),
        "line: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-influx-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_influxdb_escapes_specials_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-escapes");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(204, "").await;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{}", fake.port);

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "influxdb", "name": "mv02b-esc",
                       "server": endpoint, "bucket": "b", "org": "o",
                       "token": "t", "measurement": "m"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    for (action, topic) in [
        ("mv02b-esc-act", "mv02b/esc"),
        ("mv02b-esc-tag-act", "mv02b/a,b c"),
        ("mv02b-esc-nl-act", "mv02b/nl\nrow"),
    ] {
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "influxdb", "name": action,
                           "connector": "mv02b-esc", "parameters": {}}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": action, "name": action,
                           "sql": format!("SELECT * FROM \"{topic}\""),
                           "actions": [action]}),
                &token,
            )
            .await;
        assert_eq!(status, 201);
    }

    // Quotes, backslashes, spaces and commas in the payload
    // render as escaped line-protocol field strings.
    mv02b_publish(
        &server,
        &token,
        "mv02b/esc",
        "{\"msg\":\"a\\\"b\\\\c d,e\"}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert_eq!(got[0].path, "/api/v2/write");
    let text = Mv02bFake::body_text(&got[0]);
    let (head, stamp) = text
        .rsplit_once(' ')
        .unwrap_or_else(|| panic!("line: {text}"));
    assert_eq!(
        head, "m,topic=mv02b/esc qos=0i,payload=\"{\\\"msg\\\":\\\"a\\\\\\\"b\\\\\\\\c d,e\\\"}\"",
        "line: {text}"
    );
    assert!(
        stamp.trim_end().bytes().all(|b| b.is_ascii_digit()) && !stamp.trim().is_empty(),
        "stamp: {text}"
    );
    mv02b_wait_success(&server, &token, "mv02b-esc-act", 1).await;

    // Commas and spaces in the topic render as escaped tag values.
    mv02b_publish(&server, &token, "mv02b/a,b c", "{\"v\":1}").await;
    let got = fake.wait_for(2).await;
    let text = Mv02bFake::body_text(&got[1]);
    let (head, stamp) = text
        .rsplit_once(' ')
        .unwrap_or_else(|| panic!("line: {text}"));
    assert_eq!(
        head, "m,topic=mv02b/a\\,b\\ c qos=0i,payload=\"{\\\"v\\\":1}\"",
        "line: {text}"
    );
    assert!(
        stamp.trim_end().bytes().all(|b| b.is_ascii_digit()) && !stamp.trim().is_empty(),
        "stamp: {text}"
    );
    mv02b_wait_success(&server, &token, "mv02b-esc-tag-act", 1).await;

    // Newlines in the payload and the topic stay inside one
    // line-protocol row: both render as the two characters `\n`.
    mv02b_publish(
        &server,
        &token,
        "mv02b/nl\nrow",
        "{\"msg\":\"line1\\nline2\"}",
    )
    .await;
    let got = fake.wait_for(3).await;
    let text = Mv02bFake::body_text(&got[2]);
    let (head, stamp) = text
        .rsplit_once(' ')
        .unwrap_or_else(|| panic!("line: {text}"));
    assert_eq!(
        head, "m,topic=mv02b/nl\\nrow qos=0i,payload=\"{\\\"msg\\\":\\\"line1\\\\nline2\\\"}\"",
        "line: {text}"
    );
    assert!(
        stamp.trim_end().bytes().all(|b| b.is_ascii_digit()) && !stamp.trim().is_empty(),
        "stamp: {text}"
    );
    mv02b_wait_success(&server, &token, "mv02b-esc-nl-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_greptimedb_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-greptimedb");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "greptimedb", "name": "mv02b-greptime",
                       "server": endpoint, "database": "public", "table": "events",
                       "format": "influx", "precision": "us"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "greptime", "name": "mv02b-greptime-act",
                       "connector": "mv02b-greptime", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "the greptime alias creates");
    let (status, _) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": "mv02b-greptime-rule", "name": "mv02b-greptime-rule",
                       "sql": "SELECT * FROM \"mv02b/greptime\"", "actions": ["mv02b-greptime-act"]}),
                &token,
            )
            .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/greptime", "{\"temp\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/v1/influxdb/api/v2/write");
    assert!(
        got[0].query.contains("db=public") && got[0].query.contains("precision=us"),
        "query: {}",
        got[0].query
    );
    assert!(
        Mv02bFake::body_text(&got[0]).contains("events"),
        "line: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-greptime-act", 1).await;

    // The action override wins over the connector: SQL at
    // nanosecond precision.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-greptime-act",
            json!({"type": "greptime", "name": "mv02b-greptime-act",
                       "connector": "mv02b-greptime",
                       "parameters": {"format": "sql", "precision": "ns"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/greptime", "{\"temp\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert_eq!(got[1].path, "/v1/sql");
    assert!(
        got[1].query.contains("db=public"),
        "query: {}",
        got[1].query
    );
    mv02b_wait_success(&server, &token, "mv02b-greptime-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-greptime",
            json!({"type": "greptimedb", "name": "mv02b-greptime",
                       "server": endpoint, "database": "public"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped table refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("table"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/greptime", "{\"temp\": 23.5}").await;
    let got = fake.wait_for(3).await;
    assert_eq!(got[2].path, "/v1/sql");
    assert!(
        got[2].query.contains("db=public"),
        "query: {}",
        got[2].query
    );
    mv02b_wait_success(&server, &token, "mv02b-greptime-act", 3).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/greptime", "{\"temp\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-greptime-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-greptime-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server
        .get_auth("/api/v5/connectors/mv02b-greptime", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 200, "{}").await;
    mv02b_publish(&server, &token, "mv02b/greptime", "{\"temp\": 25.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/v1/sql");
    assert!(
        got[0].query.contains("db=public"),
        "query: {}",
        got[0].query
    );
    mv02b_wait_success(&server, &token, "mv02b-greptime-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_opentsdb_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-opentsdb");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "opentsdb", "name": "mv02b-opents",
                       "server": endpoint, "metric": "m", "value": "${payload.temp}"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "opents", "name": "mv02b-opents-act",
                       "connector": "mv02b-opents", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "the opents alias creates");
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-opents-rule", "name": "mv02b-opents-rule",
                       "sql": "SELECT * FROM \"mv02b/opents\"", "actions": ["mv02b-opents-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/opents", "{\"temp\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/api/put");
    assert_eq!(got[0].query, "summary", "query: {}", got[0].query);
    assert!(
        Mv02bFake::body_text(&got[0]).contains("\"metric\":\"m\""),
        "points: {}",
        Mv02bFake::body_text(&got[0])
    );
    assert!(
        Mv02bFake::body_text(&got[0]).contains("21.5"),
        "value: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-opents-act", 1).await;

    // The action override moves the metric.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-opents-act",
            json!({"type": "opents", "name": "mv02b-opents-act",
                       "connector": "mv02b-opents", "parameters": {"metric": "m2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/opents", "{\"temp\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert!(
        Mv02bFake::body_text(&got[1]).contains("\"metric\":\"m2\""),
        "points: {}",
        Mv02bFake::body_text(&got[1])
    );
    mv02b_wait_success(&server, &token, "mv02b-opents-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-opents",
            json!({"type": "opentsdb", "name": "mv02b-opents",
                       "server": endpoint, "value": "${payload.temp}"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped metric refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("metric"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/opents", "{\"temp\": 23.5}").await;
    let got = fake.wait_for(3).await;
    assert!(
        Mv02bFake::body_text(&got[2]).contains("\"metric\":\"m2\""),
        "points: {}",
        Mv02bFake::body_text(&got[2])
    );
    mv02b_wait_success(&server, &token, "mv02b-opents-act", 3).await;

    // A connector-level `summary: false` drops the `?summary` query.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "opentsdb", "name": "mv02b-opents-nosum",
                       "server": endpoint, "metric": "m", "value": "v", "summary": false}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "opents", "name": "mv02b-opents-nosum-act",
                       "connector": "mv02b-opents-nosum", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "the opents alias creates");
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-opents-nosum-rule", "name": "mv02b-opents-nosum-rule",
                       "sql": "SELECT * FROM \"mv02b/opents-nosum\"",
                       "actions": ["mv02b-opents-nosum-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02b/opents-nosum", "{\"temp\": 26.5}").await;
    let got = fake.wait_for(4).await;
    assert_eq!(got[3].path, "/api/put");
    assert_eq!(got[3].query, "", "query: {}", got[3].query);
    mv02b_wait_success(&server, &token, "mv02b-opents-nosum-act", 1).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/opents", "{\"temp\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-opents-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-opents-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server
        .get_auth("/api/v5/connectors/mv02b-opents", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 200, "").await;
    mv02b_publish(&server, &token, "mv02b/opents", "{\"temp\": 25.5}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).contains("\"metric\":\"m2\""),
        "points: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-opents-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_tdengine_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-tdengine");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{\"code\":0,\"rows\":1}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "tdengine", "name": "mv02b-td",
                       "server": endpoint, "database": "power", "stable": "meters",
                       "subtable": "d_one",
                       "metrics": {"temp": "${payload.temperature}"},
                       "tags": {"location": "room1"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "tdengine", "name": "mv02b-td-act",
                       "connector": "mv02b-td", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-td-rule", "name": "mv02b-td-rule",
                       "sql": "SELECT * FROM \"mv02b/td\"", "actions": ["mv02b-td-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/td", "{\"temperature\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert_eq!(got[0].path, "/rest/sql/power", "path: {}", got[0].path);
    let text = Mv02bFake::body_text(&got[0]);
    let prefix = "INSERT INTO d_one USING meters TAGS ('room1') VALUES (";
    assert!(text.starts_with(prefix), "statement: {text}");
    let rest = &text[prefix.len()..];
    let (stamp, tail) = rest
        .split_once(", 21.5);")
        .unwrap_or_else(|| panic!("values: {text}"));
    assert!(
        !stamp.is_empty() && stamp.bytes().all(|b| b.is_ascii_digit()),
        "stamp: {text}"
    );
    assert!(tail.trim().is_empty(), "tail: {text}");
    mv02b_wait_success(&server, &token, "mv02b-td-act", 1).await;

    // The action override moves the sub-table.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-td-act",
            json!({"type": "tdengine", "name": "mv02b-td-act",
                       "connector": "mv02b-td", "parameters": {"subtable": "d_two"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/td", "{\"temperature\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert!(
        Mv02bFake::body_text(&got[1]).contains("d_two USING meters"),
        "insert: {}",
        Mv02bFake::body_text(&got[1])
    );
    mv02b_wait_success(&server, &token, "mv02b-td-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-td",
            json!({"type": "tdengine", "name": "mv02b-td",
                       "server": endpoint, "database": "power", "stable": "meters",
                       "metrics": {"temp": "${payload.temperature}"},
                       "tags": {"location": "room1"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped subtable refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("subtable"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/td", "{\"temperature\": 23.5}").await;
    let got = fake.wait_for(3).await;
    assert!(
        Mv02bFake::body_text(&got[2]).contains("d_two USING meters"),
        "insert: {}",
        Mv02bFake::body_text(&got[2])
    );
    mv02b_wait_success(&server, &token, "mv02b-td-act", 3).await;

    // A token connector rides the REST transport: the same row
    // shape posts to `/{database}` instead of `/rest/sql/{database}`.
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "tdengine", "name": "mv02b-td-token",
                       "server": endpoint, "database": "power", "stable": "meters",
                       "subtable": "d_token", "metrics": {"temp": "${payload.temperature}"},
                       "tags": {"location": "room1"}, "token": "tok-1"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "tdengine", "name": "mv02b-td-token-act",
                       "connector": "mv02b-td-token", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-td-token-rule", "name": "mv02b-td-token-rule",
                       "sql": "SELECT * FROM \"mv02b/td-token\"",
                       "actions": ["mv02b-td-token-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02b/td-token", "{\"temperature\": 20.5}").await;
    let got = fake.wait_for(4).await;
    assert_eq!(got[3].method, "POST");
    assert_eq!(got[3].path, "/power", "path: {}", got[3].path);
    let text = Mv02bFake::body_text(&got[3]);
    assert!(text.contains("d_token USING meters"), "insert: {text}");
    mv02b_wait_success(&server, &token, "mv02b-td-token-act", 1).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/td", "{\"temperature\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-td-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-td-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server.get_auth("/api/v5/connectors/mv02b-td", &token).await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 200, "{\"code\":0,\"rows\":1}").await;
    mv02b_publish(&server, &token, "mv02b/td", "{\"temperature\": 25.5}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).contains("d_two USING meters"),
        "insert: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-td-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_iotdb_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-iotdb");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{\"code\":200}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "iotdb", "name": "mv02b-iot",
                       "endpoint": endpoint, "username": "root", "password": "root",
                       "device": "root.factory.d1",
                       "measurements": ["temperature"], "types": ["float"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "iotdb", "name": "mv02b-iot-act",
                       "connector": "mv02b-iot", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-iot-rule", "name": "mv02b-iot-rule",
                       "sql": "SELECT * FROM \"mv02b/iot\"", "actions": ["mv02b-iot-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/iot", "{\"temperature\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/rest/v2/insertTablet");
    assert!(
        Mv02bFake::body_text(&got[0]).contains("root.factory.d1"),
        "tablet: {}",
        Mv02bFake::body_text(&got[0])
    );
    // The tablet call carries Basic auth.
    assert!(
        got[0]
            .authorization
            .as_deref()
            .unwrap_or_default()
            .to_ascii_lowercase()
            .starts_with("basic "),
        "the tablet call carries Basic auth: {:?}",
        got[0].authorization
    );
    mv02b_wait_success(&server, &token, "mv02b-iot-act", 1).await;

    // The action override moves the device path.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-iot-act",
            json!({"type": "iotdb", "name": "mv02b-iot-act",
                       "connector": "mv02b-iot", "parameters": {"device": "root.factory.d2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/iot", "{\"temperature\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert!(
        Mv02bFake::body_text(&got[1]).contains("root.factory.d2"),
        "tablet: {}",
        Mv02bFake::body_text(&got[1])
    );
    mv02b_wait_success(&server, &token, "mv02b-iot-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-iot",
            json!({"type": "iotdb", "name": "mv02b-iot",
                       "endpoint": endpoint, "username": "root", "password": "root",
                       "measurements": ["temperature"], "types": ["float"]}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped device refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("device"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/iot", "{\"temperature\": 23.5}").await;
    let got = fake.wait_for(3).await;
    assert!(
        Mv02bFake::body_text(&got[2]).contains("root.factory.d2"),
        "tablet: {}",
        Mv02bFake::body_text(&got[2])
    );
    mv02b_wait_success(&server, &token, "mv02b-iot-act", 3).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/iot", "{\"temperature\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-iot-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-iot-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server
        .get_auth("/api/v5/connectors/mv02b-iot", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 200, "{\"code\":200}").await;
    mv02b_publish(&server, &token, "mv02b/iot", "{\"temperature\": 25.5}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).contains("root.factory.d2"),
        "tablet: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-iot-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_doris_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-doris");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{\"Status\":\"Success\",\"NumberLoadedRows\":1}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "doris", "name": "mv02b-doris",
                       "server": endpoint, "database": "db", "table": "events"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "doris", "name": "mv02b-doris-act",
                       "connector": "mv02b-doris", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-doris-rule", "name": "mv02b-doris-rule",
                       "sql": "SELECT * FROM \"mv02b/doris\"", "actions": ["mv02b-doris-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/doris", "{\"temp\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "PUT");
    assert_eq!(got[0].path, "/api/db/events/_stream_load");
    mv02b_wait_success(&server, &token, "mv02b-doris-act", 1).await;

    // The action override moves the table.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-doris-act",
            json!({"type": "doris", "name": "mv02b-doris-act",
                       "connector": "mv02b-doris", "parameters": {"table": "events2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/doris", "{\"temp\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert_eq!(got[1].path, "/api/db/events2/_stream_load");
    mv02b_wait_success(&server, &token, "mv02b-doris-act", 2).await;

    // A refused action update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/actions/mv02b-doris-act",
            json!({"type": "doris", "name": "mv02b-doris-act",
                       "connector": "mv02b-doris",
                       "parameters": {"table": "events2", "format": "csv"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a csv format refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("format"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/doris", "{\"temp\": 23.5}").await;
    let got = fake.wait_for(3).await;
    assert_eq!(got[2].path, "/api/db/events2/_stream_load");
    mv02b_wait_success(&server, &token, "mv02b-doris-act", 3).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/doris", "{\"temp\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-doris-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-doris-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server
        .get_auth("/api/v5/connectors/mv02b-doris", &token)
        .await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake =
        Mv02bFake::start_on(port, 200, "{\"Status\":\"Success\",\"NumberLoadedRows\":1}").await;
    mv02b_publish(&server, &token, "mv02b/doris", "{\"temp\": 25.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/api/db/events2/_stream_load");
    mv02b_wait_success(&server, &token, "mv02b-doris-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_datalayers_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-datalayers");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "datalayers", "name": "mv02b-dl",
                       "server": endpoint, "database": "db", "table": "events"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "datalayers", "name": "mv02b-dl-act",
                       "connector": "mv02b-dl", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-dl-rule", "name": "mv02b-dl-rule",
                       "sql": "SELECT * FROM \"mv02b/dl\"", "actions": ["mv02b-dl-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/dl", "{\"temp\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/write");
    assert!(
        got[0].query.contains("precision=us"),
        "query: {}",
        got[0].query
    );
    assert!(got[0].query.contains("db=db"), "query: {}", got[0].query);
    assert!(
        Mv02bFake::body_text(&got[0]).starts_with("events,"),
        "line: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-dl-act", 1).await;

    // The action override moves the table.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-dl-act",
            json!({"type": "datalayers", "name": "mv02b-dl-act",
                       "connector": "mv02b-dl", "parameters": {"table": "events2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/dl", "{\"temp\": 22.5}").await;
    let got = fake.wait_for(2).await;
    assert!(
        Mv02bFake::body_text(&got[1]).starts_with("events2,"),
        "line: {}",
        Mv02bFake::body_text(&got[1])
    );
    mv02b_wait_success(&server, &token, "mv02b-dl-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-dl",
            json!({"type": "datalayers", "name": "mv02b-dl",
                       "server": endpoint, "table": "events2"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped database refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("database"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/dl", "{\"temp\": 23.5}").await;
    let got = fake.wait_for(3).await;
    assert!(
        Mv02bFake::body_text(&got[2]).starts_with("events2,"),
        "line: {}",
        Mv02bFake::body_text(&got[2])
    );
    mv02b_wait_success(&server, &token, "mv02b-dl-act", 3).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/dl", "{\"temp\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-dl-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-dl-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server.get_auth("/api/v5/connectors/mv02b-dl", &token).await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 200, "").await;
    mv02b_publish(&server, &token, "mv02b/dl", "{\"temp\": 25.5}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).starts_with("events2,"),
        "line: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02b-dl-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02b_timestream_delivers_updates_and_recovers_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-timestream");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "timestream", "name": "mv02b-ts",
                       "database": "db", "table": "metrics", "region": "us-east-1",
                       "endpoint": endpoint, "precision": "us",
                       "access_key_id": "k", "secret_access_key": "s",
                       "measures": {"temperature": "DOUBLE"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "aws_timestream", "name": "mv02b-ts-act",
                       "connector": "mv02b-ts", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201, "the aws_timestream alias creates");
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02b-ts-rule", "name": "mv02b-ts-rule",
                       "sql": "SELECT * FROM \"mv02b/ts\"", "actions": ["mv02b-ts-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02b/ts", "{\"temperature\": 21.5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert_eq!(got[0].path, "/");
    let write: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[0])).expect("write is JSON");
    assert_eq!(write["DatabaseName"], json!("db"), "write: {write}");
    assert_eq!(write["TableName"], json!("metrics"), "write: {write}");
    let records = write["Records"].as_array().expect("records: {write}");
    assert_eq!(records.len(), 1, "write: {write}");
    assert_eq!(
        records[0]["Dimensions"],
        json!([{"Name": "topic", "Value": "mv02b/ts"}]),
        "write: {write}"
    );
    assert_eq!(
        records[0]["MeasureName"],
        json!("metrics"),
        "write: {write}"
    );
    assert_eq!(
        records[0]["MeasureValueType"],
        json!("MULTI"),
        "write: {write}"
    );
    assert_eq!(
        records[0]["MeasureValues"],
        json!([{"Name": "temperature", "Value": "21.5", "Type": "DOUBLE"}]),
        "write: {write}"
    );
    assert_eq!(
        records[0]["TimeUnit"],
        json!("MICROSECONDS"),
        "write: {write}"
    );
    assert!(
        records[0]["Time"]
            .as_str()
            .is_some_and(|stamp| !stamp.is_empty() && stamp.bytes().all(|b| b.is_ascii_digit())),
        "write: {write}"
    );
    mv02b_wait_success(&server, &token, "mv02b-ts-act", 1).await;

    // The action override moves the table.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02b-ts-act",
            json!({"type": "aws_timestream", "name": "mv02b-ts-act",
                       "connector": "mv02b-ts",
                       "parameters": {"table": "metrics2", "precision": "ns"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02b/ts", "{\"temperature\": 22.5}").await;
    let got = fake.wait_for(2).await;
    let write: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[1])).expect("write is JSON");
    assert_eq!(write["TableName"], json!("metrics2"), "write: {write}");
    assert_eq!(write["Records"][0]["TimeUnit"], json!("NANOSECONDS"));
    mv02b_wait_success(&server, &token, "mv02b-ts-act", 2).await;

    // A refused connector update keeps the old delivery.
    let (status, body) = server
        .put_auth(
            "/api/v5/connectors/mv02b-ts",
            json!({"type": "timestream", "name": "mv02b-ts",
                       "table": "metrics2", "region": "us-east-1",
                       "endpoint": endpoint,
                       "access_key_id": "k", "secret_access_key": "s",
                       "measures": {"temperature": "DOUBLE"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a dropped database refuses: {body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("database"),
        "{body}"
    );
    mv02b_publish(&server, &token, "mv02b/ts", "{\"temperature\": 23.5}").await;
    let got = fake.wait_for(3).await;
    let write: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[2])).expect("write is JSON");
    assert_eq!(write["TableName"], json!("metrics2"), "write: {write}");
    assert_eq!(write["Records"][0]["TimeUnit"], json!("NANOSECONDS"));
    mv02b_wait_success(&server, &token, "mv02b-ts-act", 3).await;

    // The server dies: the forward counts one failure.
    drop(fake);
    mv02b_publish(&server, &token, "mv02b/ts", "{\"temperature\": 24.5}").await;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02b-ts-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        if metrics["metrics"]["failed"].as_u64().unwrap_or(0) >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let (status, metrics) = server
        .get_auth("/api/v5/actions/mv02b-ts-act/metrics", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(metrics["metrics"]["failed"], json!(1));
    drop(server);
    drop(registry);

    // A restart stores the outage: the entry reads disconnected.
    let reloaded = Arc::new(ConfigRegistry::load(&dir).expect("reload data dir"));
    let (server, _state) = TestServer::start_with_registry(reloaded).await;
    let (status, body) = server
        .post(
            "/api/v5/login",
            json!({"username": "admin", "password": "Adm1n-test-pass!"}),
        )
        .await;
    assert_eq!(status, 200);
    let token = body["token"].as_str().expect("admin token").to_string();
    let (status, read) = server.get_auth("/api/v5/connectors/mv02b-ts", &token).await;
    assert_eq!(status, 200, "stored entry survives the restart");
    assert_eq!(read["status"], json!("disconnected"));

    // The server returns on the same port: delivery resumes.
    let fake = Mv02bFake::start_on(port, 200, "{}").await;
    mv02b_publish(&server, &token, "mv02b/ts", "{\"temperature\": 25.5}").await;
    let got = fake.wait_for(1).await;
    let write: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[0])).expect("write is JSON");
    assert_eq!(write["TableName"], json!("metrics2"), "write: {write}");
    assert_eq!(write["Records"][0]["TimeUnit"], json!("NANOSECONDS"));
    mv02b_wait_success(&server, &token, "mv02b-ts-act", 1).await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

/// Cassandra runs on the maintained driver, which needs a real
/// server for its topology handshake: the loopback fake cannot
/// prove delivery. This test proves the broker path instead (create,
/// update, refused update, restart, failed-forward counting); the
/// ignored qualification run proves delivery against a real server.
#[cfg(feature = "cassandra")]
#[tokio::test]
async fn mv02b_cassandra_counts_failures_and_keeps_state_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02b-cassandra");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // Scylla stays pending (SB-27). It never creates.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "scylla", "name": "mv02b-cass",
                       "servers": "127.0.0.1:9", "keyspace": "telemetry", "table": "events"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "pending scylla refuses: {body}");
    std::fs::remove_dir_all(&dir).ok();
}
