use super::*;

#[cfg(feature = "s3")]
#[tokio::test]
async fn mv02d_s3_action_delivers_updates_and_counts_a_dead_server_through_the_broker() {
    let dir = unique_data_dir("mv02d-s3");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");

    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "s3", "name": "mv02d-s3",
                       "endpoint": endpoint, "region": "us-east-1",
                       "access_key_id": "AKID", "secret_access_key": "secret"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "s3", "name": "mv02d-s3-act",
                       "connector": "mv02d-s3",
                       "parameters": {"bucket": "telemetry",
                                      "key": "events/sb09/${topic}.ndjson"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-s3-rule", "name": "mv02d-s3-rule",
                       "sql": "SELECT * FROM \"mv02d/ch\"", "actions": ["mv02d-s3-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);

    mv02b_publish(&server, &token, "mv02d/ch", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "PUT");
    assert_eq!(got[0].path, "/telemetry/events/sb09/mv02d/ch.ndjson");
    assert_eq!(
        got[0].content_type.as_deref(),
        Some("application/x-ndjson"),
        "the object upload carries the driver content type"
    );
    assert!(
        Mv02bFake::body_text(&got[0]).contains("\"v\":1"),
        "the projected event body must travel: {}",
        Mv02bFake::body_text(&got[0])
    );
    mv02b_wait_success(&server, &token, "mv02d-s3-act", 1).await;

    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-s3-act",
            json!({"type": "s3", "name": "mv02d-s3-act",
                       "connector": "mv02d-s3",
                       "parameters": {"bucket": "telemetry",
                                      "key": "events/sb09/updated/${topic}.ndjson"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02d/ch", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert_eq!(
        got[1].path,
        "/telemetry/events/sb09/updated/mv02d/ch.ndjson"
    );
    mv02b_wait_success(&server, &token, "mv02d-s3-act", 2).await;

    drop(fake);
    mv02b_publish(&server, &token, "mv02d/ch", "{\"v\":3}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02d-s3-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead object store counts one failed forward");
}

#[cfg(feature = "azure_blob")]
#[tokio::test]
async fn mv02d_azure_blob_delivers_updates_and_recovers_through_the_broker() {
    let dir = unique_data_dir("mv02d-azure");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "azure_blob", "name": "mv02d-azure",
                       "account_name": "mydeviceblobs", "account_key": "YUtJRA==",
                       "endpoint": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "azure_blob", "name": "mv02d-azure-act",
                       "connector": "mv02d-azure",
                       "parameters": {"container": "telemetry",
                                      "blob": "events/sb09/${topic}.json"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-azure-rule", "name": "mv02d-azure-rule",
                       "sql": "SELECT * FROM \"mv02d/azure\"", "actions": ["mv02d-azure-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02d/azure", "{\"v\":1}").await;
    let got = fake.wait_for(2).await;
    let blob_put = got
        .iter()
        .find(|r| r.method == "PUT" && r.path.contains("/telemetry/"));
    let blob_put = blob_put.expect("one blob PUT reaches the fake");
    assert_eq!(blob_put.path, "/telemetry/events/sb09/mv02d/azure.json");
    assert!(
        Mv02bFake::body_text(blob_put).contains("\"v\":1"),
        "the projected event body must travel"
    );
    assert_eq!(
        blob_put.content_type.as_deref(),
        Some("application/x-ndjson"),
        "the blob PUT carries the driver content type"
    );
    mv02b_wait_success(&server, &token, "mv02d-azure-act", 1).await;
    // A forbidden override is refused.
    let (status, body) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "azure_blob", "name": "mv02d-azure-bad",
                       "connector": "mv02d-azure",
                       "parameters": {"container": "telemetry",
                                      "blob": "events/sb09/${topic}.json",
                                      "acl": "private"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "a forbidden acl refuses: {body}");
    // An update moves the blob path.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-azure-act",
            json!({"type": "azure_blob", "name": "mv02d-azure-act",
                       "connector": "mv02d-azure",
                       "parameters": {"container": "telemetry",
                                      "blob": "events/sb09/updated/${topic}.json"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02d/azure", "{\"v\":2}").await;
    let got = fake.wait_for(4).await;
    assert!(
        got.iter()
            .any(|r| r.path == "/telemetry/events/sb09/updated/mv02d/azure.json"),
        "the updated blob path must travel: {:?}",
        got.iter().map(|r| &r.path).collect::<Vec<_>>()
    );
    // A refused update keeps the old delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-azure-act",
            json!({"type": "azure_blob", "name": "mv02d-azure-act",
                       "connector": "mv02d-azure",
                       "parameters": {"container": "telemetry"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    mv02b_publish(&server, &token, "mv02d/azure", "{\"v\":3}").await;
    let got = fake.wait_for(6).await;
    assert!(
        got.iter().any(
            |r| r.path == "/telemetry/events/sb09/updated/mv02d/azure.json"
                && Mv02bFake::body_text(r).contains("\"v\":3")
        ),
        "the old delivery continues after a refused update"
    );
    drop(fake);
    mv02b_publish(&server, &token, "mv02d/azure", "{\"v\":4}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02d-azure-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead blob store counts one failed forward");
    let fake = Mv02bFake::start_on(port, 200, "").await;
    // Backoff blocks sends for 2 s after the failure. Wait past it.
    // Then republish. Keep the backoff check.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    mv02b_publish(&server, &token, "mv02d/azure", "{\"v\":5}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got.iter()
            .any(|r| r.path == "/telemetry/events/sb09/updated/mv02d/azure.json"),
        "delivery resumes after the server restarts"
    );
    mv02b_wait_success(&server, &token, "mv02d-azure-act", 4).await;
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "dynamodb")]
#[tokio::test]
async fn mv02d_dynamodb_delivers_updates_and_recovers_through_the_broker() {
    let dir = unique_data_dir("mv02d-dynamo");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{\"UnprocessedItems\":{}}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "dynamodb", "name": "mv02d-ddb",
                       "url": endpoint, "region": "us-east-1",
                       "aws_access_key_id": "AKID", "aws_secret_access_key": "secret"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "dynamodb", "name": "mv02d-ddb-act",
                       "connector": "mv02d-ddb",
                       "parameters": {"table": "events", "hash_key": "id",
                                      "undefined_vars_as_null": true,
                                      "template": "{\"id\":\"${payload.id}\",\"temp\":\"${payload.temp}\"}"}}),
                &token,
            )
            .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-ddb-rule", "name": "mv02d-ddb-rule",
                       "sql": "SELECT * FROM \"mv02d/ddb\"", "actions": ["mv02d-ddb-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(
        &server,
        &token,
        "mv02d/ddb",
        "{\"id\":\"edge-7\",\"temp\":21}",
    )
    .await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert_eq!(
        got[0].content_type.as_deref(),
        Some("application/x-amz-json-1.0"),
        "the BatchWriteItem call carries the driver content type"
    );
    let body = Mv02bFake::body_text(&got[0]);
    assert!(
        body.contains("\"events\""),
        "the table name must travel: {body}"
    );
    assert!(
        body.contains("edge-7"),
        "the hash key value must travel: {body}"
    );
    assert!(body.contains("21"), "the temp value must travel: {body}");
    mv02b_wait_success(&server, &token, "mv02d-ddb-act", 1).await;
    // An undefined variable becomes null when the flag is set.
    mv02b_publish(&server, &token, "mv02d/ddb", "{\"id\":\"edge-8\"}").await;
    let got = fake.wait_for(2).await;
    assert!(
        Mv02bFake::body_text(&got[1]).contains("\"NULL\""),
        "an undefined variable must write null: {}",
        Mv02bFake::body_text(&got[1])
    );
    // An update moves the table.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-ddb-act",
            json!({"type": "dynamodb", "name": "mv02d-ddb-act",
                       "connector": "mv02d-ddb",
                       "parameters": {"table": "events2", "hash_key": "id",
                                      "template": "{\"id\":\"${payload.id}\"}"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02d/ddb", "{\"id\":\"edge-9\"}").await;
    let got = fake.wait_for(3).await;
    assert!(
        Mv02bFake::body_text(&got[2]).contains("\"events2\""),
        "the updated table must travel"
    );
    // A refused update keeps the old delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-ddb-act",
            json!({"type": "dynamodb", "name": "mv02d-ddb-act",
                       "connector": "mv02d-ddb",
                       "parameters": {"hash_key": "id"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    mv02b_publish(&server, &token, "mv02d/ddb", "{\"id\":\"edge-10\"}").await;
    let got = fake.wait_for(4).await;
    assert!(
        Mv02bFake::body_text(&got[3]).contains("\"events2\""),
        "the old delivery continues after a refused update"
    );
    drop(fake);
    mv02b_publish(&server, &token, "mv02d/ddb", "{\"id\":\"edge-11\"}").await;
    mv02b_publish(&server, &token, "mv02d/ddb", "{\"id\":\"edge-12\"}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02d-ddb-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 2 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 2, "two dead writes count two failed forwards");
    let fake = Mv02bFake::start_on(port, 200, "{\"UnprocessedItems\":{}}").await;
    // Backoff blocks sends for 2 s after the failure. Wait past it.
    // Then republish. Keep the backoff check.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    mv02b_publish(&server, &token, "mv02d/ddb", "{\"id\":\"edge-13\"}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).contains("edge-13"),
        "delivery resumes after the server restarts"
    );
    mv02b_wait_success(&server, &token, "mv02d-ddb-act", 5).await;
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "bigquery")]
#[tokio::test]
async fn mv02d_bigquery_delivers_updates_and_recovers_through_the_broker() {
    let dir = unique_data_dir("mv02d-bq");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "bigquery", "name": "mv02d-bq",
                       "authentication": {"type": "attached_service_account", "gcp_project_id": "p",
                                          "access_token": "test-attached-token"},
                       "endpoint": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "bigquery", "name": "mv02d-bq-act",
                       "connector": "mv02d-bq",
                       "parameters": {"dataset": "d", "table": "events"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-bq-rule", "name": "mv02d-bq-rule",
                       "sql": "SELECT * FROM \"mv02d/bq\"", "actions": ["mv02d-bq-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02d/bq", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert!(
        got[0]
            .path
            .contains("/projects/p/datasets/d/tables/events/insertAll"),
        "the dataset and table must travel: {}",
        got[0].path
    );
    assert_eq!(
        got[0].content_type.as_deref(),
        Some("application/json"),
        "the insertAll call carries JSON"
    );
    assert!(
        got[0]
            .authorization
            .as_deref()
            .unwrap_or_default()
            .contains("test-attached-token"),
        "the attached token must travel: {:?}",
        got[0].authorization
    );
    let inserted: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[0])).expect("insertAll is JSON");
    assert_eq!(
        inserted["rows"][0]["json"]["v"],
        serde_json::json!(1),
        "the published row value must travel: {inserted}"
    );
    mv02b_wait_success(&server, &token, "mv02d-bq-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-bq-act",
            json!({"type": "bigquery", "name": "mv02d-bq-act",
                       "connector": "mv02d-bq",
                       "parameters": {"dataset": "d", "table": "events2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02d/bq", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got[1].path.contains("/tables/events2/insertAll"),
        "the updated table must travel: {}",
        got[1].path
    );
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-bq-act",
            json!({"type": "bigquery", "name": "mv02d-bq-act",
                       "connector": "mv02d-bq",
                       "parameters": {"dataset": "d"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    mv02b_publish(&server, &token, "mv02d/bq", "{\"v\":3}").await;
    let got = fake.wait_for(3).await;
    assert!(
        got[2].path.contains("/tables/events2/insertAll"),
        "the old delivery continues after a refused update"
    );
    drop(fake);
    mv02b_publish(&server, &token, "mv02d/bq", "{\"v\":4}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02d-bq-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead insert counts one failed forward");
    let fake = Mv02bFake::start_on(port, 200, "{}").await;
    // Backoff blocks sends for 2 s after the failure. Wait past it.
    // Then republish. Keep the backoff check.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    mv02b_publish(&server, &token, "mv02d/bq", "{\"v\":5}").await;
    let got = fake.wait_for(1).await;
    assert!(
        got[0].path.contains("/tables/events2/insertAll"),
        "delivery resumes after the server restarts"
    );
    mv02b_wait_success(&server, &token, "mv02d-bq-act", 4).await;
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02d_snowflake_delivers_updates_and_recovers_through_the_broker() {
    let dir = unique_data_dir("mv02d-sf");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    #[cfg(test)]
    const SNOWFLAKE_TEST_KEY: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCwQ2w63oB3FtHg\n7xysQK8MuX9S0WkbAVlxWpLHDNIdRVxA9Ra2gFFpKy8jX45UMSow6Yny7IvYWFzZ\nL4y9yoFiqu+LxhlJHIO6JO8+ZmeBoNwuDiIzgesbZwjyQiQ2M7p/4c18a2ffGPWF\nBETT7uVwVKJ3hTp97RN7Mc1/eFMimuT/TC11I+sFCZUHgrbhEG3L5Gg3RJ2MKbcX\nGEIxjFDJdLJ9RK0BopD6lxR1a4zeYr+iF/m3+JeJPAaS15yMD+sB1g5C7XZ1OIsB\nNBBnHWHpNhYO2IrCc9lZeSSzSkbRC6k1oqvTurFRzHWZqBKQGYnH8BftubIPSTBg\nU/BM4rR3AgMBAAECggEAHSRwmwUZoVb1CWcPSw2Aw65RtkwoQA5Hjv3GIcHlZXCH\n0beT80Wg8C3zI7qTSik8zAx4weDJOFJXu5LohqKaJMmVRHtSx+s+fkLICX2d5GlH\nrhepIPH8gLHW4VL9MLb5wVYAhu8tI845Ha54gL/RUHK1z+QHqTVO0MIJs2cd+6zx\nKsAtnqEQJMFpl1D0y0uutuboK4soHJMyRyrHBNWdgfzmTrCsngzu2zVM4aZh/gQY\nHcQgJ1rK6Wnen/GGPrNluwWU+bfLdlWO2qiXXwGLfhyx2H6cuROGdoU607BFJNpM\nkAudvEuLa0fOi1ym6lJ5pcJ6pSLkbeveW6+thkO2fQKBgQDXc2GiKx15vQHmdDmZ\nUJEiPJ+hSry5fjaowzrfgqJHyeNfUjnM/E9WlNn2AuxKDWGc3UNEr6jB9V7leKev\nQaPB2LAgXt0YVHmyim51/gTDguE9TOTGWqL4npZG9Nqh8xMxWt08ULvknkOQQOso\nzCoZQYlG4BHegAG7n0/5IN7HdQKBgQDRb/VbJ9iE0wtY/A3e3eWPbGfTF7AZREUu\n/mt94tFEWDDvedX1EPi4DJgPMqQ4eHnBZb3+G7jPcRdm6/KQzR5QiRMHSylfIQRH\nLqqfHBzZDDSZINLW1FMReC9xGfkRoG0Tlt2iQzXOy90+uE/9k5BGSbQNakfVDXJs\n3JAHDMy6uwKBgQCaazxC+xv5MRq3jf3qgPBE1aaj9+kkGe4bLzJ3GC4vveeVXl3H\nKd/DcpR12sp4mPapc3zPMgeGXNNTLRMiba1tNl2mFdfppEJFUSqyrwnDB39gbEhc\nUoIUJ7YVzVEWWh4bdcCzhjnlNfm+3oitiQdzaqF1hwvHqX+Udi7fpEuIMQKBgQC5\nu0bkQu7Rw/MRQ93tIe19ho6AdkZV8eREq52Z8vbQXEFxbiOfBCD93zVObQOTjMu1\nBcw6uEzpsgol3OKtJSpYE2eLlU0oLriDg9AN8DlpBljy31f66iqMmH/CFl16E0II\nGEeOqXnjXYlkIMHXR/CvVJdXOkRfnWA3SFZ12hUJFwKBgD8JlGTyrVfNsNMOaTDV\nNopoYnUQ6ljFmJi6TGmnkliCRXPuqBl+2hVxiKeWI2MprJ5Ya8qLbL6M56uCwAD2\nqEhvjEuatma5rJyE5NULOjAXA5tLw9qM1M9j1FNOaXnFC9/Yii2a49R8zu05wRB2\nH+dMMSDXQ4EHHYcKIFJjDbxn\n-----END PRIVATE KEY-----\n";
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "snowflake", "name": "mv02d-sf",
                       "account": "acct", "pipe_user": "user",
                       "private_key": SNOWFLAKE_TEST_KEY,
                       "server": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "snowflake", "name": "mv02d-sf-act",
                       "connector": "mv02d-sf",
                       "parameters": {"database": "db", "schema": "PUBLIC", "pipe": "events"}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-sf-rule", "name": "mv02d-sf-rule",
                       "sql": "SELECT * FROM \"mv02d/sf\"", "actions": ["mv02d-sf-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02d/sf", "{\"v\":1}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert!(
        got[0].path.contains("/v1/data/streaming/channels/") && got[0].path.ends_with("/rows"),
        "the pipe channel rows path must travel: {}",
        got[0].path
    );
    assert_eq!(
        got[0].content_type.as_deref(),
        Some("application/json"),
        "the rows call carries JSON"
    );
    assert!(
        got[0]
            .authorization
            .as_deref()
            .unwrap_or_default()
            .starts_with("Bearer "),
        "the RS256 token must ride the wire: {:?}",
        got[0].authorization
    );
    let rows: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[0])).expect("rows is JSON");
    assert_eq!(
        rows[0]["V"],
        serde_json::json!(1),
        "the published row value must travel: {rows}"
    );
    mv02b_wait_success(&server, &token, "mv02d-sf-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-sf-act",
            json!({"type": "snowflake", "name": "mv02d-sf-act",
                       "connector": "mv02d-sf",
                       "parameters": {"database": "db", "schema": "PUBLIC", "pipe": "events2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02d/sf", "{\"v\":2}").await;
    let got = fake.wait_for(2).await;
    assert!(
        got[1].path.contains("events2"),
        "the updated pipe must travel in the channel path: {}",
        got[1].path
    );
    let rows: serde_json::Value =
        serde_json::from_str(&Mv02bFake::body_text(&got[1])).expect("rows is JSON");
    assert_eq!(
        rows[0]["V"],
        serde_json::json!(2),
        "the updated row value must travel: {rows}"
    );
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-sf-act",
            json!({"type": "snowflake", "name": "mv02d-sf-act",
                       "connector": "mv02d-sf",
                       "parameters": {"database": "db", "schema": "PUBLIC"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    mv02b_publish(&server, &token, "mv02d/sf", "{\"v\":3}").await;
    let got = fake.wait_for(3).await;
    assert_eq!(
        got.len(),
        3,
        "the old delivery continues after a refused update"
    );
    drop(fake);
    mv02b_publish(&server, &token, "mv02d/sf", "{\"v\":4}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02d-sf-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead pipe counts one failed forward");
    let fake = Mv02bFake::start_on(port, 200, "{}").await;
    // Backoff blocks sends for 2 s after the failure. Wait past it.
    // Then republish. Keep the backoff check.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    mv02b_publish(&server, &token, "mv02d/sf", "{\"v\":5}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got.len(), 1, "delivery resumes after the server restarts");
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02d_tablestore_delivers_updates_and_recovers_through_the_broker() {
    let dir = unique_data_dir("mv02d-ots");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{\"tables\":[{\"rows\":[{\"is_ok\":true}]}]}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "tablestore", "name": "mv02d-ots",
                       "endpoint": endpoint, "instance_name": "i",
                       "access_key_id": "ak", "access_key_secret": "sk"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "tablestore", "name": "mv02d-ots-act",
                       "connector": "mv02d-ots",
                       "parameters": {"table_name": "events", "measurement": "telemetry",
                                      "tags": {"device": "${client_id}"},
                                      "fields": [{"column": "temp", "value": "${payload.temp}"}]}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-ots-rule", "name": "mv02d-ots-rule",
                       "sql": "SELECT * FROM \"mv02d/ots\"", "actions": ["mv02d-ots-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02d/ots", "{\"temp\":21}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].method, "POST");
    assert_eq!(
        got[0].path, "/api/BatchWriteRow",
        "the BatchWriteRow path must travel"
    );
    assert_eq!(
        got[0].content_type.as_deref(),
        Some("application/json"),
        "the BatchWriteRow call carries JSON"
    );
    let body = Mv02bFake::body_text(&got[0]);
    assert!(
        body.contains("\"events\""),
        "the table name must travel: {body}"
    );
    assert!(
        body.contains("telemetry"),
        "the measurement must travel: {body}"
    );
    assert!(
        body.contains("temp"),
        "the field column must travel: {body}"
    );
    assert!(body.contains("21"), "the field value must travel: {body}");
    mv02b_wait_success(&server, &token, "mv02d-ots-act", 1).await;
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-ots-act",
            json!({"type": "tablestore", "name": "mv02d-ots-act",
                       "connector": "mv02d-ots",
                       "parameters": {"table_name": "events2", "measurement": "telemetry",
                                      "tags": {"device": "${client_id}"},
                                      "fields": [{"column": "temp", "value": "${payload.temp}"}]}}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02d/ots", "{\"temp\":22}").await;
    let got = fake.wait_for(2).await;
    assert!(
        Mv02bFake::body_text(&got[1]).contains("\"events2\""),
        "the updated table must travel"
    );
    let (status, _) = server
        .put_auth(
            "/api/v5/actions/mv02d-ots-act",
            json!({"type": "tablestore", "name": "mv02d-ots-act",
                       "connector": "mv02d-ots",
                       "parameters": {"table_name": "events2"}}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    mv02b_publish(&server, &token, "mv02d/ots", "{\"temp\":23}").await;
    let got = fake.wait_for(3).await;
    assert!(
        Mv02bFake::body_text(&got[2]).contains("\"events2\""),
        "the old delivery continues after a refused update"
    );
    drop(fake);
    mv02b_publish(&server, &token, "mv02d/ots", "{\"temp\":24}").await;
    let mut failed = 0;
    for _ in 0..100 {
        let (status, metrics) = server
            .get_auth("/api/v5/actions/mv02d-ots-act/metrics", &token)
            .await;
        assert_eq!(status, 200);
        failed = metrics["metrics"]["failed"].as_u64().unwrap_or(0);
        if failed >= 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(failed >= 1, "a dead table counts one failed forward");
    let fake = Mv02bFake::start_on(
        port,
        200,
        "{\"tables\":[{\"rows\":[{\"is_ok\":true},{\"is_ok\":true}]}]}",
    )
    .await;
    // Backoff blocks sends for 2 s after the failure. Wait past it.
    // Then republish. Keep the backoff check.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    mv02b_publish(&server, &token, "mv02d/ots", "{\"temp\":25}").await;
    let got = fake.wait_for(1).await;
    assert!(
        Mv02bFake::body_text(&got[0]).contains("\"events2\""),
        "delivery resumes after the server restarts"
    );
    mv02b_wait_success(&server, &token, "mv02d-ots-act", 4).await;
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mv02d_tablestore_timestamp_pk_delivers_through_the_broker() {
    let dir = unique_data_dir("mv02d-ots-ts");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let fake = Mv02bFake::start(200, "{\"tables\":[{\"rows\":[{\"is_ok\":true}]}]}").await;
    let port = fake.port;
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let endpoint = format!("http://127.0.0.1:{port}");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "tablestore", "name": "mv02d-ots-ts",
                       "endpoint": endpoint, "instance_name": "i",
                       "access_key_id": "ak", "access_key_secret": "sk"}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "tablestore", "name": "mv02d-ots-ts-act",
                       "connector": "mv02d-ots-ts",
                       "parameters": {"table_name": "events", "measurement": "telemetry",
                                      "timestamp": "${timestamp}",
                                      "fields": [{"column": "temp", "value": "${payload.temp}"}]}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-ots-ts-rule", "name": "mv02d-ots-ts-rule",
                       "sql": "SELECT * FROM \"mv02d/ots-ts\"", "actions": ["mv02d-ots-ts-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    mv02b_publish(&server, &token, "mv02d/ots-ts", "{\"temp\":21}").await;
    let got = fake.wait_for(1).await;
    assert_eq!(got[0].path, "/api/BatchWriteRow");
    let body = Mv02bFake::body_text(&got[0]);
    assert!(body.contains("\"events\""), "the table must travel: {body}");
    assert!(
        body.contains("telemetry"),
        "the measurement must travel: {body}"
    );
    assert!(
        body.contains("timestamp"),
        "the timestamp key must travel: {body}"
    );
    mv02b_wait_success(&server, &token, "mv02d-ots-ts-act", 1).await;
    drop(fake);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(test)]
async fn mv02d_disk_log_retention_arms(
    server: &TestServer,
    token: &str,
    dir: &std::path::Path,
    unique: &str,
) {
    // Both retention values deliver through the broker entry.
    // The arms share one file and one payload. Only the
    // retention period changes. The driver test proves pruning.
    // Four publishes are the least that keep one backup segment.
    // Each raw record holds 13 bytes. Three records hold 39 bytes.
    // The fourth write overflows the 40-byte limit and rotates.
    // The file and backup asserts stay.
    let retpath = format!("target/{unique}.log");
    let payload = "{\"v\":\"same\"}";
    // Clear the earlier phase. It used the same file path.
    std::fs::remove_file(dir.join(&retpath)).ok();
    if let Ok(entries) = std::fs::read_dir(dir.join("target")) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&format!("{unique}.log.")) {
                std::fs::remove_file(entry.path()).ok();
            }
        }
    }
    let mut arm_backups = Vec::new();
    for (suffix, retention) in [("ret1", json!(1)), ("retinf", json!("infinity"))] {
        let conn = format!("mv02d-disk-{suffix}");
        let act = format!("mv02d-disk-act-{suffix}");
        let rule = format!("mv02d-disk-rule-{suffix}");
        let topic = format!("mv02d/disk-{suffix}");
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "disk_log", "name": conn.clone(),
                           "filepath": retpath.clone(),
                           "max_file_size": "40B",
                           "max_file_number": 10,
                           "rotation": {"period": "day", "retention_period": retention.clone()}}),
                token,
            )
            .await;
        assert_eq!(status, 201, "retention arm {suffix} creates");
        let (status, _) = server
            .post_auth(
                "/api/v5/actions",
                json!({"type": "disk_log", "name": act.clone(),
                           "connector": conn.clone(), "parameters": {}}),
                token,
            )
            .await;
        assert_eq!(status, 201);
        let (status, _) = server
            .post_auth(
                "/api/v5/rules",
                json!({"id": rule.clone(), "name": rule.clone(),
                           "sql": format!("SELECT * FROM \"{topic}\""),
                           "actions": [act.clone()]}),
                token,
            )
            .await;
        assert_eq!(status, 201);
        for _ in 0..4 {
            mv02b_publish(server, token, topic.as_str(), payload).await;
        }
        mv02b_wait_success(server, token, act.as_str(), 4).await;
        let text = std::fs::read_to_string(dir.join(&retpath)).unwrap_or_default();
        assert!(
            text.contains(payload),
            "the {suffix} file must hold its record"
        );
        // Fresh backup segments survive under both retention values.
        // Old-segment pruning needs synthetic time. The driver test
        // proves the pruning difference with exact timestamps.
        let active_name = format!("{unique}.log");
        let mut backups = 0;
        if let Ok(entries) = std::fs::read_dir(dir.join("target")) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with(&format!("{active_name}.")) {
                    backups += 1;
                }
            }
        }
        assert!(backups > 0, "the {suffix} arm must keep one backup segment");
        arm_backups.push(backups);
        // Clean the shared file before the next arm runs.
        std::fs::remove_file(dir.join(&retpath)).ok();
        if let Ok(entries) = std::fs::read_dir(dir.join("target")) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with(&format!("{active_name}.")) {
                    std::fs::remove_file(entry.path()).ok();
                }
            }
        }
    }
    assert_eq!(
        arm_backups.len(),
        2,
        "both retention arms run through the broker"
    );
    assert_eq!(
        arm_backups[0], arm_backups[1],
        "fresh backups survive under both retention values"
    );
}

#[tokio::test]
async fn mv02d_disk_log_writes_reads_and_rotates_through_the_broker() {
    let dir = unique_data_dir("mv02d-disk");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;
    let unique = format!(
        "mv02d-disk-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let filepath = format!("target/{unique}.log");
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors",
            json!({"type": "disk_log", "name": "mv02d-disk",
                       "filepath": filepath,
                       "max_file_size": "40B",
                       "max_file_number": 3}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/actions",
            json!({"type": "disk_log", "name": "mv02d-disk-act",
                       "connector": "mv02d-disk", "parameters": {}}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    let (status, _) = server
        .post_auth(
            "/api/v5/rules",
            json!({"id": "mv02d-disk-rule", "name": "mv02d-disk-rule",
                       "sql": "SELECT * FROM \"mv02d/disk\"", "actions": ["mv02d-disk-act"]}),
            &token,
        )
        .await;
    assert_eq!(status, 201);
    for index in 0..10 {
        mv02b_publish(&server, &token, "mv02d/disk", &format!("{{\"v\":{index}}}")).await;
    }
    mv02b_wait_success(&server, &token, "mv02d-disk-act", 10).await;
    let active = dir.join(&filepath);
    assert!(active.exists(), "the disk log file must exist");
    let parent = active.parent().expect("parent");
    let prefix = active
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or(unique.as_str());
    let mut backup_paths = Vec::new();
    if let Ok(entries) = std::fs::read_dir(parent) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(prefix)
                && name != active.file_name().unwrap().to_string_lossy().as_ref()
            {
                backup_paths.push(entry.path());
            }
        }
    }
    assert!(
        !backup_paths.is_empty(),
        "rotation must keep at least one backup segment"
    );
    // Each Raw record holds 8 bytes. Ten records hold 80 bytes.
    // A 40-byte limit forces one rotation. The backup keeps
    // the first records. The active file keeps the last records.
    let mut backup_text = String::new();
    for path in &backup_paths {
        backup_text.push_str(&std::fs::read_to_string(path).unwrap_or_default());
    }
    assert!(
        backup_text.contains("\"v\":0"),
        "the backup must hold the first record"
    );
    let text = std::fs::read_to_string(&active).unwrap_or_default();
    assert!(
        text.contains("\"v\":9"),
        "the active file must hold the last record"
    );
    // A symlink refusal is enforced through the broker.
    let linkpath = format!("target/{unique}-link.log");
    #[cfg(unix)]
    {
        let link_abs = dir.join(&linkpath);
        if let Some(parent) = link_abs.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        let _ = std::fs::remove_file(&link_abs);
        std::os::unix::fs::symlink(&active, &link_abs).expect("symlink");
        let (status, _) = server
            .post_auth(
                "/api/v5/connectors",
                json!({"type": "disk_log", "name": "mv02d-disk-link",
                           "filepath": linkpath,
                           "max_file_size": "1MB",
                           "max_file_number": 3}),
                &token,
            )
            .await;
        assert_eq!(status, 400, "a symlink path must refuse");
        std::fs::remove_file(dir.join(&linkpath)).ok();
    }
    // An update moves the file.
    let filepath2 = format!("target/{unique}-2.log");
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv02d-disk",
            json!({"type": "disk_log", "name": "mv02d-disk",
                       "filepath": filepath2,
                       "max_file_size": "100B",
                       "max_file_number": 3}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    mv02b_publish(&server, &token, "mv02d/disk", "{\"v\":99}").await;
    mv02b_wait_success(&server, &token, "mv02d-disk-act", 11).await;
    assert!(dir.join(&filepath2).exists(), "the updated file must exist");
    // A refused update keeps the old delivery.
    let (status, _) = server
        .put_auth(
            "/api/v5/connectors/mv02d-disk",
            json!({"type": "disk_log", "name": "mv02d-disk",
                       "filepath": "../escape.log",
                       "max_file_size": "1MB",
                       "max_file_number": 3}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    mv02b_publish(&server, &token, "mv02d/disk", "{\"v\":100}").await;
    mv02b_wait_success(&server, &token, "mv02d-disk-act", 12).await;
    mv02d_disk_log_retention_arms(&server, &token, &dir, &unique).await;
    std::fs::remove_file(dir.join(&filepath)).ok();
    std::fs::remove_file(dir.join(&filepath2)).ok();
    if let Ok(entries) = std::fs::read_dir(dir.join("target")) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with(&unique) {
                std::fs::remove_file(entry.path()).ok();
            }
        }
    }
    std::fs::remove_dir_all(&dir).ok();
}
