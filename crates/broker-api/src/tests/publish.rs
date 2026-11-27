use super::*;

#[tokio::test]
async fn publish_to_durable_subscriber_queues_offline() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-11-a", false);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-11-a/subscribe",
            json!({"topic": "w1-11/delivery", "qos": 1}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post_auth(
            "/api/v5/publish",
            json!({"topic": "w1-11/delivery", "payload": "hello-w1-11", "qos": 1}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert!(body["id"].is_string(), "publish accepts with id: {body:?}");

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-11-a/mqueue_messages", &token)
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("mqueue data is a list");
    assert!(
        data.iter()
            .any(|m| m["topic"] == json!("w1-11/delivery") && m["payload"] == json!("hello-w1-11")),
        "published message reaches detached durable queue: {data:?}"
    );
}

#[tokio::test]
async fn publish_without_subscribers_still_accepts() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let (status, body) = server
        .post_auth(
            "/api/v5/publish",
            json!({"topic": "w1-11/nobody-here", "payload": "hello", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 202);
    assert_eq!(body["reason_code"], json!(16));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn publish_malformed_body_is_bad_request() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    for bad in [
        json!({"payload": "x"}),
        json!({"topic": "", "payload": "x"}),
        json!({"topic": "bad/#/topic", "payload": "x"}),
        json!({"topic": "w1-11/ok", "qos": 9, "payload": "x"}),
        json!({"topic": "w1-11/ok", "payload": 42}),
        json!({"topic": "w1-11/ok", "payload": "x", "payload_encoding": "hex"}),
        json!({"topic": "w1-11/ok", "payload": "!!!", "payload_encoding": "base64"}),
        json!({"topic": "w1-11/ok", "retain": "yes", "payload": "x"}),
        json!({"topic": "w1-11/ok", "payload": "x", "payload_encoding": 42}),
        json!({"topic": "w1-11/ok", "payload": "x", "qos": "1"}),
    ] {
        let (status, body) = server.post_auth("/api/v5/publish", bad, &token).await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
        let obj = body.as_object().expect("error body is an object");
        assert_eq!(
            obj.len(),
            2,
            "error shape is exactly code+message: {body:?}"
        );
    }
}

#[tokio::test]
async fn publish_retain_flag_and_base64_honoured() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-11-b", false);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-11-b/subscribe",
            json!({"topic": "w1-11/retained", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    // "hi-w11" as base64 with retain flag set.
    let (status, body) = server
        .post_auth(
            "/api/v5/publish",
            json!({
                "topic": "w1-11/retained",
                "payload": "aGktdzEx",
                "payload_encoding": "base64",
                "qos": 0,
                "retain": true,
            }),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert!(body["id"].is_string());

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-11-b/mqueue_messages", &token)
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("mqueue data is a list");
    assert!(
        data.iter()
            .any(|m| m["topic"] == json!("w1-11/retained") && m["payload"] == json!("hi-w11")),
        "base64 payload decodes and retain flag delivers: {data:?}"
    );
}

#[tokio::test]
async fn publish_bulk_mixed_good_and_bad_reports_per_entry() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-12-a", false);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-12-a/subscribe",
            json!({"topic": "w1-12/bulk/good", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post_auth(
            "/api/v5/publish/bulk",
            json!([
                {"topic": "w1-12/bulk/good", "payload": "hello-w1-12", "qos": 0},
                {"topic": "bad/#/topic", "payload": "x", "qos": 0},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    let results = body.as_array().expect("bulk result is a list");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0]["topic"], json!("w1-12/bulk/good"));
    assert!(
        results[0]["id"].is_string(),
        "bulk good entry accepts with id: {results:?}"
    );
    assert_eq!(results[1]["topic"], json!("bad/#/topic"));
    assert!(results[1]["message"].is_string());
    assert!(results[1]["reason_code"].is_number());

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-12-a/mqueue_messages", &token)
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("mqueue data is a list");
    assert!(
            data.iter()
                .any(|m| m["topic"] == json!("w1-12/bulk/good")
                    && m["payload"] == json!("hello-w1-12")),
            "bulk good entry delivers while bad entry fails: {data:?}"
        );
}

#[tokio::test]
async fn publish_bulk_all_good_delivers_every_entry() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-12-b", false);

    for topic in ["w1-12/all/1", "w1-12/all/2"] {
        let (status, _) = server
            .post_auth(
                "/api/v5/clients/w1-12-b/subscribe",
                json!({"topic": topic, "qos": 0}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
    }

    let (status, body) = server
        .post_auth(
            "/api/v5/publish/bulk",
            json!([
                {"topic": "w1-12/all/1", "payload": "one", "qos": 0},
                {"topic": "w1-12/all/2", "payload": "two", "qos": 0},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let results = body.as_array().expect("bulk result is a list");
    assert_eq!(results.len(), 2);
    for result in results {
        assert!(
            result["id"].is_string(),
            "every bulk entry accepts with id: {results:?}"
        );
        assert!(
            result.get("code").is_none(),
            "success entries carry no error code: {results:?}"
        );
    }

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-12-b/mqueue_messages", &token)
        .await;
    assert_eq!(status, 200);
    let data = body["data"].as_array().expect("mqueue data is a list");
    assert!(
        data.iter()
            .any(|m| m["topic"] == json!("w1-12/all/1") && m["payload"] == json!("one")),
        "first bulk entry delivers: {data:?}"
    );
    assert!(
        data.iter()
            .any(|m| m["topic"] == json!("w1-12/all/2") && m["payload"] == json!("two")),
        "second bulk entry delivers: {data:?}"
    );
}

#[tokio::test]
async fn publish_bulk_malformed_body_is_bad_request() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .post_auth(
            "/api/v5/publish/bulk",
            json!({"topic": "w1-12/ok", "payload": "x"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
    let obj = body.as_object().expect("error body is an object");
    assert_eq!(
        obj.len(),
        2,
        "error shape is exactly code+message: {body:?}"
    );
}
