use super::*;

#[tokio::test]
async fn global_subscriptions_lists_both_with_paging_and_shrinks() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    // Empty system reads as a bare empty array, not an envelope.
    let (status, body) = server.get_auth("/api/v5/subscriptions", &token).await;
    assert_eq!(status, 200);
    let empty = body.as_array().expect("global list is a bare array");
    assert!(empty.is_empty(), "no subscriptions yet: {empty:?}");

    // Two clients subscribed through the real subscribe route.
    for (id, topic) in [("w1-14-a", "w1-14/first"), ("w1-14-b", "w1-14/second")] {
        state.sessions.get_or_create(id, true);
        let (status, _) = server
            .post_auth(
                &format!("/api/v5/clients/{id}/subscribe"),
                json!({"topic": topic, "qos": 1}),
                &token,
            )
            .await;
        assert_eq!(status, 200);
    }

    // List shows both entries with the documented per-entry fields.
    let (status, body) = server.get_auth("/api/v5/subscriptions", &token).await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("global list is a bare array");
    assert_eq!(subs.len(), 2, "both subscriptions listed: {subs:?}");
    // Deterministic order by (clientid, topic).
    assert_eq!(subs[0]["clientid"], json!("w1-14-a"));
    assert_eq!(subs[0]["topic"], json!("w1-14/first"));
    assert_eq!(subs[1]["clientid"], json!("w1-14-b"));
    assert_eq!(subs[1]["topic"], json!("w1-14/second"));
    for entry in subs {
        assert_eq!(entry["qos"], json!(1));
        assert_eq!(entry["nl"], json!(0));
        assert_eq!(entry["rap"], json!(0));
        assert_eq!(entry["rh"], json!(0));
        assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
        assert!(entry["clientid"].is_string());
        assert!(entry["topic"].is_string());
    }

    // Paging is honoured: one row per page.
    let (status, body) = server
        .get_auth("/api/v5/subscriptions?page=1&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    let first = body.as_array().expect("paged list stays a bare array");
    assert_eq!(first.len(), 1, "first page holds one row: {first:?}");
    assert_eq!(first[0]["clientid"], json!("w1-14-a"));

    let (status, body) = server
        .get_auth("/api/v5/subscriptions?page=2&limit=1", &token)
        .await;
    assert_eq!(status, 200);
    let second = body.as_array().expect("paged list stays a bare array");
    assert_eq!(second.len(), 1, "second page holds one row: {second:?}");
    assert_eq!(second[0]["clientid"], json!("w1-14-b"));

    // After one unsubscribe the list shrinks to the survivor.
    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-14-a/unsubscribe",
            json!({"topic": "w1-14/first"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);

    let (status, body) = server.get_auth("/api/v5/subscriptions", &token).await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("global list is a bare array");
    assert_eq!(subs.len(), 1, "list shrinks after unsubscribe: {subs:?}");
    assert_eq!(subs[0]["clientid"], json!("w1-14-b"));
    assert_eq!(subs[0]["topic"], json!("w1-14/second"));
}

#[tokio::test]
async fn client_subscribe_returns_documented_status() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("tk02-sub", true);
    let (status, _) = server
        .post_auth(
            "/api/v5/clients/tk02-sub/subscribe",
            json!({"topic": "tk02/sub", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
}

#[tokio::test]
async fn subscribe_single_registers_and_lists() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-08-a", true);

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-08-a/subscribe",
            json!({"topic": "w1-08/single", "qos": 1}),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(body["clientid"], json!("w1-08-a"));
    assert_eq!(body["topic"], json!("w1-08/single"));
    assert_eq!(body["qos"], json!(1));
    assert_eq!(body["node"], json!(crate::node_scope::local_node_name()));

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-08-a/subscriptions", &token)
        .await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("subscriptions list");
    assert!(
        subs.iter()
            .any(|s| s["topic"] == json!("w1-08/single") && s["qos"] == json!(1)),
        "single subscribe lands and reads back: {subs:?}"
    );
}

#[tokio::test]
async fn subscribe_bulk_mixed_good_and_bad_reports_per_entry() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-08-b", true);

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-08-b/subscribe/bulk",
            json!([
                {"topic": "w1-08/bulk/good", "qos": 0},
                {"topic": "bad/#/topic", "qos": 0},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 200);
    let results = body.as_array().expect("bulk result is a list");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["topic"], json!("w1-08/bulk/good"));
    assert_eq!(results[0]["clientid"], json!("w1-08-b"));

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-08-b/subscriptions", &token)
        .await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("subscriptions list");
    assert!(
        subs.iter().any(|s| s["topic"] == json!("w1-08/bulk/good")),
        "bulk good entry lands: {subs:?}"
    );
    assert!(
        !subs.iter().any(|s| s["topic"] == json!("bad/#/topic")),
        "bulk bad entry never lands: {subs:?}"
    );
}

#[tokio::test]
async fn subscribe_unknown_client_is_not_found() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-08-missing/subscribe",
            json!({"topic": "w1-08/nowhere", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-08-missing/subscribe/bulk",
            json!([{"topic": "w1-08/nowhere", "qos": 0}]),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
}

#[tokio::test]
async fn subscribe_malformed_body_is_bad_request() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-08-c", true);

    for bad in [
        json!({"qos": 0}),
        json!({"topic": "", "qos": 0}),
        json!({"topic": "bad/#/topic", "qos": 0}),
        json!({"topic": "w1-08/ok", "qos": 9}),
        json!({"topic": "w1-08/ok", "nl": 2}),
    ] {
        let (status, body) = server
            .post_auth("/api/v5/clients/w1-08-c/subscribe", bad, &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-08-c/subscribe/bulk",
            json!({"topic": "w1-08/ok"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
}

#[tokio::test]
async fn unsubscribe_single_round_trip_removes_and_reads_empty() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-10-a", true);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-10-a/subscribe",
            json!({"topic": "w1-10/single", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-10-a/subscriptions", &token)
        .await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("subscriptions list");
    assert!(
        subs.iter().any(|s| s["topic"] == json!("w1-10/single")),
        "subscribed entry present before unsubscribe: {subs:?}"
    );

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-10-a/unsubscribe",
            json!({"topic": "w1-10/single"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-10-a/subscriptions", &token)
        .await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("subscriptions list");
    assert!(
        !subs.iter().any(|s| s["topic"] == json!("w1-10/single")),
        "unsubscribed entry gone: {subs:?}"
    );

    // Removing an absent subscription still succeeds.
    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-10-a/unsubscribe",
            json!({"topic": "w1-10/single"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
}

#[tokio::test]
async fn unsubscribe_bulk_mixed_present_absent_reports_success() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-10-b", true);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-10-b/subscribe",
            json!({"topic": "w1-10/bulk/present", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-10-b/unsubscribe/bulk",
            json!([
                {"topic": "w1-10/bulk/present"},
                {"topic": "w1-10/bulk/absent"},
            ]),
            &token,
        )
        .await;
    // Spec reports 204 with no body. Absent still succeeds.
    assert_eq!(status, 204);

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-10-b/subscriptions", &token)
        .await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("subscriptions list");
    assert!(
        !subs
            .iter()
            .any(|s| s["topic"] == json!("w1-10/bulk/present")),
        "bulk-removed entry gone: {subs:?}"
    );
}

#[tokio::test]
async fn unsubscribe_unknown_client_is_not_found() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-10-missing/unsubscribe",
            json!({"topic": "w1-10/nowhere"}),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-10-missing/unsubscribe/bulk",
            json!([{"topic": "w1-10/nowhere"}]),
            &token,
        )
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
}

#[tokio::test]
async fn unsubscribe_malformed_body_is_bad_request() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-10-c", true);

    for bad in [
        json!({"qos": 0}),
        json!({"topic": ""}),
        json!({"topic": "bad/#/topic"}),
    ] {
        let (status, body) = server
            .post_auth("/api/v5/clients/w1-10-c/unsubscribe", bad, &token)
            .await;
        assert_eq!(status, 400);
        assert_eq!(body["code"], json!("BAD_REQUEST"));
        assert!(body["message"].is_string());
    }

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-10-c/unsubscribe/bulk",
            json!({"topic": "w1-10/ok"}),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    // One bad entry fails the whole batch with 400.
    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-10-c/subscribe",
            json!({"topic": "w1-10/bulk/good", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .post_auth(
            "/api/v5/clients/w1-10-c/unsubscribe/bulk",
            json!([
                {"topic": "w1-10/bulk/good"},
                {"topic": "bad/#/topic"},
            ]),
            &token,
        )
        .await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));
    assert!(body["message"].is_string());
}

#[tokio::test]
async fn client_subscriptions_empty_list_is_empty_not_error() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-09-empty", true);

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-09-empty/subscriptions", &token)
        .await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("subscriptions list");
    assert!(subs.is_empty(), "unsubscribed client reads empty: {subs:?}");
}

#[tokio::test]
async fn client_subscriptions_lists_subscribe_with_options() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    state.sessions.get_or_create("w1-09-opts", true);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-09-opts/subscribe",
            json!({"topic": "w1-09/with-opts", "qos": 1, "nl": 1, "rap": 0, "rh": 1}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, _) = server
        .post_auth(
            "/api/v5/clients/w1-09-opts/subscribe",
            json!({"topic": "w1-09/defaults", "qos": 0}),
            &token,
        )
        .await;
    assert_eq!(status, 200);

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-09-opts/subscriptions", &token)
        .await;
    assert_eq!(status, 200);
    let subs = body.as_array().expect("subscriptions list");
    assert_eq!(subs.len(), 2, "both subscribes read back: {subs:?}");
    let entry = subs
        .iter()
        .find(|s| s["topic"] == json!("w1-09/with-opts"))
        .expect("subscribed entry present");
    assert_eq!(entry["qos"], json!(1));
    assert_eq!(entry["nl"], json!(1));
    assert_eq!(entry["rap"], json!(0));
    assert_eq!(entry["rh"], json!(1));
    assert_eq!(entry["node"], json!(crate::node_scope::local_node_name()));
    assert_eq!(entry["clientid"], json!("w1-09-opts"));
    let defaults = subs
        .iter()
        .find(|s| s["topic"] == json!("w1-09/defaults"))
        .expect("default entry present");
    assert_eq!(defaults["qos"], json!(0));
    assert_eq!(defaults["nl"], json!(0));
    assert_eq!(defaults["rap"], json!(0));
    assert_eq!(defaults["rh"], json!(0));
    assert_eq!(
        defaults["node"],
        json!(crate::node_scope::local_node_name())
    );
    assert_eq!(defaults["clientid"], json!("w1-09-opts"));
}

#[tokio::test]
async fn client_subscriptions_unknown_is_not_found() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, body) = server
        .get_auth("/api/v5/clients/w1-09-missing/subscriptions", &token)
        .await;
    assert_eq!(status, 404);
    assert_eq!(body["code"], json!("CLIENTID_NOT_FOUND"));
}
