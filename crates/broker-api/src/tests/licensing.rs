use super::*;

/// The licence endpoints have the shapes of the reference API and
/// show an open-source licence without a real limit.
#[tokio::test]
async fn the_licence_endpoints_answer_as_the_reference_does() {
    let (server, state) = TestServer::start().await;
    let token = server.login_as_admin().await;

    let (status, doc) = server.get_auth("/api/v5/license", &token).await;
    assert_eq!(status, 200);
    assert_eq!(doc["max_sessions"], json!(10_000_000_000u64));
    assert_eq!(doc["expiry"], json!(false));
    let (status, same) = server
        .post_auth("/api/v5/license", json!({"key": "default"}), &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(same, doc);
    let (status, body) = server.post_auth("/api/v5/license", json!({}), &token).await;
    assert_eq!(status, 400);
    assert_eq!(body["code"], json!("BAD_REQUEST"));

    let (status, setting) = server.get_auth("/api/v5/license/setting", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        setting,
        json!({
            "connection_low_watermark": "75%",
            "connection_high_watermark": "80%",
            "high_watermark_timezone": "system",
        })
    );
    let (status, updated) = server
        .put_auth(
            "/api/v5/license/setting",
            json!({"connection_high_watermark": "90%"}),
            &token,
        )
        .await;
    assert_eq!(status, 200, "update: {updated}");
    assert_eq!(updated["connection_high_watermark"], json!("90%"));
    assert_eq!(updated["connection_low_watermark"], json!("75%"));
    let (status, body) = server
        .put_auth(
            "/api/v5/license/setting",
            json!({"connection_low_watermark": "95%"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "low above high: {body}");

    let (status, empty) = server
        .get_auth("/api/v5/license/session_hwm_history", &token)
        .await;
    assert_eq!(status, 200);
    assert_eq!(empty, json!({"period": "monthly", "count": 0, "data": []}));
    state.license.record_sessions(12, 20_731 * 86_400 + 5);
    let (status, daily) = server
        .get_auth(
            "/api/v5/license/session_hwm_history?period=daily&limit=5",
            &token,
        )
        .await;
    assert_eq!(status, 200);
    assert_eq!(
        daily,
        json!({
            "period": "daily",
            "count": 1,
            "data": [{
                "period": "2026-10-05",
                "high_watermark": 12,
                "observed_at": "2026-10-05T00:00:05+00:00",
            }],
        })
    );
    let (status, _) = server
        .get_auth("/api/v5/license/session_hwm_history?period=weekly", &token)
        .await;
    assert_eq!(status, 400);
}
