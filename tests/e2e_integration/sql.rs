use super::*;

/// A real MQTT input and the HTTP tester use the same ordered result.
#[tokio::test]
async fn mv00_projection_order_through_mqtt_and_http() {
    let kernel = spawn_kernel("").await;
    let mut subscriber = mv00_connect(&kernel, "mv00-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-pub").await;
    let cases: &[(&str, &str, &str, &[&str])] = &[
        (
            "explicit",
            "SELECT b, a, c",
            r#"{"c":3,"a":1,"b":2}"#,
            &[r#"{"b":2,"a":1,"c":3}"#],
        ),
        (
            "wildcard",
            "SELECT *",
            r#"{"c":3,"a":1,"b":2}"#,
            &[r#"{"c":3,"a":1,"b":2}"#],
        ),
        (
            "mixed",
            "SELECT c AS first, *, a AS last",
            r#"{"c":3,"a":1,"b":2}"#,
            &[r#"{"first":3,"c":3,"a":1,"b":2,"last":1}"#],
        ),
        (
            "mixed-duplicate",
            "SELECT 99 AS a, *",
            r#"{"b":2,"a":1,"c":3}"#,
            &[r#"{"a":1,"b":2,"c":3}"#],
        ),
        (
            "alias",
            "SELECT b AS x, a",
            r#"{"a":1,"b":2}"#,
            &[r#"{"x":2,"a":1}"#],
        ),
        (
            "duplicate",
            "SELECT b AS x, a AS y, c AS x",
            r#"{"a":1,"b":2,"c":3}"#,
            &[r#"{"x":3,"y":1}"#],
        ),
        (
            "nested",
            "SELECT obj, b",
            r#"{"b":3,"obj":{"z":1,"a":2}}"#,
            &[r#"{"obj":{"z":1,"a":2},"b":3}"#],
        ),
        (
            "shadow",
            "SELECT *",
            r#"{"topic":"payload-topic","qos":2,"b":3}"#,
            &[r#"{"topic":"payload-topic","qos":2,"b":3}"#],
        ),
        (
            "shadow-payload",
            "SELECT payload AS shadow, b",
            r#"{"payload":{"z":1,"a":2},"b":3}"#,
            &[r#"{"shadow":{"z":1,"a":2},"b":3}"#],
        ),
        (
            "metadata",
            "SELECT qos AS level, b, topic AS origin",
            r#"{"b":2,"qos":0}"#,
            &[r#"{"level":0,"b":2,"origin":"mv00/in/metadata"}"#],
        ),
        (
            "missing",
            "SELECT missing, b",
            r#"{"b":2}"#,
            &[r#"{"missing":null,"b":2}"#],
        ),
        (
            "expression",
            "SELECT b + 1, abs(a), c + 2",
            r#"{"b":2,"a":-1.5,"c":4}"#,
            &[r#"{"kuiper_field_0":3,"abs":1.5,"kuiper_field_1":6}"#],
        ),
        (
            "indexed",
            "SELECT p[1], q[1:2]",
            r#"{"p":[7,8],"q":[4,5,6]}"#,
            &[r#"{"kuiper_field_0":7,"kuiper_field_1":[5]}"#],
        ),
        (
            "foreach-indexed",
            "FOREACH payload.items DO item.b[1], item.a[1:2]",
            r#"{"items":[{"b":[7,8],"a":[4,5,6]}]}"#,
            &[r#"{"b":7,"a":[5]}"#],
        ),
        (
            "foreach",
            "FOREACH payload.items",
            r#"{"items":[{"b":2,"a":1},{"b":4,"a":3}]}"#,
            &[r#"{"item":{"b":2,"a":1}}"#, r#"{"item":{"b":4,"a":3}}"#],
        ),
        (
            "foreach-do",
            "FOREACH payload.items DO item.b AS z, item.a, item.b + 1",
            r#"{"items":[{"b":2,"a":1},{"b":4,"a":3}]}"#,
            &[r#"{"z":2,"a":1,"col_2":3}"#, r#"{"z":4,"a":3,"col_2":5}"#],
        ),
        (
            "foreach-duplicate",
            "FOREACH payload.items DO item.b AS x, item.a AS y, item.a AS x",
            r#"{"items":[{"b":2,"a":1}]}"#,
            &[r#"{"x":1,"y":1}"#],
        ),
        (
            "dotted-missing",
            "SELECT b AS z, payload.a, missing",
            r#"{"a":1,"b":2}"#,
            &[r#"{"z":2,"a":1,"missing":null}"#],
        ),
        (
            "foreach-guards",
            "FOREACH payload.items DO item.b AS z, item.missing INCASE item.b > 1",
            r#"{"items":[{"b":0},{"b":2},{"b":3}]}"#,
            &[r#"{"z":2,"missing":null}"#, r#"{"z":3,"missing":null}"#],
        ),
    ];
    let mut failures = Vec::new();
    for (id, fields, input, expected) in cases {
        let sql = format!("{fields} FROM \"mv00/in/{id}\"");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            mv00_rule(id, &sql),
        )
        .await;
        assert_eq!(status, 201, "create {id}: {body}");
        let repeats = if *id == "explicit" { 50 } else { 1 };
        for _ in 0..repeats {
            let messages =
                mv00_publish_outputs(&mut publisher, &mut subscriber, id, input, expected.len())
                    .await;
            let want: Vec<&[u8]> = expected.iter().map(|row| row.as_bytes()).collect();
            if messages != want {
                failures.push(format!("MQTT {id}: {messages:?} != {want:?}"));
            }
        }
        let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
            "sql": sql,
            "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": input},
        })).await;
        assert_eq!(status, 200, "test {id}: {body}");
        let want = if fields.starts_with("FOREACH") {
            format!("[{}]", expected.join(","))
        } else {
            expected[0].to_string()
        };
        let projected_bytes = body["projected"].to_string();
        if projected_bytes != want {
            failures.push(format!("HTTP {id}: {} != {want}", body["projected"]));
        }
        assert_eq!(
            body["output"].to_string(),
            want,
            "HTTP {id} output alias bytes"
        );
        let (status, rpc) = mv00_api(&kernel, reqwest::Method::POST, "/mcp", serde_json::json!({
            "jsonrpc": "2.0", "id": id, "method": "tools/call",
            "params": {"name": "post_api_v5_rule_test", "arguments": {"body": {
                "sql": sql,
                "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": input},
            }}},
        })).await;
        assert_eq!(status, 200, "MCP {id}: {rpc}");
        assert_ne!(rpc["result"]["isError"], true, "MCP {id}: {rpc}");
        let text = rpc["result"]["content"][0]["text"]
            .as_str()
            .expect("MCP result text");
        let forwarded: serde_json::Value = serde_json::from_str(text).expect("MCP forwarded JSON");
        let forwarded_bytes = forwarded["projected"].to_string();
        if forwarded_bytes != want {
            failures.push(format!("MCP {id}: {} != {want}", forwarded["projected"]));
        }
    }
    kernel.finish().await;
    assert!(
        failures.is_empty(),
        "projection mismatches:\n{}",
        failures.join("\n")
    );
}

/// Window aliases keep their first position and use the last value.
/// Regression for upstream issue #47: retain the last-value contract.
#[tokio::test]
async fn mv00_window_w4_last_duplicate_value_through_mqtt() {
    mv00_window_case(
        "w4",
        "SELECT sum(v) AS x, v AS x, count(*) AS n",
        r#"{"x":1,"n":2}"#,
    )
    .await;
}

/// A wildcard after an aggregate replaces its value at its first position.
/// Regression for upstream issue #47: retain the last-value contract.
#[tokio::test]
async fn mv00_window_w5_later_wildcard_value_through_mqtt() {
    mv00_window_case("w5", "SELECT sum(v) AS v, *", r#"{"v":1}"#).await;
}

async fn mv00_window_case(id: &str, fields: &str, expected: &str) {
    mv00_window_inputs_case(id, fields, &[r#"{"v":1}"#, r#"{"v":3}"#], expected).await;
}

async fn mv00_window_inputs_case(id: &str, fields: &str, inputs: &[&str], expected: &str) {
    let sql = format!("{fields} FROM \"mv00/in/{id}\" GROUP BY COUNTWINDOW(2)");
    mv00_window_sql_case(id, &sql, inputs, expected).await;
}

async fn mv00_window_sql_case(id: &str, sql: &str, inputs: &[&str], expected: &str) {
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "window create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-window-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-window-pub").await;
    for input in inputs {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
        )
        .await;
    }
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
        "sql": sql,
        "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": format!("[{}]", inputs.join(","))},
    })).await;
    assert_eq!(status, 200, "window HTTP: {body}");
    assert_eq!(
        (message.payload, body["projected"].to_string()),
        (expected.as_bytes().to_vec(), format!("[{expected}]")),
        "window {id}: MQTT bytes and HTTP result",
    );
    kernel.finish().await;
}

/// An accepted update changes the order; a refused update and restart keep it.
#[tokio::test]
async fn mv00_update_refusal_and_restart_keep_projection_order() {
    let mut kernel = spawn_kernel("").await;
    let id = "update";
    let sql = "SELECT b, a, c FROM \"mv00/in/update\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-update-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-update-pub").await;
    let input = r#"{"a":1,"b":2,"c":3}"#;
    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        input,
        &[r#"{"b":2,"a":1,"c":3}"#],
    )
    .await;
    let sql = "SELECT c, b, a FROM \"mv00/in/update\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/update",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 200, "update: {body}");
    let expected = &[r#"{"c":3,"b":2,"a":1}"#];
    mv00_assert_publish(&mut publisher, &mut subscriber, id, input, expected).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/update",
        mv00_rule(id, "SELECT WHERE WHERE"),
    )
    .await;
    assert_eq!(status, 400, "invalid update: {body}");
    assert_eq!(body["code"], "BAD_REQUEST");
    mv00_assert_publish(&mut publisher, &mut subscriber, id, input, expected).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules/update",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["sql"], sql,
        "refused SQL did not replace durable or live state"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/absent",
        mv00_rule("absent", sql),
    )
    .await;
    assert_eq!(status, 404, "unknown update: {body}");
    drop(publisher);
    drop(subscriber);
    mv00_restart(&mut kernel).await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules/update",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["sql"], sql, "accepted order persisted through restart");
    let mut subscriber = mv00_connect(&kernel, "mv00-restart-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-restart-pub").await;
    mv00_assert_publish(&mut publisher, &mut subscriber, id, input, expected).await;
    kernel.finish().await;
}

/// Ordered aggregate fields and wildcard inputs pass through the real window bus.
#[tokio::test]
async fn mv00_window_projection_order_through_mqtt_and_http() {
    mv00_window_case(
        "window-explicit",
        "SELECT sum(v) AS total, count(*) AS n",
        r#"{"total":4,"n":2}"#,
    )
    .await;
}

#[tokio::test]
async fn mv00_window_fraction_keeps_numeric_value_and_field_order() {
    mv00_window_inputs_case(
        "window-fraction",
        "SELECT sum(v) AS total, count(*) AS n",
        &[r#"{"v":1.5}"#, r#"{"v":3}"#],
        r#"{"total":4.5,"n":2}"#,
    )
    .await;
}

#[tokio::test]
async fn mv00_window_wildcard_keeps_input_order_without_metadata() {
    mv00_window_case(
        "window-wildcard",
        "SELECT *, sum(v) AS total",
        r#"{"v":1,"total":4}"#,
    )
    .await;
}

#[tokio::test]
async fn mv00_window_expressions_keep_names_and_order() {
    mv00_window_case(
        "window-expression",
        "SELECT sum(v), count(*), sum(v) + 1",
        r#"{"sum":4,"count":2,"kuiper_field_0":5}"#,
    )
    .await;
}

/// Time-window metadata stays in its SELECT position on each supported path.
#[tokio::test]
async fn mv00_window_bounds_keep_select_positions_through_mqtt() {
    for (id, window) in [
        ("count-bounds", "COUNTWINDOW(1)"),
        ("tumbling-bounds", "TUMBLINGWINDOW(ms, 50)"),
        ("hopping-bounds", "HOPPINGWINDOW(ms, 1000, 50)"),
        ("sliding-bounds", "SLIDINGWINDOW(ss, 1)"),
    ] {
        let kernel = spawn_kernel("").await;
        let sql = format!("SELECT window_end() AS end, sum(v) AS total, window_start() AS start FROM \"mv00/in/{id}\" GROUP BY {window}");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            mv00_rule(id, &sql),
        )
        .await;
        assert_eq!(status, 201, "window bounds create: {body}");
        let mut subscriber = mv00_connect(&kernel, "mv00-bounds-sub").await;
        mv00_subscribe(&mut subscriber).await;
        let mut publisher = mv00_connect(&kernel, "mv00-bounds-pub").await;
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, br#"{"v":1}"#),
        )
        .await;
        let (head, body) = recv_mqtt(&mut subscriber).await;
        assert_eq!(head & 0xf0, 0x30);
        let message = sy02_parse_publish_classic(head, &body);
        let output: serde_json::Value =
            serde_json::from_slice(&message.payload).expect("window JSON");
        let keys: Vec<&str> = output
            .as_object()
            .expect("window object")
            .keys()
            .map(String::as_str)
            .collect();
        assert_eq!(keys, ["end", "total", "start"], "{id} SELECT order");
        let start = output["start"].as_u64().expect("real window start");
        let end = output["end"].as_u64().expect("real window end");
        assert!(start > 0 && end >= start, "{id} real bounds");
        assert_eq!(output["total"].as_f64(), Some(1.0));
        kernel.finish().await;
    }
}

/// Window rules emit for plain payloads, as stateless rules do.
#[tokio::test]
async fn sb23_window_plain_payload_emits_through_mqtt() {
    let id = "sb23-plain";
    let kernel = spawn_kernel("").await;
    let sql = format!("SELECT * FROM \"mv00/in/{id}\" GROUP BY COUNTWINDOW(1)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, &sql),
    )
    .await;
    assert_eq!(status, 201, "window create: {body}");
    let mut subscriber = mv00_connect(&kernel, "sb23-plain-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "sb23-plain-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, b"hello"),
    )
    .await;
    let (head, body) = tokio::time::timeout(STEP, recv_mqtt(&mut subscriber))
        .await
        .expect("window output in time");
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    let output: serde_json::Value = serde_json::from_slice(&message.payload).expect("window JSON");
    assert_eq!(output, serde_json::json!({}), "plain yields empty row");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "metrics read: {body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert_eq!(body["metrics"]["passed"], 1, "{body}");
    assert_eq!(body["metrics"]["actions.total"], 1, "{body}");
    kernel.finish().await;
}

/// SB-27 plain payloads agree inline and in windows through sockets.
#[tokio::test]
async fn sb27_inline_and_window_plain_payload_agree_through_mqtt() {
    let kernel = spawn_kernel("").await;
    let inline_id = "sb27-inline";
    let inline_sql = format!("SELECT * FROM \"mv00/in/{inline_id}\"");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(inline_id, &inline_sql),
    )
    .await;
    assert_eq!(status, 201, "inline create: {body}");
    let window_id = "sb27-window";
    let window_sql = format!("SELECT * FROM \"mv00/in/{window_id}\" GROUP BY COUNTWINDOW(1)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(window_id, &window_sql),
    )
    .await;
    assert_eq!(status, 201, "window create: {body}");
    let mut subscriber = mv00_connect(&kernel, "sb27-agree-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "sb27-agree-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{inline_id}"), 0, 0, false, b"hello"),
    )
    .await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{window_id}"), 0, 0, false, b"hello"),
    )
    .await;
    let mut got = Vec::new();
    for _ in 0..2 {
        let (head, body) = tokio::time::timeout(STEP, recv_mqtt(&mut subscriber))
            .await
            .expect("rule output in time");
        assert_eq!(head & 0xf0, 0x30);
        let message = sy02_parse_publish_classic(head, &body);
        let output: serde_json::Value =
            serde_json::from_slice(&message.payload).expect("rule JSON");
        assert_eq!(output, serde_json::json!({}), "plain yields empty row");
        got.push(message.topic);
    }
    got.sort();
    assert_eq!(
        got,
        vec![
            format!("mv00/out/{inline_id}"),
            format!("mv00/out/{window_id}")
        ]
    );
    for id in [inline_id, window_id] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/rules/{id}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "metrics read: {body}");
        assert_eq!(body["metrics"]["matched"], 1, "{body}");
        assert_eq!(body["metrics"]["passed"], 1, "{body}");
        assert_eq!(body["metrics"]["failed"], 0, "{body}");
    }
    let object_id = "sb27-object";
    let object_sql = format!("SELECT * FROM \"mv00/in/{object_id}\" GROUP BY COUNTWINDOW(1)");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(object_id, &object_sql),
    )
    .await;
    assert_eq!(status, 201, "object window create: {body}");
    // A JSON object payload uses the same rule path.
    // It delivers the object in windows.
    send_mqtt(
        &mut publisher,
        &sy02_publish311(&format!("mv00/in/{object_id}"), 0, 0, false, br#"{"v":42}"#),
    )
    .await;
    let (head, body) = tokio::time::timeout(STEP, recv_mqtt(&mut subscriber))
        .await
        .expect("object window output in time");
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{object_id}"));
    let output: serde_json::Value =
        serde_json::from_slice(&message.payload).expect("object window JSON");
    assert_eq!(
        output,
        serde_json::json!({"v": 42}),
        "object yields its fields"
    );
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{object_id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "object metrics read: {body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert_eq!(body["metrics"]["passed"], 1, "{body}");
    assert_eq!(body["metrics"]["failed"], 0, "{body}");
    // A filtered input counts failed once. The rule stays stored.
    let filtered_id = "sb27-filtered";
    let filtered_sql = format!("SELECT * FROM \"mv00/in/{filtered_id}\" WHERE v = 1");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(filtered_id, &filtered_sql),
    )
    .await;
    assert_eq!(status, 201, "filtered create: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            &format!("mv00/in/{filtered_id}"),
            0,
            0,
            false,
            br#"{"v":2}"#,
        ),
    )
    .await;
    for _ in 0..100 {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/rules/{filtered_id}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "filtered metrics read: {body}");
        if body["metrics"]["failed"].as_u64().unwrap_or(0) == 1 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{filtered_id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "filtered metrics read: {body}");
    assert_eq!(body["metrics"]["matched"], 1, "{body}");
    assert_eq!(body["metrics"]["passed"], 0, "{body}");
    assert_eq!(body["metrics"]["failed"], 1, "{body}");
    kernel.finish().await;
}

/// Group rows keep first-seen order and each group's payload metadata keys.
#[tokio::test]
async fn mv00_group_windows_keep_order_and_payload_shadows() {
    let mut failures = Vec::new();
    for (id, fields, expected) in [
        (
            "groups",
            "SELECT sum(v) AS total, count(*) AS n, sensor_id",
            vec![
                r#"{"total":4,"n":2,"sensor_id":"z"}"#,
                r#"{"total":2,"n":1,"sensor_id":"a"}"#,
            ],
        ),
        (
            "group-shadows",
            "SELECT *, sum(v) AS total",
            vec![
                r#"{"v":1,"sensor_id":"z","topic":"payload-topic","total":4}"#,
                r#"{"v":2,"sensor_id":"a","total":2}"#,
            ],
        ),
    ] {
        let kernel = spawn_kernel("").await;
        let sql = format!("{fields} FROM \"mv00/in/{id}\" GROUP BY sensor_id, COUNTWINDOW(3)");
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            mv00_rule(id, &sql),
        )
        .await;
        assert_eq!(status, 201, "group create: {body}");
        let mut subscriber = mv00_connect(&kernel, "mv00-group-sub").await;
        mv00_subscribe(&mut subscriber).await;
        let mut publisher = mv00_connect(&kernel, "mv00-group-pub").await;
        let inputs = [
            r#"{"v":1,"sensor_id":"z","topic":"payload-topic"}"#,
            r#"{"v":2,"sensor_id":"a"}"#,
            r#"{"v":3,"sensor_id":"z"}"#,
        ];
        let mut messages = Vec::new();
        for input in inputs {
            send_mqtt(
                &mut publisher,
                &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
            )
            .await;
        }
        for _ in &expected {
            let (head, body) = recv_mqtt(&mut subscriber).await;
            assert_eq!(head & 0xf0, 0x30);
            let message = sy02_parse_publish_classic(head, &body);
            assert_eq!(message.topic, format!("mv00/out/{id}"));
            messages.push(String::from_utf8(message.payload).expect("group JSON bytes"));
        }
        if messages != expected {
            failures.push(format!("MQTT {id}: {messages:?} != {expected:?}"));
        }
        let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
            "sql": sql, "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": format!("[{}]", inputs.join(","))},
        })).await;
        assert_eq!(status, 200, "group HTTP: {body}");
        let want = format!("[{}]", expected.join(","));
        let projected_bytes = body["projected"].to_string();
        if projected_bytes != want {
            failures.push(format!("HTTP {id}: {} != {want}", body["projected"]));
        }
        kernel.finish().await;
    }
    assert!(
        failures.is_empty(),
        "group mismatches:\n{}",
        failures.join("\n")
    );
}

/// Non-array input and a false guard produce no FOREACH output.
#[tokio::test]
async fn mv00_foreach_no_match_and_refused_update_keep_state() {
    let kernel = spawn_kernel("").await;
    let id = "foreach-negative";
    let sql = "FOREACH payload.items DO item.b AS z, item.a FROM \"mv00/in/foreach-negative\" WHERE allow = true";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "FOREACH create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-negative-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-negative-pub").await;
    for input in [
        r#"{"allow":false,"items":[{"b":2,"a":1}]}"#,
        r#"{"allow":true,"items":2}"#,
    ] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
        )
        .await;
        let mut head = [0_u8; 1];
        use tokio::io::AsyncReadExt;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(200),
                subscriber.read_exact(&mut head)
            )
            .await
            .is_err(),
            "no output for unmatched FOREACH"
        );
        let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
            "sql": sql, "context": {"event_type": "message_publish", "topic": format!("mv00/in/{id}"), "payload": input},
        })).await;
        assert_eq!(status, 412, "unmatched HTTP: {body}");
        assert_eq!(body["code"], "NOT_MATCH");
    }
    for invalid in [
        "FOREACH payload.items DO * FROM \"mv00/in/foreach-negative\"",
        "FOREACH payload.items FROM \"mv00/in/foreach-negative\" GROUP BY COUNTWINDOW(2)",
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::PUT,
            "/api/v5/rules/foreach-negative",
            mv00_rule(id, invalid),
        )
        .await;
        assert_eq!(status, 400, "refused FOREACH update: {body}");
        assert_eq!(body["code"], "BAD_REQUEST");
        mv00_assert_publish(
            &mut publisher,
            &mut subscriber,
            id,
            r#"{"allow":true,"items":[{"b":2,"a":1}]}"#,
            &[r#"{"z":2,"a":1}"#],
        )
        .await;
    }
    kernel.finish().await;
}

/// The connector receives the projected bytes after real MQTT ingress.
#[tokio::test]
async fn mv00_webhook_keeps_projection_bytes_through_mqtt() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("HTTP receiver binds");
    let address = listener.local_addr().expect("HTTP receiver address");
    let receiver = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("HTTP request accepted");
        let mut request = Vec::new();
        let header_end = loop {
            let mut byte = [0_u8; 1];
            socket
                .read_exact(&mut byte)
                .await
                .expect("HTTP header byte");
            request.push(byte[0]);
            assert!(request.len() <= 16384, "bounded HTTP header");
            if request.ends_with(b"\r\n\r\n") {
                break request.len();
            }
        };
        let headers = String::from_utf8(request).expect("HTTP headers");
        let length: usize = headers
            .lines()
            .find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse().expect("HTTP length"))
            })
            .expect("HTTP content length");
        assert!(header_end > 0 && length <= 4096, "bounded body");
        let mut body = vec![0_u8; length];
        socket.read_exact(&mut body).await.expect("HTTP body");
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok")
            .await
            .expect("HTTP response");
        body
    });
    let kernel = spawn_kernel("").await;
    let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v1/connectors", serde_json::json!({
        "id": "mv00-hook", "kind": "webhook", "config": {"url": format!("http://{address}/order"), "batch_size": 1},
    })).await;
    assert_eq!(status, 201, "connector create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v1/rules",
        serde_json::json!({
            "name": "mv00-hook-rule", "topic_filter": "mv00/hook/in",
            "sql_query": "SELECT b, a, obj FROM \"mv00/hook/in\"",
            "actions": [{"type": "forwardconnector", "connector_id": "mv00-hook"}],
        }),
    )
    .await;
    assert_eq!(status, 201, "forward rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "mv00-hook-pub").await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            "mv00/hook/in",
            0,
            0,
            false,
            br#"{"a":1,"b":2,"obj":{"z":3,"a":4}}"#,
        ),
    )
    .await;
    let bytes = tokio::time::timeout(STEP, receiver)
        .await
        .expect("HTTP delivery in time")
        .expect("HTTP receiver task");
    assert_eq!(bytes, br#"{"b":2,"a":1,"obj":{"z":3,"a":4}}"#);
    kernel.finish().await;
}

/// The legacy parser also keeps wildcard order and hides injected bounds.
#[tokio::test]
async fn mv00_legacy_window_wildcard_keeps_payload_order() {
    mv00_window_sql_case("legacy-window", "SELECT *, sum(v) AS total FROM \"mv00/in/legacy-window\" GROUP BY COUNTWINDOW(2) ORDER BY v", &[r#"{"v":1,"b":2,"a":3}"#, r#"{"v":3,"b":4,"a":5}"#], r#"{"v":1,"b":2,"a":3,"total":4}"#).await;
}

/// CASE WHEN/THEN/ELSE evaluates per-message through MQTT ingress.
#[tokio::test]
async fn mv00_case_through_mqtt() {
    let kernel = spawn_kernel("").await;
    let id = "case";
    let sql =
        "SELECT CASE WHEN temperature > 40 THEN 'hot' ELSE 'ok' END AS state FROM \"mv00/in/case\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "CASE create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-case-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-case-pub").await;

    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        r#"{"temperature": 72.5}"#,
        &[r#"{"state":"hot"}"#],
    )
    .await;
    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        r#"{"temperature": 20.0}"#,
        &[r#"{"state":"ok"}"#],
    )
    .await;

    let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
        "sql": sql,
        "context": {"event_type": "message_publish", "topic": "mv00/in/case", "payload": r#"{"temperature": 72.5}"#},
    })).await;
    assert_eq!(status, 200, "CASE HTTP: {body}");
    assert_eq!(body["projected"].to_string(), r#"{"state":"hot"}"#);

    kernel.finish().await;
}

/// Nested expressions (parenthesised arithmetic) evaluate per-message through MQTT.
#[tokio::test]
async fn mv00_nested_expressions_through_mqtt() {
    let kernel = spawn_kernel("").await;
    let id = "nested";
    let sql = "SELECT (a + b) * 2 AS x, abs(a - b) AS d FROM \"mv00/in/nested\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "nested create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-nested-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-nested-pub").await;

    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        r#"{"a":3,"b":5}"#,
        &[r#"{"x":16,"d":2}"#],
    )
    .await;
    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        r#"{"a":10,"b":3}"#,
        &[r#"{"x":26,"d":7}"#],
    )
    .await;

    let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
        "sql": sql,
        "context": {"event_type": "message_publish", "topic": "mv00/in/nested", "payload": r#"{"a":3,"b":5}"#},
    })).await;
    assert_eq!(status, 200, "nested HTTP: {body}");
    assert_eq!(body["projected"].to_string(), r#"{"x":16,"d":2}"#);

    kernel.finish().await;
}

/// FOREACH survives a rule replace (same shape) and a kernel restart.
#[tokio::test]
async fn mv00_foreach_update_and_restart_keep_state() {
    let mut kernel = spawn_kernel("").await;
    let id = "foreach-update";
    let sql_v1 = "FOREACH payload.items DO item.b AS z, item.a FROM \"mv00/in/foreach-update\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql_v1),
    )
    .await;
    assert_eq!(status, 201, "FOREACH v1 create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-foreach-upd-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-foreach-upd-pub").await;

    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        r#"{"items":[{"b":2,"a":1},{"b":4,"a":3}]}"#,
        &[r#"{"z":2,"a":1}"#, r#"{"z":4,"a":3}"#],
    )
    .await;

    // Replace with same-shape FOREACH (alias change only): accepted, output keeps flowing.
    let sql_v2 =
        "FOREACH payload.items DO item.b AS x, item.a AS y FROM \"mv00/in/foreach-update\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/foreach-update",
        mv00_rule(id, sql_v2),
    )
    .await;
    assert_eq!(status, 200, "FOREACH replace: {body}");
    mv00_assert_publish(
        &mut publisher,
        &mut subscriber,
        id,
        r#"{"items":[{"b":8,"a":5}]}"#,
        &[r#"{"x":8,"y":5}"#],
    )
    .await;

    // Restart: rule is restored from durable state and still fires.
    mv00_restart(&mut kernel).await;
    let mut subscriber2 = mv00_connect(&kernel, "mv00-foreach-upd-sub2").await;
    mv00_subscribe(&mut subscriber2).await;
    let mut publisher2 = mv00_connect(&kernel, "mv00-foreach-upd-pub2").await;
    mv00_assert_publish(
        &mut publisher2,
        &mut subscriber2,
        id,
        r#"{"items":[{"b":1,"a":9}]}"#,
        &[r#"{"x":1,"y":9}"#],
    )
    .await;

    kernel.finish().await;
}

/// Window (GROUP BY COUNTWINDOW) survives a rule replace and a kernel restart.
#[tokio::test]
async fn mv00_window_update_and_restart_keep_state() {
    let mut kernel = spawn_kernel("").await;
    let id = "window-update";
    let sql_v1 =
        "SELECT sum(v) AS s, count(*) AS n FROM \"mv00/in/window-update\" GROUP BY COUNTWINDOW(2)";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql_v1),
    )
    .await;
    assert_eq!(status, 201, "window v1 create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-window-upd-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-window-upd-pub").await;

    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"v":1}"#.as_bytes(),
        ),
    )
    .await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"v":3}"#.as_bytes(),
        ),
    )
    .await;
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    assert_eq!(message.payload, r#"{"s":4,"n":2}"#.as_bytes());

    // Replace with same-shape window: accepted.
    let sql_v2 = "SELECT sum(v) AS total, count(*) AS cnt FROM \"mv00/in/window-update\" GROUP BY COUNTWINDOW(2)";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/window-update",
        mv00_rule(id, sql_v2),
    )
    .await;
    assert_eq!(status, 200, "window replace: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"v":2}"#.as_bytes(),
        ),
    )
    .await;
    send_mqtt(
        &mut publisher,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"v":4}"#.as_bytes(),
        ),
    )
    .await;
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    assert_eq!(message.payload, r#"{"total":6,"cnt":2}"#.as_bytes());

    // Restart: rule restored and still aggregates.
    mv00_restart(&mut kernel).await;
    let mut subscriber2 = mv00_connect(&kernel, "mv00-window-upd-sub2").await;
    mv00_subscribe(&mut subscriber2).await;
    let mut publisher2 = mv00_connect(&kernel, "mv00-window-upd-pub2").await;
    send_mqtt(
        &mut publisher2,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"v":5}"#.as_bytes(),
        ),
    )
    .await;
    send_mqtt(
        &mut publisher2,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"v":7}"#.as_bytes(),
        ),
    )
    .await;
    let (head, body) = recv_mqtt(&mut subscriber2).await;
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    assert_eq!(message.payload, r#"{"total":12,"cnt":2}"#.as_bytes());

    kernel.finish().await;
}

/// GROUP BY survives a rule replace and a kernel restart.
#[tokio::test]
async fn mv00_group_update_and_restart_keep_state() {
    let mut kernel = spawn_kernel("").await;
    let id = "group-update";
    let sql_v1 = "SELECT k AS key, sum(v) AS s FROM \"mv00/in/group-update\" GROUP BY k";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql_v1),
    )
    .await;
    assert_eq!(status, 201, "group v1 create: {body}");
    let mut subscriber = mv00_connect(&kernel, "mv00-group-upd-sub").await;
    mv00_subscribe(&mut subscriber).await;
    let mut publisher = mv00_connect(&kernel, "mv00-group-upd-pub").await;

    for input in [
        r#"{"k":"x","v":1}"#,
        r#"{"k":"y","v":2}"#,
        r#"{"k":"x","v":3}"#,
    ] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
        )
        .await;
    }
    let mut got = Vec::new();
    for _ in 0..2 {
        let (head, body) = recv_mqtt(&mut subscriber).await;
        assert_eq!(head & 0xf0, 0x30);
        let message = sy02_parse_publish_classic(head, &body);
        assert_eq!(message.topic, format!("mv00/out/{id}"));
        got.push(String::from_utf8(message.payload).expect("group JSON bytes"));
    }
    got.sort();
    assert_eq!(got, vec![r#"{"key":"x","s":4}"#, r#"{"key":"y","s":2}"#]);

    // Replace with same-shape group: accepted.
    let sql_v2 = "SELECT k AS key, sum(v) AS total FROM \"mv00/in/group-update\" GROUP BY k";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/group-update",
        mv00_rule(id, sql_v2),
    )
    .await;
    assert_eq!(status, 200, "group replace: {body}");
    for input in [r#"{"k":"x","v":10}"#, r#"{"k":"z","v":5}"#] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(&format!("mv00/in/{id}"), 0, 0, false, input.as_bytes()),
        )
        .await;
    }
    let mut got = Vec::new();
    for _ in 0..2 {
        let (head, body) = recv_mqtt(&mut subscriber).await;
        assert_eq!(head & 0xf0, 0x30);
        let message = sy02_parse_publish_classic(head, &body);
        assert_eq!(message.topic, format!("mv00/out/{id}"));
        got.push(String::from_utf8(message.payload).expect("group JSON bytes"));
    }
    got.sort();
    assert_eq!(
        got,
        vec![r#"{"key":"x","total":10}"#, r#"{"key":"z","total":5}"#]
    );

    // Restart: rule restored and still groups.
    mv00_restart(&mut kernel).await;
    let mut subscriber2 = mv00_connect(&kernel, "mv00-group-upd-sub2").await;
    mv00_subscribe(&mut subscriber2).await;
    let mut publisher2 = mv00_connect(&kernel, "mv00-group-upd-pub2").await;
    send_mqtt(
        &mut publisher2,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"k":"x","v":1}"#.as_bytes(),
        ),
    )
    .await;
    send_mqtt(
        &mut publisher2,
        &sy02_publish311(
            &format!("mv00/in/{id}"),
            0,
            0,
            false,
            r#"{"k":"x","v":2}"#.as_bytes(),
        ),
    )
    .await;
    let (head, body) = recv_mqtt(&mut subscriber2).await;
    assert_eq!(head & 0xf0, 0x30);
    let message = sy02_parse_publish_classic(head, &body);
    assert_eq!(message.topic, format!("mv00/out/{id}"));
    assert_eq!(message.payload, r#"{"key":"x","total":3}"#.as_bytes());

    kernel.finish().await;
}
