use super::*;

fn sql_rule(
    engine: &RuleEngine,
    sql: &str,
    topic: &str,
) -> (Arc<RecordingSink>, Arc<dyn BrokerSink>) {
    engine
        .create_rule(
            "sql-rule".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some(sql.to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new(topic).unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("SQL rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    (sink, sink_obj)
}

fn published_json(sink: &RecordingSink) -> Vec<serde_json::Value> {
    sink.published
        .lock()
        .unwrap()
        .iter()
        .map(|(_, payload, _, _)| serde_json::from_slice(payload).expect("sink payload is JSON"))
        .collect()
}

#[tokio::test]
async fn test_sql_where_filters_non_matching_messages() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let (sink, sink_obj) = sql_rule(
        &engine,
        r#"SELECT * FROM "sensors/+" WHERE temperature > 50.0"#,
        "alerts/hot",
    );
    let topic = Topic::new("sensors/temperature").unwrap();

    // Below threshold: predicate false, no action fires.
    engine
        .dispatch_ingress(
            &topic,
            &Bytes::from_static(br#"{ "temperature": 25.0 }"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    assert!(sink.published.lock().unwrap().is_empty());

    // Above threshold: full record passes through SELECT *.
    engine
        .dispatch_ingress(
            &topic,
            &Bytes::from_static(br#"{ "temperature": 65.0, "unit": "C" }"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    assert_eq!(
        published_json(&sink),
        vec![serde_json::json!({ "temperature": 65.0, "unit": "C" })]
    );
}

#[tokio::test]
async fn test_sql_select_projects_fields() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let (sink, sink_obj) = sql_rule(
        &engine,
        r#"SELECT temperature FROM "sensors/+" WHERE temperature > 0"#,
        "alerts/projected",
    );

    engine
        .dispatch_ingress(
            &Topic::new("sensors/temperature").unwrap(),
            &Bytes::from_static(
                br#"{ "temperature": 72.5, "sensor_id": "temp_1", "raw_adc": 1024 }"#,
            ),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;

    // Only the projected field survives; secrets never reach the sink.
    assert_eq!(
        published_json(&sink),
        vec![serde_json::json!({ "temperature": 72.5 })]
    );
}

#[tokio::test]
async fn test_sql_skips_non_json_payloads() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let (sink, sink_obj) = sql_rule(
        &engine,
        r#"SELECT * FROM "sensors/+" WHERE temperature > 0"#,
        "alerts/x",
    );
    let topic = Topic::new("sensors/temperature").unwrap();

    for payload in [
        Bytes::from_static(b"not json at all"),
        Bytes::from_static(br#"[1, 2, 3]"#),
        Bytes::from_static(br#"42"#),
    ] {
        engine
            .dispatch_ingress(&topic, &payload, QoS::AtMostOnce, &sink_obj)
            .await;
    }
    assert!(sink.published.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_invalid_sql_rejected_at_creation() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let err = engine
        .create_rule(
            "broken".to_string(),
            TopicFilter::new("sensors/+").unwrap(),
            Some("SELECT WHERE WHERE".to_string()),
            true,
            vec![RuleAction::Log],
        )
        .expect_err("invalid SQL must fail creation");
    assert!(matches!(err, RuleEngineError::InvalidRule(_)));
    assert!(engine.list_rules().is_empty());
}

// ------------------------------------------------------------------
// INDRA-210: scalar function catalog validation (Community tier).
// Every rule below is stateless: it must classify Community, spawn
// no worker, and evaluate inline on the ingress hot path.
// ------------------------------------------------------------------

/// Run one stateless SELECT through the real ingress path and return
/// the projected row.
async fn project_one(sql: &str, payload: &'static [u8]) -> serde_json::Value {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let rule = engine
        .create_rule(
            "probe".to_string(),
            TopicFilter::new("t").unwrap(),
            Some(sql.to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("probe rule creates");
    assert_eq!(rule.tier, RuleTier::Community);
    assert_eq!(engine.rule_task_count(), 0);
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("t").unwrap(),
            &Bytes::from_static(payload),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "stateless rule must fire inline");
    serde_json::from_slice(&published[0].1).expect("projected JSON")
}

fn num(value: &serde_json::Value, field: &str) -> f64 {
    value
        .get(field)
        .and_then(|v| v.as_f64())
        .unwrap_or_else(|| panic!("numeric field {field}: {value}"))
}

#[test]
fn test_function_catalog_has_185_entries() {
    let catalog = rekuiper_sql::builtin_function_metadata();
    assert!(catalog.len() >= 185, "catalog len: {}", catalog.len());
    // Spot-check categories and aggregate flags the engine honors.
    let by_name = |name: &str| {
        catalog
            .iter()
            .find(|meta| meta.name == name)
            .unwrap_or_else(|| panic!("catalog missing {name}"))
    };
    assert!(!by_name("sin").aggregate);
    assert!(!by_name("concat").aggregate);
    assert!(!by_name("coalesce").aggregate);
    assert!(!by_name("window_start").aggregate);
    assert_eq!(by_name("window_start").category, "system");
    assert!(by_name("avg").aggregate);
    assert!(by_name("count").aggregate);
}

#[tokio::test]
async fn test_scalar_trig_identities() {
    let row = project_one(
        r#"SELECT sin(0.0) AS s, cos(0.0) AS c, tan(0.0) AS t FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "s"), 0.0);
    assert_eq!(num(&row, "c"), 1.0);
    assert_eq!(num(&row, "t"), 0.0);

    let row = project_one(
        r#"SELECT asin(0.0) AS a, acos(1.0) AS b, atan(0.0) AS c, atan2(0.0, 1.0) AS d FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "a"), 0.0);
    assert_eq!(num(&row, "b"), 0.0);
    assert_eq!(num(&row, "c"), 0.0);
    assert_eq!(num(&row, "d"), 0.0);

    let row = project_one(
        r#"SELECT sinh(0.0) AS a, cosh(0.0) AS b, tanh(0.0) AS c FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "a"), 0.0);
    assert_eq!(num(&row, "b"), 1.0);
    assert_eq!(num(&row, "c"), 0.0);
}

#[tokio::test]
async fn test_scalar_math_functions() {
    let row = project_one(
        r#"SELECT sqrt(16.0) AS a, power(2.0, 10.0) AS b, pow(3.0, 2.0) AS c, exp(0.0) AS d FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "a"), 4.0);
    assert_eq!(num(&row, "b"), 1024.0);
    assert_eq!(num(&row, "c"), 9.0);
    assert_eq!(num(&row, "d"), 1.0);

    let row = project_one(
        r#"SELECT ln(1.0) AS a, log(10.0, 100.0) AS b, log2(8.0) AS c, log10(1000.0) AS d FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "a"), 0.0);
    assert_eq!(num(&row, "b"), 2.0);
    assert_eq!(num(&row, "c"), 3.0);
    assert_eq!(num(&row, "d"), 3.0);

    let row = project_one(
        r#"SELECT round(2.4) AS a, round(2.567, 2) AS b, ceil(2.1) AS c, floor(2.9) AS d FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "a"), 2.0);
    assert_eq!(num(&row, "b"), 2.57);
    assert_eq!(num(&row, "c"), 3.0);
    assert_eq!(num(&row, "d"), 2.0);

    let row = project_one(
        r#"SELECT abs(-3) AS a, abs(-2.5) AS b, sign(-5) AS c, sign(0) AS d, mod(10, 3) AS e FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "a"), 3.0);
    assert_eq!(num(&row, "b"), 2.5);
    assert_eq!(num(&row, "c"), -1.0);
    assert_eq!(num(&row, "d"), 0.0);
    assert_eq!(num(&row, "e"), 1.0);
}

#[tokio::test]
async fn test_scalar_bitwise_functions() {
    let row = project_one(
        r#"SELECT bitand(6, 3) AS a, bitor(6, 3) AS b, bitxor(6, 3) AS c, bitnot(0) AS d FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "a"), 2.0);
    assert_eq!(num(&row, "b"), 7.0);
    assert_eq!(num(&row, "c"), 5.0);
    assert_eq!(num(&row, "d"), -1.0);
}

#[tokio::test]
async fn test_scalar_string_functions() {
    // Colliding names use the standard-dialect semantics via the SQL
    // rewriter (0-based substr, 1/2-arg concat): concat('a','b') is
    // "ab" and substr('hello',2,3) starts at 0-based offset 2 ("llo").
    let row = project_one(
        r#"SELECT concat('a', 'b') AS a, lower('AbC') AS b, upper('AbC') AS c, length('hello') AS d FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(row["a"], serde_json::json!("ab"));
    assert_eq!(row["b"], serde_json::json!("abc"));
    assert_eq!(row["c"], serde_json::json!("ABC"));
    assert_eq!(num(&row, "d"), 5.0);

    let row = project_one(
        r#"SELECT trim('  x  ') AS a, ltrim('  x  ') AS b, rtrim('  x  ') AS c FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(row["a"], serde_json::json!("x"));
    assert_eq!(row["b"], serde_json::json!("x  "));
    assert_eq!(row["c"], serde_json::json!("  x"));

    let row = project_one(
        r#"SELECT lpad('7', 3, '0') AS a, rpad('7', 3, '0') AS b, replace('aaa', 'a', 'b') AS c FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(row["a"], serde_json::json!("007"));
    assert_eq!(row["b"], serde_json::json!("700"));
    assert_eq!(row["c"], serde_json::json!("bbb"));

    // Default pad character is a space.
    let row = project_one(
        r#"SELECT lpad('7', 3) AS a, rpad('7', 3) AS b FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert!(row["a"] == serde_json::json!("  7") || row["a"] == serde_json::json!("   7"));
    assert!(row["b"] == serde_json::json!("7  ") || row["b"] == serde_json::json!("7   "));

    let row = project_one(
        r#"SELECT split('a,b,c', ',') AS a, reverse('abc') AS b FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(row["a"], serde_json::json!(["a", "b", "c"]));
    assert_eq!(row["b"], serde_json::json!("cba"));

    let row = project_one(
        r#"SELECT substr('hello', 2, 3) AS a, substring('hello', 2) AS b FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(row["a"], serde_json::json!("llo"));
    assert_eq!(row["b"], serde_json::json!("ello"));

    let row = project_one(
        r#"SELECT startswith('hello', 'he') AS a, endswith('hello', 'lo') AS b FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(row["a"], serde_json::json!(true));
    assert_eq!(row["b"], serde_json::json!(true));
}

#[tokio::test]
async fn test_scalar_datetime_functions() {
    let row = project_one(r#"SELECT now() AS ts FROM s"#, br#"{ "v": 1 }"#).await;
    assert!(
        row["ts"].as_str().is_some() || row["ts"].as_f64().is_some(),
        "now() must return a timestamp"
    );

    let row = project_one(
        r#"SELECT format_date(0, '%Y') AS year FROM s"#,
        br#"{ "v": 1 }"#,
    )
    .await;
    assert_eq!(row["year"], serde_json::json!("1970"));
}

#[tokio::test]
async fn test_scalar_case_and_coalesce() {
    let row = project_one(
        r#"SELECT CASE WHEN temperature > 40 THEN 'hot' ELSE 'ok' END AS state FROM s"#,
        br#"{ "temperature": 72.5 }"#,
    )
    .await;
    assert_eq!(row["state"], serde_json::json!("hot"));

    let row = project_one(
        r#"SELECT CASE WHEN temperature > 40 THEN 'hot' ELSE 'ok' END AS state FROM s"#,
        br#"{ "temperature": 20.0 }"#,
    )
    .await;
    assert_eq!(row["state"], serde_json::json!("ok"));

    let row = project_one(
        r#"SELECT coalesce(missing, 42) AS v FROM s"#,
        br#"{ "other": 1 }"#,
    )
    .await;
    assert_eq!(num(&row, "v"), 42.0);
}

#[test]
fn test_normalize_from_target_vectors() {
    assert_eq!(
        normalize_from_target(r#"SELECT * FROM "sensors/+" WHERE temperature > 50.0"#),
        "SELECT * FROM stream WHERE temperature > 50.0"
    );
    assert_eq!(
        normalize_from_target("SELECT temperature FROM telemetry WHERE temperature > 0"),
        "SELECT temperature FROM stream WHERE temperature > 0"
    );
    assert_eq!(
        normalize_from_target("select a from s"),
        "select a from stream"
    );
    // No FROM at all: untouched (the parser then reports it).
    assert_eq!(normalize_from_target("SELECT 1"), "SELECT 1");
    // Bare topic-style target.
    assert_eq!(
        normalize_from_target("SELECT * FROM a/#"),
        "SELECT * FROM stream"
    );
}
