//! RQ-02 standard-dialect corpus: at least 60 rules covering every
//! construct in the ticket, each with an input message and the exact
//! expected output rows, plus broker-path tests (publish through
//! `dispatch_ingress`, assert the sink) for a plain rule, a FOREACH rule
//! and a window rule.
//!
//! All cases go through the broker-rules front end (`try_evaluate` for the
//! corpus, `RuleEngine::dispatch_ingress` for the broker path), never only
//! through the store.

use async_trait::async_trait;
use broker_protocol::{QoS, Topic, TopicFilter};
use broker_rules::{BackpressurePolicy, BrokerSink, RuleAction, RuleEngine, RuleEngineError};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;

#[derive(Debug, Default)]
struct RecordingSink {
    published: StdMutex<Vec<(String, serde_json::Value)>>,
}

#[async_trait]
impl BrokerSink for RecordingSink {
    async fn publish(
        &self,
        topic: Topic,
        payload: Bytes,
        _qos: QoS,
        _retain: bool,
    ) -> Result<(), RuleEngineError> {
        let value: serde_json::Value =
            serde_json::from_slice(&payload).unwrap_or(serde_json::Value::Null);
        self.published
            .lock()
            .unwrap()
            .push((topic.as_str().to_string(), value));
        Ok(())
    }
}

fn eval_one(
    sql: &str,
    topic: &str,
    payload: &serde_json::Value,
) -> (bool, Option<serde_json::Value>) {
    broker_rules::try_evaluate(Some(sql), None, topic, payload)
        .unwrap_or_else(|e| panic!("corpus SQL failed to evaluate {sql:?}: {e}"))
}

fn assert_match(sql: &str, topic: &str, payload: serde_json::Value, expected: serde_json::Value) {
    let (matched, projected) = eval_one(sql, topic, &payload);
    assert!(matched, "expected match for {sql:?} on {topic:?}");
    assert_eq!(
        projected,
        Some(expected),
        "wrong rows for {sql:?} on {topic:?}"
    );
}

fn assert_no_match(sql: &str, topic: &str, payload: serde_json::Value) {
    let (matched, projected) = eval_one(sql, topic, &payload);
    assert!(!matched, "expected no match for {sql:?} on {topic:?}");
    assert_eq!(projected, None, "expected no rows for {sql:?}");
}

#[test]
fn std_corpus_60_plus() {
    // 1: plain field through nested payload key.
    assert_match(
        r#"SELECT payload.x FROM "t/#""#,
        "t/a",
        serde_json::json!({"payload": {"x": 1}}),
        serde_json::json!({"x": 1}),
    );
    // 2: flat payload via injected `payload` (decoded JSON on first use).
    assert_match(
        r#"SELECT payload.x FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 2}),
        serde_json::json!({"x": 2}),
    );
    // 3: AS alias.
    assert_match(
        r#"SELECT payload.x AS y FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 3}),
        serde_json::json!({"y": 3}),
    );
    // 4: star keeps exactly the payload fields (injected keys stripped).
    assert_match(
        r#"SELECT * FROM "t/#""#,
        "t/a",
        serde_json::json!({"a": 1, "b": 2}),
        serde_json::json!({"a": 1, "b": 2}),
    );
    // 5: star plus explicit topic alias.
    assert_match(
        r#"SELECT *, topic AS t FROM "t/#""#,
        "t/z",
        serde_json::json!({"a": 1}),
        serde_json::json!({"a": 1, "t": "t/z"}),
    );
    // 6: searched CASE.
    assert_match(
        r#"SELECT CASE WHEN payload.x > 10 THEN 'big' ELSE 'small' END AS s FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 20}),
        serde_json::json!({"s": "big"}),
    );
    // 7: simple CASE.
    assert_match(
        r#"SELECT CASE payload.x WHEN 1 THEN 'one' ELSE 'other' END AS s FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 1}),
        serde_json::json!({"s": "one"}),
    );
    // 8: arithmetic add.
    assert_match(
        r#"SELECT payload.x + 1 AS y FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"y": 6}),
    );
    // 9: arithmetic sub.
    assert_match(
        r#"SELECT payload.x - 2 AS y FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"y": 3}),
    );
    // 10: arithmetic mul.
    assert_match(
        r#"SELECT payload.x * 3 AS y FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 4}),
        serde_json::json!({"y": 12}),
    );
    // 11: arithmetic mod.
    assert_match(
        r#"SELECT payload.x % 2 AS y FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"y": 1}),
    );
    // 12: comparison = match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x = 5"#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"x": 5}),
    );
    // 13: comparison != match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x != 5"#,
        "t/a",
        serde_json::json!({"x": 6}),
        serde_json::json!({"x": 6}),
    );
    // 14: comparison < match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x < 10"#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"x": 5}),
    );
    // 15: comparison <= match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x <= 5"#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"x": 5}),
    );
    // 16: comparison > match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x > 10"#,
        "t/a",
        serde_json::json!({"x": 20}),
        serde_json::json!({"x": 20}),
    );
    // 17: comparison >= match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x >= 20"#,
        "t/a",
        serde_json::json!({"x": 20}),
        serde_json::json!({"x": 20}),
    );
    // 18: AND.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x > 1 AND payload.y < 10"#,
        "t/a",
        serde_json::json!({"x": 5, "y": 5}),
        serde_json::json!({"x": 5}),
    );
    // 19: OR.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x = 1 OR payload.x = 2"#,
        "t/a",
        serde_json::json!({"x": 2}),
        serde_json::json!({"x": 2}),
    );
    // 20: NOT.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE NOT payload.x = 1"#,
        "t/a",
        serde_json::json!({"x": 2}),
        serde_json::json!({"x": 2}),
    );
    // 21: =~ child match.
    assert_match(
        r#"SELECT topic AS t FROM "sport/#" WHERE topic =~ 'sport/#'"#,
        "sport/football",
        serde_json::json!({"a": 1}),
        serde_json::json!({"t": "sport/football"}),
    );
    // 22: =~ parent match (`sport/#` matches `sport`).
    assert_match(
        r#"SELECT topic AS t FROM "sport/#" WHERE topic =~ 'sport/#'"#,
        "sport",
        serde_json::json!({"a": 1}),
        serde_json::json!({"t": "sport"}),
    );
    // 23: =~ no match.
    assert_no_match(
        r#"SELECT topic AS t FROM "sport/#" WHERE topic =~ 'sport/#'"#,
        "other",
        serde_json::json!({"a": 1}),
    );
    // 24: IN match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x IN (1, 2, 3)"#,
        "t/a",
        serde_json::json!({"x": 2}),
        serde_json::json!({"x": 2}),
    );
    // 25: NOT IN match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x NOT IN (1, 2, 3)"#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"x": 5}),
    );
    // 26: BETWEEN match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x BETWEEN 1 AND 10"#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"x": 5}),
    );
    // 27: LIKE match.
    assert_match(
        r#"SELECT topic AS t FROM "t/#" WHERE topic LIKE 't/%'"#,
        "t/a",
        serde_json::json!({"a": 1}),
        serde_json::json!({"t": "t/a"}),
    );
    // 28: IS NULL match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.missing IS NULL"#,
        "t/a",
        serde_json::json!({"x": 1}),
        serde_json::json!({"x": 1}),
    );
    // 29: IS NOT NULL match.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x IS NOT NULL"#,
        "t/a",
        serde_json::json!({"x": 1}),
        serde_json::json!({"x": 1}),
    );
    // 30: FROM with two topic filters, second matches.
    assert_match(
        r#"SELECT payload.x AS x FROM "a/#", "b/#""#,
        "b/c",
        serde_json::json!({"x": 7}),
        serde_json::json!({"x": 7}),
    );
    // 31: FROM with two topic filters, none matches.
    assert_no_match(
        r#"SELECT payload.x AS x FROM "a/#", "b/#""#,
        "c/d",
        serde_json::json!({"x": 7}),
    );
    // 32: event source matches nothing yet (fail closed).
    assert_no_match(
        r#"SELECT payload.x AS x FROM "$events/client_connected""#,
        "t/a",
        serde_json::json!({"x": 1}),
    );
    // 33: nested payload access.
    assert_match(
        r#"SELECT payload.a.b AS v FROM "t/#""#,
        "t/a",
        serde_json::json!({"a": {"b": 99}}),
        serde_json::json!({"v": 99}),
    );
    // 34: 1-based array index, first element.
    assert_match(
        r#"SELECT payload.arr[1] AS v FROM "t/#""#,
        "t/a",
        serde_json::json!({"arr": [10, 20, 30]}),
        serde_json::json!({"v": 10}),
    );
    // 35: 1-based array index, second element.
    assert_match(
        r#"SELECT payload.arr[2] AS v FROM "t/#""#,
        "t/a",
        serde_json::json!({"arr": [10, 20, 30]}),
        serde_json::json!({"v": 20}),
    );
    // 36: index 0 has no 1-based meaning and reads Null.
    assert_match(
        r#"SELECT payload.arr[0] AS v FROM "t/#""#,
        "t/a",
        serde_json::json!({"arr": [10, 20]}),
        serde_json::json!({"v": null}),
    );
    // 37: non-JSON payload treated as a string.
    assert_match(
        r#"SELECT payload AS p FROM "t/#""#,
        "t/a",
        serde_json::json!("hello"),
        serde_json::json!({"p": "hello"}),
    );
    // 38: topic message field.
    assert_match(
        r#"SELECT topic AS t FROM "t/#""#,
        "t/my",
        serde_json::json!({"x": 1}),
        serde_json::json!({"t": "t/my"}),
    );
    // 39: qos is omitted in the dry-run tester (no QoS carried), reads Null.
    assert_match(
        r#"SELECT qos AS q FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 1}),
        serde_json::json!({"q": null}),
    );
    // 40: unsupplied message fields are omitted, never constants.
    assert_match(
        r#"SELECT clientid AS c FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 1}),
        serde_json::json!({"c": null}),
    );
    // 41: FOREACH without DO emits {"item": element}.
    {
        let (matched, projected) = eval_one(
            r#"FOREACH payload.items FROM "t/#""#,
            "t/a",
            &serde_json::json!({"items": [1, 2]}),
        );
        assert!(matched);
        assert_eq!(
            projected,
            Some(serde_json::json!([{"item": 1}, {"item": 2}]))
        );
    }
    // 42: FOREACH with DO projection.
    {
        let (matched, projected) = eval_one(
            r#"FOREACH payload.items DO item.x AS x FROM "t/#""#,
            "t/a",
            &serde_json::json!({"items": [{"x": 1}, {"x": 2}]}),
        );
        assert!(matched);
        assert_eq!(projected, Some(serde_json::json!([{"x": 1}, {"x": 2}])));
    }
    // 43: FOREACH with INCASE guard.
    {
        let (matched, projected) = eval_one(
            r#"FOREACH payload.items DO item.x AS x INCASE item.x > 1 FROM "t/#""#,
            "t/a",
            &serde_json::json!({"items": [{"x": 1}, {"x": 2}, {"x": 3}]}),
        );
        assert!(matched);
        assert_eq!(projected, Some(serde_json::json!([{"x": 2}, {"x": 3}])));
    }
    // 44: FOREACH with per-element WHERE over `item`.
    {
        let (matched, projected) = eval_one(
            r#"FOREACH payload.items DO item AS v FROM "t/#" WHERE item > 1"#,
            "t/a",
            &serde_json::json!({"items": [1, 2, 3]}),
        );
        assert!(matched);
        assert_eq!(projected, Some(serde_json::json!([{"v": 2}, {"v": 3}])));
    }
    // 45: FOREACH over an empty array yields no rows.
    assert_no_match(
        r#"FOREACH payload.items FROM "t/#""#,
        "t/a",
        serde_json::json!({"items": []}),
    );
    // 46: FOREACH over a non-array yields no rows.
    assert_no_match(
        r#"FOREACH payload.items FROM "t/#""#,
        "t/a",
        serde_json::json!({"items": 5}),
    );
    // 47: COUNTWINDOW batch aggregation.
    {
        let (matched, projected) = eval_one(
            r#"SELECT count(*) AS n FROM "t/#" GROUP BY COUNTWINDOW(2)"#,
            "t/a",
            &serde_json::json!([{"v": 1}, {"v": 2}]),
        );
        assert!(matched);
        assert_eq!(projected, Some(serde_json::json!([{"n": 2}])));
    }
    // 48: TUMBLINGWINDOW batch average over `payload.v`.
    {
        let (matched, projected) = eval_one(
            r#"SELECT avg(payload.v) AS a FROM "t/#" GROUP BY TUMBLINGWINDOW(ss, 10)"#,
            "t/a",
            &serde_json::json!([{"v": 10}, {"v": 20}]),
        );
        assert!(matched);
        assert_eq!(projected, Some(serde_json::json!([{"a": 15.0}])));
    }
    // 49: plain GROUP BY without a window still evaluates inline.
    assert_match(
        r#"SELECT payload.k AS k FROM "t/#" GROUP BY payload.k"#,
        "t/a",
        serde_json::json!({"k": "a"}),
        serde_json::json!({"k": "a"}),
    );
    // 50: upper() scalar function.
    assert_match(
        r#"SELECT upper(payload.name) AS n FROM "t/#""#,
        "t/a",
        serde_json::json!({"name": "abc"}),
        serde_json::json!({"n": "ABC"}),
    );
    // 51: coalesce() with a missing key.
    assert_match(
        r#"SELECT coalesce(payload.missing, 42) AS v FROM "t/#""#,
        "t/a",
        serde_json::json!({}),
        serde_json::json!({"v": 42}),
    );
    // 52: combined IN plus equality.
    assert_match(
        r#"SELECT * FROM "t/#" WHERE payload.x IN (1, 2) AND payload.y = 'a'"#,
        "t/a",
        serde_json::json!({"x": 1, "y": "a"}),
        serde_json::json!({"x": 1, "y": "a"}),
    );
    // 53: deep payload path.
    assert_match(
        r#"SELECT payload.a.b.c AS v FROM "t/#""#,
        "t/a",
        serde_json::json!({"a": {"b": {"c": 5}}}),
        serde_json::json!({"v": 5}),
    );
    // 54: third array element (1-based).
    assert_match(
        r#"SELECT payload.arr[3] AS v FROM "t/#""#,
        "t/a",
        serde_json::json!({"arr": [1, 2, 3]}),
        serde_json::json!({"v": 3}),
    );
    // 55: CASE over IS NULL.
    assert_match(
        r#"SELECT CASE WHEN payload.x IS NULL THEN 'none' ELSE 'some' END AS s FROM "t/#""#,
        "t/a",
        serde_json::json!({}),
        serde_json::json!({"s": "none"}),
    );
    // 56: multi-field projection.
    assert_match(
        r#"SELECT payload.x AS a, payload.y AS b FROM "t/#""#,
        "t/a",
        serde_json::json!({"x": 1, "y": 2}),
        serde_json::json!({"a": 1, "b": 2}),
    );
    // 57: bare stream source (streaming extension) constrains nothing.
    assert_match(
        r#"SELECT 1 AS one FROM s"#,
        "t/a",
        serde_json::json!({"x": 1}),
        serde_json::json!({"one": 1}),
    );
    // 58: combined topic-match and numeric guard.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x > 10 AND topic =~ 't/#'"#,
        "t/a",
        serde_json::json!({"x": 20}),
        serde_json::json!({"x": 20}),
    );
    // 59: negated parenthesised predicate.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE NOT (payload.x > 10)"#,
        "t/a",
        serde_json::json!({"x": 5}),
        serde_json::json!({"x": 5}),
    );
    // 60: BETWEEN plus topic equality.
    assert_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x BETWEEN 5 AND 15 OR topic = 't/special'"#,
        "t/a",
        serde_json::json!({"x": 10}),
        serde_json::json!({"x": 10}),
    );
    // 61: WHERE false yields no rows.
    assert_no_match(
        r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x > 100"#,
        "t/a",
        serde_json::json!({"x": 5}),
    );
    // 62: plus wildcard filter.
    assert_match(
        r#"SELECT payload.x AS x FROM "sensors/+" WHERE payload.x > 0"#,
        "sensors/kitchen",
        serde_json::json!({"x": 3}),
        serde_json::json!({"x": 3}),
    );
    // 63: single-level wildcard source.
    assert_match(
        r#"SELECT payload.x AS x FROM "sport/+" WHERE payload.x > 0"#,
        "sport/tennis",
        serde_json::json!({"x": 4}),
        serde_json::json!({"x": 4}),
    );
    // 64: multi-level wildcard source.
    assert_match(
        r#"SELECT payload.x AS x FROM "sport/#" WHERE payload.x > 0"#,
        "sport/tennis/player",
        serde_json::json!({"x": 6}),
        serde_json::json!({"x": 6}),
    );
    // 65: rekuiper-only bare target keeps working through the fallback.
    assert_match(
        r#"SELECT * FROM a"#,
        "t/a",
        serde_json::json!({"x": 1}),
        serde_json::json!({"x": 1}),
    );
}

#[test]
fn std_window_error_points_at_token() {
    let err = broker_rules::try_evaluate(
        Some(r#"SELECT * FROM "t/#" GROUP BY TUMBLINGWINDOW(xx, 10)"#),
        None,
        "t/a",
        &serde_json::json!({"x": 1}),
    )
    .expect_err("bad window unit must fail");
    assert!(
        err.contains("line 1"),
        "error must carry a line, got {err:?}"
    );
    assert!(
        !err.contains("column 1:"),
        "window error must not be hardcoded to column 1, got {err:?}"
    );
}

#[test]
fn std_rule_sql_never_rewritten() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    let original = r#"SELECT payload.x AS x FROM "t/#" WHERE payload.x > 10"#.to_string();
    let rule = engine
        .create_rule(
            "keep-sql".to_string(),
            TopicFilter::new("t/#").unwrap(),
            Some(original.clone()),
            true,
            vec![],
        )
        .expect("standard rule creates");
    assert_eq!(rule.sql_query, Some(original));
}

/// Publish one plain standard rule through the broker path.
#[tokio::test]
async fn std_broker_plain() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "plain".to_string(),
            TopicFilter::new("t/plain/#").unwrap(),
            Some(r#"SELECT payload.x AS x FROM "t/plain/#""#.to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/plain").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("plain rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("t/plain/a").unwrap(),
            &Bytes::from_static(br#"{"x": 42}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "plain rule must reach the sink");
    assert_eq!(published[0].0, "out/plain");
    assert_eq!(published[0].1, serde_json::json!({"x": 42}));
}

/// Publish one FOREACH standard rule through the broker path.
#[tokio::test]
async fn std_broker_foreach() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "foreach".to_string(),
            TopicFilter::new("t/foreach/#").unwrap(),
            Some(r#"FOREACH payload.items DO item AS v FROM "t/foreach/#""#.to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/foreach").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("foreach rule creates");
    let sink = Arc::new(RecordingSink::default());
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine
        .dispatch_ingress(
            &Topic::new("t/foreach/a").unwrap(),
            &Bytes::from_static(br#"{"items": [1, 2, 3]}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 3, "foreach must emit one row per element");
    let mut values: Vec<i64> = published
        .iter()
        .map(|(_, v)| v.get("v").and_then(|n| n.as_i64()).unwrap())
        .collect();
    values.sort_unstable();
    assert_eq!(values, vec![1, 2, 3]);
}

/// Publish one window standard rule through the broker path.
#[tokio::test]
async fn std_broker_window() {
    let engine = RuleEngine::new(16, BackpressurePolicy::DropOldest);
    engine
        .create_rule(
            "window".to_string(),
            TopicFilter::new("t/window/#").unwrap(),
            Some(r#"SELECT count(*) AS n FROM "t/window/#" GROUP BY COUNTWINDOW(2)"#.to_string()),
            true,
            vec![RuleAction::Republish {
                topic: Topic::new("out/window").unwrap(),
                qos: QoS::AtMostOnce,
            }],
        )
        .expect("window rule creates");
    let sink = Arc::new(RecordingSink::default());
    sink.published.lock().unwrap().clear();
    let sink_obj: Arc<dyn BrokerSink> = sink.clone();
    engine.set_broker_sink(sink_obj.clone());
    let topic = Topic::new("t/window/a").unwrap();
    engine
        .dispatch_ingress(
            &topic,
            &Bytes::from_static(br#"{"v": 1}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    engine
        .dispatch_ingress(
            &topic,
            &Bytes::from_static(br#"{"v": 2}"#),
            QoS::AtMostOnce,
            &sink_obj,
        )
        .await;
    // COUNTWINDOW(2) flushes on the second arrival in the background worker.
    // Bound: fixed 500ms settle wait, not a timing SLO; the assertion is the
    // exact aggregated count.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    let published = sink.published.lock().unwrap();
    assert_eq!(published.len(), 1, "window must flush one aggregate row");
    assert_eq!(published[0].1, serde_json::json!({"n": 2}));
}
