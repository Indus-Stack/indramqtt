use super::*;

fn step(name: &str, topics: &[&str], value: &str) -> TransformStep {
    TransformStep {
        name: name.to_string(),
        topics: topics
            .iter()
            .map(|t| TopicFilter::new((*t).to_string()).expect("valid filter"))
            .collect(),
        failure_action: "drop".to_string(),
        enable: true,
        operations: vec![TransformOperation {
            key: "payload".to_string(),
            value: value.to_string(),
        }],
    }
}

#[test]
fn chain_order_defines_execution_order() {
    let topic = Topic::new("live/1".to_string()).expect("valid topic");
    let payload = Bytes::from("m");
    let steps = vec![
        step("t-a", &["live/#"], "${payload}[t-a]"),
        step("t-b", &["live/#"], "${payload}[t-b]"),
    ];
    let (_, out, shed) = apply_transform_steps(&steps, &topic, &payload);
    assert!(!shed);
    assert_eq!(String::from_utf8_lossy(&out), "m[t-a][t-b]");
    let reversed = vec![
        step("t-b", &["live/#"], "${payload}[t-b]"),
        step("t-a", &["live/#"], "${payload}[t-a]"),
    ];
    let (_, out, shed) = apply_transform_steps(&reversed, &topic, &payload);
    assert!(!shed);
    assert_eq!(String::from_utf8_lossy(&out), "m[t-b][t-a]");
}

#[test]
fn non_matching_and_disabled_steps_are_skipped() {
    let topic = Topic::new("live/1".to_string()).expect("valid topic");
    let payload = Bytes::from("m");
    let mut steps = vec![
        step("other", &["other/#"], "${payload}[x]"),
        step("live", &["live/#"], "${payload}[y]"),
    ];
    steps[1].enable = false;
    let (_, out, shed) = apply_transform_steps(&steps, &topic, &payload);
    assert!(!shed);
    assert_eq!(String::from_utf8_lossy(&out), "m");
}

#[test]
fn invalid_topic_rewrite_sheds_unless_ignored() {
    let topic = Topic::new("live/1".to_string()).expect("valid topic");
    let payload = Bytes::from("m");
    let bad = TransformStep {
        name: "bad".to_string(),
        topics: vec![TopicFilter::new("live/#".to_string()).expect("valid filter")],
        failure_action: "drop".to_string(),
        enable: true,
        operations: vec![TransformOperation {
            key: "topic".to_string(),
            value: String::new(),
        }],
    };
    let (_, _, shed) = apply_transform_steps(std::slice::from_ref(&bad), &topic, &payload);
    assert!(shed, "invalid rewrite with `drop` sheds the message");
    let mut ignored = bad.clone();
    ignored.failure_action = "ignore".to_string();
    let (out_topic, _, shed) =
        apply_transform_steps(std::slice::from_ref(&ignored), &topic, &payload);
    assert!(!shed);
    assert_eq!(out_topic.as_str(), "live/1");
}

#[test]
fn empty_chain_costs_no_rewrite() {
    let topic = Topic::new("live/1".to_string()).expect("valid topic");
    let payload = Bytes::from("m");
    let (out_topic, out_payload, shed) = apply_transform_steps(&[], &topic, &payload);
    assert!(!shed);
    assert_eq!(out_topic.as_str(), "live/1");
    assert_eq!(out_payload, payload);
}

#[test]
fn metrics_attribute_only_runs_and_sheds() {
    let topic = Topic::new("live/1".to_string()).expect("valid topic");
    let payload = Bytes::from("m");
    let mut steps = vec![
        step("hit", &["live/#"], "${payload}[h]"),
        step("miss", &["other/#"], "${payload}[x]"),
    ];
    steps.push(TransformStep {
        name: "skip".to_string(),
        topics: vec![TopicFilter::new("live/#".to_string()).expect("valid filter")],
        failure_action: "drop".to_string(),
        enable: false,
        operations: vec![TransformOperation {
            key: "payload".to_string(),
            value: "${payload}[s]".to_string(),
        }],
    });
    let mut metrics = HashMap::new();
    for name in ["hit", "miss", "skip"] {
        metrics.insert(name.to_string(), Arc::new(TransformMetrics::new()));
    }
    let (_, out, shed) = apply_transform_steps_with_metrics(&steps, &metrics, &topic, &payload);
    assert!(!shed);
    assert_eq!(String::from_utf8_lossy(&out), "m[h]");
    assert_eq!(metrics["hit"].snapshot(), (1, 1, 0));
    assert_eq!(metrics["miss"].snapshot(), (0, 0, 0));
    assert_eq!(metrics["skip"].snapshot(), (0, 0, 0));
    // A shed counts failed and stops the chain.
    let bad = TransformStep {
        name: "bad".to_string(),
        topics: vec![TopicFilter::new("live/#".to_string()).expect("valid filter")],
        failure_action: "drop".to_string(),
        enable: true,
        operations: vec![TransformOperation {
            key: "topic".to_string(),
            value: String::new(),
        }],
    };
    let mut shed_metrics = HashMap::new();
    shed_metrics.insert("bad".to_string(), Arc::new(TransformMetrics::new()));
    shed_metrics.insert("later".to_string(), Arc::new(TransformMetrics::new()));
    let chained = vec![bad, step("later", &["live/#"], "${payload}[l]")];
    let (_, _, shed) =
        apply_transform_steps_with_metrics(&chained, &shed_metrics, &topic, &payload);
    assert!(shed);
    assert_eq!(shed_metrics["bad"].snapshot(), (1, 0, 1));
    assert_eq!(shed_metrics["later"].snapshot(), (0, 0, 0));
    // Steps without a record still rewrite without bumps.
    let (_, out, shed) =
        apply_transform_steps_with_metrics(&steps, &HashMap::new(), &topic, &payload);
    assert!(!shed);
    assert_eq!(String::from_utf8_lossy(&out), "m[h]");
}
