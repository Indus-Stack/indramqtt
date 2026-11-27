use super::*;

/// Qualification (QUAL-NONE: a generic webhook is a test-owned
/// endpoint, not a server product, so no container is needed) for
/// the enterprise HTTP webhook sink on the maintained `reqwest`
/// driver.
///
/// Serves its own loopback receiver (ephemeral `axum` server with
/// HMAC verification plus scripted `429`-then-`200` and `500`
/// faults), registers the sink with the engine's connector
/// manager, creates `ForwardConnector` rules, publishes 500 events
/// through [`RuleEngine::dispatch_ingress`] (never `sink.send`
/// directly), asserts byte-identical bodies with valid HMAC headers
/// and exact counts, proves `429`-then-`200` retries to success,
/// and proves a persistent `500` aborts with the buffer retained.
/// Runs offline: needs only loopback, never skips. Bind failure
/// panics (fail closed).
///
/// Run with e.g.:
/// `cargo test -p broker-rules --lib tests::test_qualify_webhook_write_path_through_rule_engine -- --nocapture`
#[tokio::test]
async fn test_qualify_webhook_write_path_through_rule_engine() {
    use axum::{extract::State, http::StatusCode, routing::post, Router};
    use broker_connectors::{
        ConnectorError, HmacAlgorithm, HmacEncoding, HttpAuth, HttpBodyFormat, HttpHmacSignature,
        HttpMethod, HttpSink, HttpSinkConfig, ReqwestHttpTransport, Sink as _,
    };
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicU64, Ordering as AtomicOrdering},
        Mutex as StdMutex,
    };

    const SECRET: &str = "qual-hmac-secret";
    const EVENTS: usize = 500;

    #[derive(Debug, Default)]
    struct QualState {
        bodies: StdMutex<Vec<Vec<u8>>>,
        signatures: StdMutex<Vec<String>>,
        bad_hmac: AtomicU64,
        flaky_calls: AtomicU64,
        dead_calls: AtomicU64,
    }

    fn expected_signature(body: &[u8]) -> String {
        HttpHmacSignature {
            header_name: "X-Signature-SHA256".to_string(),
            algorithm: HmacAlgorithm::Sha256,
            secret: SECRET.to_string(),
            encoding: HmacEncoding::Hex,
        }
        .sign(body)
    }

    async fn hook_handler(
        State(state): State<Arc<QualState>>,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> (StatusCode, String) {
        let signature = headers
            .get("X-Signature-SHA256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        if signature != expected_signature(&body) {
            state.bad_hmac.fetch_add(1, AtomicOrdering::SeqCst);
            return (StatusCode::BAD_REQUEST, "bad hmac".to_string());
        }
        state.signatures.lock().unwrap().push(signature);
        state.bodies.lock().unwrap().push(body.to_vec());
        (StatusCode::OK, "ok".to_string())
    }

    async fn flaky_handler(
        State(state): State<Arc<QualState>>,
        headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> (StatusCode, String) {
        let call = state.flaky_calls.fetch_add(1, AtomicOrdering::SeqCst);
        if call == 0 {
            return (StatusCode::TOO_MANY_REQUESTS, "slow down".to_string());
        }
        hook_handler(State(state), headers, body).await
    }

    async fn dead_handler(
        State(state): State<Arc<QualState>>,
        _headers: axum::http::HeaderMap,
        body: Bytes,
    ) -> (StatusCode, String) {
        state.dead_calls.fetch_add(1, AtomicOrdering::SeqCst);
        state.bodies.lock().unwrap().push(body.to_vec());
        (StatusCode::INTERNAL_SERVER_ERROR, "nope".to_string())
    }

    let state = Arc::new(QualState::default());
    let app = Router::new()
        .route("/hook", post(hook_handler))
        .route("/flaky", post(flaky_handler))
        .route("/dead", post(dead_handler))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("qual bind loopback");
    let port = listener.local_addr().expect("qual addr").port();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("qual serve");
    });

    fn qual_config(url: &str) -> HttpSinkConfig {
        HttpSinkConfig {
            url: url.to_string(),
            method: HttpMethod::Post,
            headers: HashMap::new(),
            auth: HttpAuth::None,
            body_format: HttpBodyFormat::RawJson,
            signature: Some(HttpHmacSignature {
                header_name: "X-Signature-SHA256".to_string(),
                algorithm: HmacAlgorithm::Sha256,
                secret: SECRET.to_string(),
                encoding: HmacEncoding::Hex,
            }),
            batch_size: Some(EVENTS),
            batch_bytes: Some(1_048_576),
            linger_ms: Some(50),
            timeout_ms: Some(5_000),
            max_retries: Some(3),
            initial_backoff_ms: Some(100),
            max_backoff_ms: Some(2_000),
            buffer_capacity: Some(10_000),
        }
    }

    // 500 events end to end over the real `reqwest` driver, through
    // the broker's rule path (rule registered with the connector
    // manager, message published through the rule engine).
    let engine = RuleEngine::new(65_536, BackpressurePolicy::DropOldest);
    let config = qual_config(&format!("http://127.0.0.1:{port}/hook"));
    let transport = Arc::new(
        ReqwestHttpTransport::new(&config, reqwest::Client::new()).expect("qual transport"),
    );
    let sink = Arc::new(HttpSink::new(config, transport).expect("qual sink"));
    assert_eq!(sink.kind(), "webhook");
    engine.connectors().register("qual-http", sink.clone());
    engine
        .create_rule(
            "qual-http-rule".to_string(),
            TopicFilter::new("sensors/qual").unwrap(),
            None,
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "qual-http".to_string(),
            }],
        )
        .expect("qual rule creates");
    let broker_sink: Arc<dyn BrokerSink> = Arc::new(RecordingSink::default());

    let topic = Topic::new("sensors/qual").unwrap();
    let mut expected_bodies = Vec::with_capacity(EVENTS);
    for seq in 0..EVENTS {
        let payload = format!(r#"{{"seq":{seq},"device":"dev-{:02}"}}"#, seq % 16);
        // The sink forwards the projected JSON verbatim, so the wire
        // body is the canonical re-serialization of the payload.
        let document: serde_json::Value =
            serde_json::from_str(&payload).expect("qual payload parses");
        expected_bodies.push(serde_json::to_vec(&document).expect("qual body encodes"));
        let matched = engine
            .dispatch_ingress(
                &topic,
                &Bytes::from(payload),
                QoS::AtLeastOnce,
                &broker_sink,
            )
            .await;
        assert_eq!(matched, 1, "qual rule must fire once per event");
    }
    sink.flush().await.expect("qual flush");
    assert_eq!(sink.sent_records(), EVENTS as u64, "qual row count");
    assert_eq!(sink.sent_requests(), EVENTS as u64, "qual request count");
    assert_eq!(sink.buffered_rows(), 0);
    eprintln!("qual rows sent: records={EVENTS}");

    // Byte-identical bodies with valid HMAC headers, exact counts, no
    // tolerance: every key present, none extra, none mutated.
    let captured = state.bodies.lock().unwrap().clone();
    assert_eq!(captured.len(), EVENTS, "qual captured count");
    for (index, (got, want)) in captured.iter().zip(expected_bodies.iter()).enumerate() {
        assert_eq!(got, want, "qual body byte mismatch at {index}");
    }
    assert_eq!(
        state.bad_hmac.load(AtomicOrdering::SeqCst),
        0,
        "every request must carry a valid HMAC header"
    );
    assert_eq!(
        state.signatures.lock().unwrap().len(),
        EVENTS,
        "every request must carry the signature header"
    );
    eprintln!("qual rows asserted: count={EVENTS} byte-identical with valid HMAC");

    // 429-then-200 retries to success on the same body, through the
    // rule path as well.
    let flaky_config = HttpSinkConfig {
        url: format!("http://127.0.0.1:{port}/flaky"),
        batch_size: Some(10),
        initial_backoff_ms: Some(1),
        max_backoff_ms: Some(2),
        ..qual_config(&format!("http://127.0.0.1:{port}/flaky"))
    };
    let flaky_transport = Arc::new(
        ReqwestHttpTransport::new(&flaky_config, reqwest::Client::new())
            .expect("qual flaky transport"),
    );
    let flaky_sink =
        Arc::new(HttpSink::new(flaky_config, flaky_transport).expect("qual flaky sink"));
    engine
        .connectors()
        .register("qual-flaky", flaky_sink.clone());
    engine
        .create_rule(
            "qual-flaky-rule".to_string(),
            TopicFilter::new("sensors/qual-flaky").unwrap(),
            None,
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "qual-flaky".to_string(),
            }],
        )
        .expect("qual flaky rule creates");
    let flaky_topic = Topic::new("sensors/qual-flaky").unwrap();
    let bodies_before = state.bodies.lock().unwrap().len();
    let matched = engine
        .dispatch_ingress(
            &flaky_topic,
            &Bytes::from_static(br#"{"seq":"retry-probe"}"#),
            QoS::AtLeastOnce,
            &broker_sink,
        )
        .await;
    assert_eq!(matched, 1, "qual flaky rule must fire");
    flaky_sink.flush().await.expect("429-then-200 must succeed");
    assert_eq!(
        state.flaky_calls.load(AtomicOrdering::SeqCst),
        2,
        "one 429 plus one 200"
    );
    assert_eq!(flaky_sink.sent_requests(), 1);
    assert_eq!(flaky_sink.sent_records(), 1);
    assert_eq!(flaky_sink.buffered_rows(), 0);
    // The retried body arrived intact with a valid HMAC.
    let after_retry = state.bodies.lock().unwrap().clone();
    assert_eq!(after_retry.len(), bodies_before + 1);
    let probe: serde_json::Value =
        serde_json::from_slice(&after_retry[bodies_before]).expect("retry body parses");
    assert_eq!(probe, serde_json::json!({"seq": "retry-probe"}));
    eprintln!("qual retry asserted: 429-then-200 delivered once with valid HMAC");

    // Persistent 500 aborts: retries exhausted, buffer retained for
    // inspection, fail-fast breaker engaged.
    let dead_config = HttpSinkConfig {
        url: format!("http://127.0.0.1:{port}/dead"),
        batch_size: Some(10),
        max_retries: Some(1),
        initial_backoff_ms: Some(1),
        max_backoff_ms: Some(2),
        ..qual_config(&format!("http://127.0.0.1:{port}/dead"))
    };
    let dead_transport = Arc::new(
        ReqwestHttpTransport::new(&dead_config, reqwest::Client::new())
            .expect("qual dead transport"),
    );
    let dead_sink = Arc::new(HttpSink::new(dead_config, dead_transport).expect("qual dead sink"));
    engine.connectors().register("qual-dead", dead_sink.clone());
    engine
        .create_rule(
            "qual-dead-rule".to_string(),
            TopicFilter::new("sensors/qual-dead").unwrap(),
            None,
            true,
            vec![RuleAction::ForwardConnector {
                connector_id: "qual-dead".to_string(),
            }],
        )
        .expect("qual dead rule creates");
    let dead_topic = Topic::new("sensors/qual-dead").unwrap();
    let matched = engine
        .dispatch_ingress(
            &dead_topic,
            &Bytes::from_static(br#"{"seq":"abort-probe"}"#),
            QoS::AtLeastOnce,
            &broker_sink,
        )
        .await;
    assert_eq!(matched, 1, "qual dead rule must fire");
    let err = dead_sink.flush().await.expect_err("500 must abort");
    assert!(
        matches!(err, ConnectorError::Connection(_)),
        "exhausted 500s are backpressure, got {err:?}"
    );
    assert_eq!(
        state.dead_calls.load(AtomicOrdering::SeqCst),
        2,
        "initial attempt plus one retry"
    );
    assert_eq!(dead_sink.buffered_rows(), 1);
    let calls = state.dead_calls.load(AtomicOrdering::SeqCst);
    assert!(dead_sink.flush().await.is_err());
    assert_eq!(
        state.dead_calls.load(AtomicOrdering::SeqCst),
        calls,
        "breaker must fail fast without touching the transport"
    );
    eprintln!("qual abort asserted: persistent 500 aborts with buffer retained");

    // Cleanup: stop the loopback receiver (nothing else to remove;
    // no table, bucket, or container was created).
    server.abort();
    eprintln!("qual done: rows={EVENTS} cleaned loopback receiver");
}
