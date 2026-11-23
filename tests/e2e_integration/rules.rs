use super::*;

/// Built-in rule actions of the reference API, through real sockets and
/// the real management API.
///
/// The kernel boots from `indra.toml` with its own MQTT listener, so no
/// edge is needed. Each rule is created with `POST /api/v5/rules`. Real
/// MQTT clients publish, and other clients receive on the republish
/// topic. Every value is random, so the broker cannot know it.
#[tokio::test]
async fn rule_builtin_actions_republish_and_console_through_the_broker() {
    use tokio::io::AsyncWriteExt;

    const QUIET: std::time::Duration = std::time::Duration::from_millis(1500);
    let tag = format!(
        "rb-actions-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    let kernel_log = std::env::temp_dir().join(format!("indramqtt-{tag}.log"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");

    let bl_port = ephemeral_port();
    let mqtt_port = ephemeral_port();
    let api_port = ephemeral_port();
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"rb-e2e\"\n\
             data_dir = \"{}\"\n\
             brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
             \n\
             [listeners.tcp]\n\
              bind = \"127.0.0.1:{mqtt_port}\"\n\
              \n\
              [listeners.tls]\n\
              enabled = false\n\
              \n\
              [listeners.ws]\n\
              enabled = false\n\
              \n\
              [listeners.wss]\n\
              enabled = false\n\
              \n\
              [listeners.api]\n\
              bind = \"127.0.0.1:{api_port}\"\n\
              \n\
              [logging]\n\
              level = \"info\"\n\
              \n\
              [[tenants.rules]]\n\
              expression = \"t-${{username}}\"\n",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");

    let api_key = "rb-e2e-test-key-0001";
    let log_file = std::fs::File::create(&kernel_log).expect("create kernel log");
    let mut child = tokio::process::Command::new(shard_kernel_binary())
        .arg("--config-dir")
        .arg(&config_dir)
        .arg("--allow-anonymous")
        .env("INDRA_API_KEYS", api_key)
        .env("NO_COLOR", "1")
        .stdout(log_file.try_clone().expect("clone log handle"))
        .stderr(log_file)
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real kernel");
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}");
    let deadline = std::time::Instant::now() + BOOT;
    loop {
        let up = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            http.get(format!("{base}/healthz")).send(),
        )
        .await;
        if let Ok(Ok(res)) = up {
            if let Ok(text) = res.text().await {
                if text == "OK"
                    && tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
                        .await
                        .is_ok()
                {
                    break;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            panic!("rule actions kernel never opened API {api_port} and MQTT {mqtt_port}");
        }
        if let Ok(Some(status)) = child.try_wait() {
            panic!("rule actions kernel exited during boot: {status}");
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    async fn api(
        http: &reqwest::Client,
        base: &str,
        key: &str,
        method: reqwest::Method,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> (u16, serde_json::Value) {
        let mut request = http
            .request(method, format!("{base}{path}"))
            .bearer_auth(key);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let res = tokio::time::timeout(STEP, request.send())
            .await
            .expect("API in time")
            .expect("API send");
        let status = res.status().as_u16();
        let text = res.text().await.expect("API body");
        let value = serde_json::from_str(&text).unwrap_or(serde_json::Value::Null);
        (status, value)
    }
    async fn dial(port: u16) -> tokio::net::TcpStream {
        tokio::time::timeout(STEP, tokio::net::TcpStream::connect(("127.0.0.1", port)))
            .await
            .expect("dial in time")
            .expect("dial")
    }
    async fn connect(port: u16, client_id: &str, username: Option<&str>) -> tokio::net::TcpStream {
        let mut sock = dial(port).await;
        let creds = username.map(|name| (name, &b"pw"[..]));
        tokio::time::timeout(
            STEP,
            sock.write_all(&sy02_connect311(client_id, true, 60, creds)),
        )
        .await
        .expect("CONNECT in time")
        .expect("CONNECT send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut sock))
            .await
            .expect("CONNACK in time")
            .expect("CONNACK");
        assert_eq!(head, 0x20, "kernel answers {client_id} with CONNACK");
        assert_eq!(body, vec![0x00, 0x00], "CONNECT of {client_id} is accepted");
        sock
    }
    async fn subscribe(sock: &mut tokio::net::TcpStream, filter: &str, qos: u8) {
        tokio::time::timeout(STEP, sock.write_all(&sy02_subscribe(1, &[(filter, qos)])))
            .await
            .expect("SUBSCRIBE in time")
            .expect("SUBSCRIBE send");
        let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
            .await
            .expect("SUBACK in time")
            .expect("SUBACK");
        assert_eq!(head & 0xF0, 0x90, "kernel answers with SUBACK");
        assert_eq!(body, vec![0x00, 0x01, qos], "subscribe to {filter} granted");
    }
    async fn publish(sock: &mut tokio::net::TcpStream, topic: &str, qos: u8, payload: &[u8]) {
        tokio::time::timeout(
            STEP,
            sock.write_all(&sy02_publish311(topic, 7, qos, false, payload)),
        )
        .await
        .expect("publish in time")
        .expect("publish send");
        if qos > 0 {
            let (head, _) = tokio::time::timeout(STEP, read_mqtt_packet(sock))
                .await
                .expect("PUBACK in time")
                .expect("PUBACK");
            assert_eq!(head, 0x40, "publisher sees PUBACK");
        }
    }
    /// The next PUBLISH on the socket, or `None` when nothing comes in
    /// the given time.
    async fn next_publish(
        sock: &mut tokio::net::TcpStream,
        wait: std::time::Duration,
    ) -> Option<Sy02Publish> {
        let (head, body) = tokio::time::timeout(wait, read_mqtt_packet(sock))
            .await
            .ok()?
            .expect("downlink");
        assert_eq!(head & 0xF0, 0x30, "downlink is PUBLISH");
        Some(sy02_parse_publish_classic(head, &body))
    }
    let nonce = || format!("{:016x}", rand::random::<u64>());

    // Two tenants own rules; clients with these usernames land there.
    for ns in ["t-rb-a", "t-rb-b"] {
        let (status, body) = api(
            &http,
            &base,
            api_key,
            reqwest::Method::POST,
            &format!("/api/v5/mt/ns/{ns}"),
            None,
        )
        .await;
        assert!(status < 300, "namespace {ns} answered {status}: {body}");
    }
    let make_rule = |name: &str, sql: &str, actions: serde_json::Value| serde_json::json!({"id": name, "name": name, "sql": sql, "actions": actions});

    // 1. A fixed topic and the default `${payload}`. The republish topic
    // matches the filter of the rule itself: a loop would deliver more
    // than one copy.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-fixed",
            "SELECT * FROM \"rb/one/#\"",
            serde_json::json!([
                {"function": "republish", "args": {"topic": "rb/one/out", "payload": "${payload}"}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    assert_eq!(body["actions"][0]["function"], "republish");
    let mut sub_one = connect(mqtt_port, "rb-sub-one", None).await;
    subscribe(&mut sub_one, "rb/one/out", 0).await;
    let mut pubr = connect(mqtt_port, "rb-pub", None).await;
    let value = nonce();
    let message = format!("{{\"v\":\"{value}\"}}");
    publish(&mut pubr, "rb/one/in", 0, message.as_bytes()).await;
    let got = next_publish(&mut sub_one, STEP).await.expect("republish");
    assert_eq!(got.topic, "rb/one/out");
    assert_eq!(got.payload, message.as_bytes());
    assert!(
        next_publish(&mut sub_one, QUIET).await.is_none(),
        "one input gives one output: a republish does not start its own rule again"
    );

    // 2. A payload template with selected fields and a topic template.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-template",
            "SELECT temp, dev FROM \"rb/two/+\"",
            serde_json::json!([
                {"function": "republish",
                 "args": {"topic": "rb/two/out/${dev}", "payload": "temp=${temp};dev=${dev}"}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut sub_two = connect(mqtt_port, "rb-sub-two", None).await;
    subscribe(&mut sub_two, "rb/two/out/#", 0).await;
    let dev = nonce();
    let temp = rand::random::<u32>();
    let message = format!("{{\"temp\":{temp},\"dev\":\"{dev}\",\"noise\":\"x\"}}");
    publish(&mut pubr, "rb/two/in", 0, message.as_bytes()).await;
    let got = next_publish(&mut sub_two, STEP).await.expect("template");
    assert_eq!(got.topic, format!("rb/two/out/{dev}"));
    assert_eq!(got.payload, format!("temp={temp};dev={dev}").as_bytes());

    // 3. A WHERE clause filters.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-where",
            "SELECT * FROM \"rb/three\" WHERE n > 5",
            serde_json::json!([{"function": "republish", "args": {"topic": "rb/three/out"}}]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut sub_three = connect(mqtt_port, "rb-sub-three", None).await;
    subscribe(&mut sub_three, "rb/three/out", 0).await;
    publish(&mut pubr, "rb/three", 0, br#"{"n":1,"w":"low"}"#).await;
    assert!(
        next_publish(&mut sub_three, QUIET).await.is_none(),
        "WHERE n > 5 drops n = 1"
    );
    let high = format!("{{\"n\":9,\"w\":\"{}\"}}", nonce());
    publish(&mut pubr, "rb/three", 0, high.as_bytes()).await;
    let got = next_publish(&mut sub_three, STEP).await.expect("where");
    assert_eq!(got.payload, high.as_bytes());

    // 4. QoS: the default follows the input message, a number is fixed,
    // and a template reads a field.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-qos",
            "SELECT * FROM \"rb/four\"",
            serde_json::json!([
                {"function": "republish", "args": {"topic": "rb/four/follow"}},
                {"function": "republish", "args": {"topic": "rb/four/fixed", "qos": 1}},
                {"function": "republish", "args": {"topic": "rb/four/field", "qos": "${lvl}"}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut sub_four = connect(mqtt_port, "rb-sub-four", None).await;
    subscribe(&mut sub_four, "rb/four/+", 2).await;
    for (input_qos, expect) in [(1u8, [1u8, 1, 2]), (0u8, [0u8, 1, 2])] {
        let message = format!("{{\"lvl\":2,\"id\":\"{}\"}}", nonce());
        publish(&mut pubr, "rb/four", input_qos, message.as_bytes()).await;
        let mut seen = std::collections::HashMap::new();
        for _ in 0..3 {
            let got = next_publish(&mut sub_four, STEP).await.expect("qos output");
            if got.qos == 1 {
                tokio::time::timeout(STEP, sub_four.write_all(&sy02_puback(got.packet_id)))
                    .await
                    .expect("PUBACK in time")
                    .expect("PUBACK send");
            }
            seen.insert(got.topic.clone(), got.qos);
        }
        assert_eq!(
            seen.len(),
            3,
            "three outputs for input QoS {input_qos}: {seen:?}"
        );
        assert_eq!(
            seen["rb/four/follow"], expect[0],
            "follows input QoS {input_qos}"
        );
        assert_eq!(seen["rb/four/fixed"], expect[1], "fixed QoS 1");
        assert_eq!(seen["rb/four/field"], expect[2], "QoS from the field `lvl`");
    }

    // 5. A retained republish is kept for later subscribers.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-retain",
            "SELECT * FROM \"rb/five\"",
            serde_json::json!([
                {"function": "republish", "args": {"topic": "rb/five/kept", "retain": true}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let kept = format!("{{\"keep\":\"{}\"}}", nonce());
    publish(&mut pubr, "rb/five", 0, kept.as_bytes()).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let mut late = connect(mqtt_port, "rb-late", None).await;
    subscribe(&mut late, "rb/five/kept", 0).await;
    let got = next_publish(&mut late, STEP)
        .await
        .expect("retained replay");
    assert_eq!(got.payload, kept.as_bytes());
    assert!(got.retain, "the replay carries the retain flag");

    // 6. A rule of tenant A republishes inside tenant A only.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(serde_json::json!({
            "name": "rb-tenant-a",
            "tenant": "t-rb-a",
            "sql": "SELECT * FROM \"rb/six\"",
            "actions": [{"function": "republish", "args": {"topic": "rb/six/out"}}]
        })),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut a_sub = connect(mqtt_port, "rb-a-sub", Some("rb-a")).await;
    let mut a_pub = connect(mqtt_port, "rb-a-pub", Some("rb-a")).await;
    let mut b_sub = connect(mqtt_port, "rb-b-sub", Some("rb-b")).await;
    let mut b_pub = connect(mqtt_port, "rb-b-pub", Some("rb-b")).await;
    let mut default_sub = connect(mqtt_port, "rb-d-sub", None).await;
    for sock in [&mut a_sub, &mut b_sub, &mut default_sub] {
        subscribe(sock, "rb/six/out", 0).await;
    }
    let secret = nonce();
    let message = format!("{{\"s\":\"{secret}\"}}");
    publish(&mut a_pub, "rb/six", 0, message.as_bytes()).await;
    let got = next_publish(&mut a_sub, STEP)
        .await
        .expect("tenant A output");
    assert_eq!(got.payload, message.as_bytes());
    assert!(
        next_publish(&mut b_sub, QUIET).await.is_none(),
        "tenant B holds none of the output of a rule of tenant A"
    );
    assert!(
        next_publish(&mut default_sub, std::time::Duration::from_millis(300))
            .await
            .is_none(),
        "the default tenant holds none of the output of a rule of tenant A"
    );
    // A publish of tenant B on the same topic does not fire the rule.
    publish(&mut b_pub, "rb/six", 0, message.as_bytes()).await;
    assert!(
        next_publish(&mut a_sub, QUIET).await.is_none(),
        "a publish of tenant B does not fire a rule of tenant A"
    );
    assert!(
        next_publish(&mut b_sub, std::time::Duration::from_millis(300))
            .await
            .is_none(),
        "a publish of tenant B gives no output in tenant B"
    );

    // 7. MQTT 5 properties: user properties from a result field and the
    // message expiry from the arguments.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-props",
            "SELECT * FROM \"rb/seven\"",
            serde_json::json!([
                {"function": "republish",
                 "args": {"topic": "rb/seven/out", "user_properties": "${props}",
                          "mqtt_properties": {"Message-Expiry-Interval": "120"}}}
            ]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let mut v5_sub = dial(mqtt_port).await;
    tokio::time::timeout(
        STEP,
        v5_sub.write_all(&sy02_connect5("rb-v5-sub", true, 60, None, None, 0)),
    )
    .await
    .expect("CONNECT in time")
    .expect("CONNECT send");
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut v5_sub))
        .await
        .expect("CONNACK in time")
        .expect("CONNACK");
    assert_eq!(head, 0x20);
    assert_eq!(body[1], 0, "v5 CONNECT accepted");
    tokio::time::timeout(
        STEP,
        v5_sub.write_all(&sy02_subscribe_v5(1, &[("rb/seven/out", 0)], 0)),
    )
    .await
    .expect("SUBSCRIBE in time")
    .expect("SUBSCRIBE send");
    let (head, _) = tokio::time::timeout(STEP, read_mqtt_packet(&mut v5_sub))
        .await
        .expect("SUBACK in time")
        .expect("SUBACK");
    assert_eq!(head & 0xF0, 0x90);
    let prop_value = nonce();
    let message = format!("{{\"props\":{{\"trace\":\"{prop_value}\"}},\"body\":1}}");
    publish(&mut pubr, "rb/seven", 0, message.as_bytes()).await;
    let (head, body) = tokio::time::timeout(STEP, read_mqtt_packet(&mut v5_sub))
        .await
        .expect("v5 downlink in time")
        .expect("v5 downlink");
    let parsed = sy02_parse_publish_v5(head, &body);
    assert_eq!(parsed.topic, "rb/seven/out");
    assert_eq!(parsed.expiry, 120);
    assert_eq!(parsed.users, vec![("trace".to_string(), prop_value)]);

    // 8. The console function writes the result to the log, on one line,
    // with a size limit.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-console",
            "SELECT * FROM \"rb/eight\"",
            serde_json::json!([{"function": "console"}]),
        )),
    )
    .await;
    assert_eq!(status, 201, "create answered {body}");
    let seen_value = nonce();
    publish(
        &mut pubr,
        "rb/eight",
        0,
        format!("{{\"seen\":\"{seen_value}\"}}").as_bytes(),
    )
    .await;
    let big_mark = nonce();
    let big = format!(
        "{{\"mark\":\"{big_mark}\",\"pad\":\"{}\"}}",
        "x".repeat(4000)
    );
    publish(&mut pubr, "rb/eight", 0, big.as_bytes()).await;
    let mut console_lines = Vec::new();
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let text = std::fs::read_to_string(&kernel_log).unwrap_or_default();
        console_lines = text
            .lines()
            .filter(|line| line.contains("rule console"))
            .map(str::to_string)
            .collect();
        if console_lines.iter().any(|l| l.contains(&seen_value))
            && console_lines.iter().any(|l| l.contains(&big_mark))
        {
            break;
        }
    }
    assert!(
        console_lines.iter().any(|l| l.contains(&seen_value)),
        "the console line holds the result: {console_lines:?}"
    );
    let big_line = console_lines
        .iter()
        .find(|l| l.contains(&big_mark))
        .expect("console line of the big result");
    assert!(
        !big_line.contains(&"x".repeat(2000)),
        "the console line is cut at the size limit"
    );
    assert!(big_line.contains("..."), "a cut line ends with dots");

    // 9. A user function is refused with 400 and nothing is stored.
    let (status, body) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::POST,
        "/api/v5/rules",
        Some(make_rule(
            "rb-user-fn",
            "SELECT * FROM \"rb/nine\"",
            serde_json::json!([{"function": "my_module:my_function", "args": {}}]),
        )),
    )
    .await;
    assert_eq!(status, 400, "user function answered {body}");
    assert_eq!(body["code"], "BAD_REQUEST");
    let (status, _) = api(
        &http,
        &base,
        api_key,
        reqwest::Method::GET,
        "/api/v5/rules/rb-user-fn",
        None,
    )
    .await;
    assert_eq!(status, 404, "the refused rule was not stored");

    let _ = child.kill().await;
    let _ = std::fs::remove_dir_all(&scratch);
    let _ = std::fs::remove_dir_all(&config_dir);
    let _ = std::fs::remove_file(&kernel_log);
}

/// SB-14: the rules API reads FROM with the SQL parser, merges PUT
/// fields, filters the list, and serves the rule events through HTTP.
#[tokio::test]
async fn sb14_rules_crud_topic_parsing_and_filters_through_api() {
    let kernel = spawn_kernel("").await;
    let create = |id: &str, sql: &str| serde_json::json!({"id": id, "sql": sql, "actions": []});

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "sb14-topic-a",
            "sql": r#"SELECT * FROM "sb14/topic/+""#,
            "description": "kitchen sensors",
            "metadata": {"owner": "sb14"},
        }),
    )
    .await;
    assert_eq!(status, 201, "quoted topic create: {body}");
    assert_eq!(body["from"], serde_json::json!(["sb14/topic/+"]));
    assert_eq!(body["name"], serde_json::json!(""));
    assert_eq!(body["description"], serde_json::json!("kitchen sensors"));
    assert_eq!(body["metadata"], serde_json::json!({"owner": "sb14"}));

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        create(
            "sb14-topic-event",
            r#"SELECT * FROM "$events/client_connected""#,
        ),
    )
    .await;
    assert_eq!(status, 201, "event source create: {body}");
    assert_eq!(
        body["from"],
        serde_json::json!(["$events/client_connected"])
    );

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": "sb14-topic-multi",
            "sql": r#"SELECT * FROM "sb14/multi/a", "sb14/multi/b""#,
            "actions": [{"function": "republish", "args": {"topic": "sb14/out/multi"}}],
        }),
    )
    .await;
    assert_eq!(status, 201, "multi-source create: {body}");
    assert_eq!(
        body["from"],
        serde_json::json!(["sb14/multi/a", "sb14/multi/b"])
    );
    let mut subscriber = mv00_connect(&kernel, "sb14-topic-multi-sub").await;
    send_mqtt(&mut subscriber, &sy02_subscribe(1, &[("sb14/out/#", 0)])).await;
    let (head, body) = recv_mqtt(&mut subscriber).await;
    assert_eq!(head, 0x90);
    assert_eq!(sy02_parse_suback(&body), (1, vec![0]));
    let mut publisher = mv00_connect(&kernel, "sb14-topic-multi-pub").await;
    for topic in ["sb14/multi/a", "sb14/multi/b"] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311(topic, 0, 0, false, br#"{"v":1}"#),
        )
        .await;
    }
    for _ in 0..2 {
        let (head, body) = recv_mqtt(&mut subscriber).await;
        assert_eq!(head & 0xf0, 0x30, "multi-source output is PUBLISH");
        let message = sy02_parse_publish_classic(head, &body);
        assert_eq!(message.topic, "sb14/out/multi");
    }

    for (id, sql) in [
        ("sb14-no-from", "SELECT 1"),
        ("sb14-bad-sql", "SELECT WHERE WHERE"),
        ("sb14-bad-topic", r#"SELECT * FROM "sb14/[bad""#),
    ] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::POST,
            "/api/v5/rules",
            create(id, sql),
        )
        .await;
        assert_eq!(status, 400, "invalid FROM {id}: {body}");
        assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
    }

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules?like_id=sb14-topic",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "like_id filter: {body}");
    let ids: Vec<&str> = body["data"]
        .as_array()
        .expect("rule list")
        .iter()
        .filter_map(|row| row["id"].as_str())
        .collect();
    assert_eq!(
        ids,
        vec!["sb14-topic-a", "sb14-topic-event", "sb14-topic-multi"]
    );

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules?like_description=kitchen",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "like_description filter: {body}");
    let ids: Vec<&str> = body["data"]
        .as_array()
        .expect("rule list")
        .iter()
        .filter_map(|row| row["id"].as_str())
        .collect();
    assert_eq!(ids, vec!["sb14-topic-a"]);

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules?from=sb14%2Fmulti%2Fa",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "from filter: {body}");
    assert!(body["data"]
        .as_array()
        .expect("rule list")
        .iter()
        .any(|row| row["id"] == "sb14-topic-multi"));

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/sb14-topic-a",
        serde_json::json!({"name": "renamed"}),
    )
    .await;
    assert_eq!(status, 200, "partial name update: {body}");
    assert_eq!(body["name"], serde_json::json!("renamed"));
    assert_eq!(
        body["sql"],
        serde_json::json!(r#"SELECT * FROM "sb14/topic/+""#)
    );
    assert_eq!(body["from"], serde_json::json!(["sb14/topic/+"]));

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/sb14-topic-a",
        serde_json::json!({"enable": false}),
    )
    .await;
    assert_eq!(status, 200, "partial enable update: {body}");
    assert_eq!(body["enable"], serde_json::json!(false));
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules?enable=false",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "enable filter: {body}");
    assert!(body["data"]
        .as_array()
        .expect("rule list")
        .iter()
        .any(|row| row["id"] == "sb14-topic-a"));

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/sb14-topic-a",
        serde_json::json!({"sql": "SELECT WHERE WHERE"}),
    )
    .await;
    assert_eq!(status, 400, "refused update: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules/sb14-topic-a",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(
        body["sql"],
        serde_json::json!(r#"SELECT * FROM "sb14/topic/+""#)
    );

    for query in ["page=0", "limit=0", "limit=10001"] {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/rules?{query}"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 400, "invalid page/limit {query}: {body}");
    }

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rule_events",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "rule events: {body}");
    assert!(body
        .as_array()
        .expect("rule event list")
        .iter()
        .any(|row| row["event"] == "$events/client_connected"));

    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rule_test",
        serde_json::json!({
            "sql": r#"SELECT * FROM "sb14/test""#,
            "context": {"event_type": "message_publish", "topic": "sb14/test", "payload": "{\"a\":1}"},
        }),
    )
    .await;
    assert_eq!(status, 200, "rule test match: {body}");
    assert_eq!(body["matched"], serde_json::json!(true));
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rule_test",
        serde_json::json!({"sql": r#"SELECT * FROM "sb14/test""#}),
    )
    .await;
    assert_eq!(status, 412, "rule test without topic: {body}");
    assert_eq!(body["code"], serde_json::json!("NOT_MATCH"));
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rule_test",
        serde_json::json!({"context": {"event_type": "message_publish", "topic": "t"}}),
    )
    .await;
    assert_eq!(status, 400, "rule test without sql");

    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::DELETE,
        "/api/v5/rules/sb14-topic-multi",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        "/api/v5/rules/sb14-topic-multi",
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 404);
    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        "/api/v5/rules/sb14-absent",
        serde_json::json!({"sql": r#"SELECT * FROM "sb14/absent""#}),
    )
    .await;
    assert_eq!(status, 404, "absent update");

    kernel.finish().await;
}

/// SB-14: rule counters come from the live engine. A filtered publish
/// counts a failure and a no-result. Reset zeroes every counter. A
/// disabled rule counts nothing. A multi-action rule counts each action.
#[tokio::test]
async fn sb14_rule_counters_and_reset_through_mqtt() {
    let kernel = spawn_kernel("").await;
    let id = "sb14-count";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": id,
            "sql": r#"SELECT * FROM "sb14/count/in" WHERE v = 1"#,
            "actions": [{"function": "republish", "args": {"topic": "sb14/count/out"}}],
        }),
    )
    .await;
    assert_eq!(status, 201, "counter rule create: {body}");
    let mut publisher = mv00_connect(&kernel, "sb14-count-pub").await;
    for payload in [r#"{"v":1}"#, r#"{"v":1}"#, r#"{"v":0}"#, "not-json"] {
        send_mqtt(
            &mut publisher,
            &sy02_publish311("sb14/count/in", 0, 0, false, payload.as_bytes()),
        )
        .await;
    }

    let mut latest = serde_json::Value::Null;
    for _ in 0..100 {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/rules/{id}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "counter read: {body}");
        latest = body.clone();
        if body["metrics"]["matched"] == serde_json::json!(4)
            && body["metrics"]["failed"] == serde_json::json!(2)
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(
        latest["metrics"]["matched"],
        serde_json::json!(4),
        "{latest}"
    );
    assert_eq!(
        latest["metrics"]["passed"],
        serde_json::json!(2),
        "{latest}"
    );
    assert_eq!(
        latest["metrics"]["failed"],
        serde_json::json!(2),
        "{latest}"
    );
    assert_eq!(
        latest["metrics"]["failed.no_result"],
        serde_json::json!(2),
        "{latest}"
    );
    assert_eq!(
        latest["metrics"]["actions.total"],
        serde_json::json!(2),
        "{latest}"
    );
    assert!(latest["node_metrics"][0]["node"]
        .as_str()
        .is_some_and(|node| node.contains('@')));

    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        &format!("/api/v5/rules/{id}/metrics/reset"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204, "counter reset");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    for counter in [
        "matched",
        "passed",
        "failed",
        "failed.no_result",
        "actions.total",
        "actions.success",
        "actions.failed",
    ] {
        assert_eq!(body["metrics"][counter], serde_json::json!(0), "{counter}");
    }

    let (status, _) = mv00_api(
        &kernel,
        reqwest::Method::PUT,
        &format!("/api/v5/rules/{id}"),
        serde_json::json!({"enable": false}),
    )
    .await;
    assert_eq!(status, 200, "disable rule");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sb14/count/in", 0, 0, false, br#"{"v":1}"#),
    )
    .await;
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::GET,
        &format!("/api/v5/rules/{id}/metrics"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(body["metrics"]["matched"], serde_json::json!(0));

    let multi_id = "sb14-count-multi";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        serde_json::json!({
            "id": multi_id,
            "sql": r#"SELECT * FROM "sb14/count/multi""#,
            "actions": [
                {"function": "republish", "args": {"topic": "sb14/count/one"}},
                {"function": "republish", "args": {"topic": "sb14/count/two"}},
            ],
        }),
    )
    .await;
    assert_eq!(status, 201, "multi-action rule create: {body}");
    send_mqtt(
        &mut publisher,
        &sy02_publish311("sb14/count/multi", 0, 0, false, br#"{"v":1}"#),
    )
    .await;
    let mut multi = serde_json::Value::Null;
    for _ in 0..100 {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            &format!("/api/v5/rules/{multi_id}/metrics"),
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200, "multi counter read: {body}");
        multi = body.clone();
        if body["metrics"]["actions.total"] == serde_json::json!(2) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(multi["metrics"]["matched"], serde_json::json!(1), "{multi}");
    assert_eq!(multi["metrics"]["passed"], serde_json::json!(1), "{multi}");
    assert_eq!(
        multi["metrics"]["actions.total"],
        serde_json::json!(2),
        "{multi}"
    );
    assert_eq!(
        multi["metrics"]["actions.success"],
        serde_json::json!(2),
        "{multi}"
    );
    kernel.finish().await;
}

/// A duplicate returns the reference code and keeps the live and stored rule.
#[tokio::test]
async fn mv00_api_error_duplicate_create_keeps_live_and_stored_rule() {
    let mut kernel = spawn_kernel("").await;
    let id = "duplicate-error";
    let sql = "SELECT b FROM \"mv00/in/duplicate-error\"";
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, sql),
    )
    .await;
    assert_eq!(status, 201, "create: {body}");
    let (status, body) = mv00_api(
        &kernel,
        reqwest::Method::POST,
        "/api/v5/rules",
        mv00_rule(id, "SELECT a FROM \"mv00/in/duplicate-error\""),
    )
    .await;
    assert_eq!(status, 400, "duplicate: {body}");
    assert!(body["message"].is_string());
    let duplicate_code = body["code"].as_str().expect("error code").to_string();
    for phase in 0..2 {
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/rules/duplicate-error",
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["sql"], sql, "duplicate kept the original SQL");
        let mut subscriber = mv00_connect(&kernel, "mv00-duplicate-sub").await;
        mv00_subscribe(&mut subscriber).await;
        let mut publisher = mv00_connect(&kernel, "mv00-duplicate-pub").await;
        mv00_assert_publish(
            &mut publisher,
            &mut subscriber,
            id,
            r#"{"a":1,"b":2}"#,
            &[r#"{"b":2}"#],
        )
        .await;
        drop(publisher);
        drop(subscriber);
        if phase == 0 {
            mv00_restart(&mut kernel).await;
        }
    }
    kernel.finish().await;
    assert_eq!(duplicate_code, "BAD_REQUEST");
}

/// An absent stored-rule probe returns the reference code and creates no state.
#[tokio::test]
async fn mv00_api_error_missing_probe_keeps_store_empty() {
    let mut kernel = spawn_kernel("").await;
    let mut codes = Vec::new();
    for phase in 0..2 {
        for sample in [
            serde_json::json!({"context": {
                "event_type": "message_publish",
                "topic": "mv00/in/missing",
                "payload": r#"{"b":2}"#,
            }}),
            serde_json::Value::Null,
        ] {
            let (status, body) = mv00_api(
                &kernel,
                reqwest::Method::POST,
                "/api/v5/rules/no-such-rule/test",
                sample,
            )
            .await;
            assert_eq!(status, 404, "absent rule: {body}");
            assert!(body["message"].is_string());
            codes.push(body["code"].as_str().expect("error code").to_string());
        }
        let (status, body) = mv00_api(
            &kernel,
            reqwest::Method::GET,
            "/api/v5/rules",
            serde_json::Value::Null,
        )
        .await;
        assert_eq!(status, 200);
        assert_eq!(body["data"], serde_json::json!([]));
        if phase == 0 {
            mv00_restart(&mut kernel).await;
        }
    }
    kernel.finish().await;
    assert!(
        codes.iter().all(|code| code == "RULE_NOT_FOUND"),
        "absent-rule probe codes: {codes:?}"
    );
}

// ---------------------------------------------------------------------------
// MV-01/SB-03: action-owned runtime sinks. One action is one dispatch key
// (`action:{id}`); the sink builds from the referenced connector's settings
// plus the action's own parameters, and the fire path crosses to it.
// ---------------------------------------------------------------------------

/// `$events/client_connected` is accepted as a rule source and evaluates via rule_test.
#[tokio::test]
async fn mv00_events_client_connected_rule_test() {
    let kernel = spawn_kernel("").await;
    let sql = "SELECT clientid, username FROM \"$events/client_connected\"";
    let (status, body) = mv00_api(&kernel, reqwest::Method::POST, "/api/v5/rule_test", serde_json::json!({
        "sql": sql,
        "context": {"event_type": "client_connected", "topic": "$events/client_connected", "payload": r#"{"clientid":"c1","username":"u1"}"#},
    })).await;
    assert_eq!(status, 200, "$events rule_test: {body}");
    assert_eq!(
        body["projected"].to_string(),
        r#"{"clientid":"c1","username":"u1"}"#
    );
    kernel.finish().await;
}
