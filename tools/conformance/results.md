# Public MQTT conformance run

A real run of the Eclipse Paho interoperability suite against the
built broker. `tools/conformance/run.sh` writes this file.

- Date (UTC): 2026-10-17T19:39:16Z
- Broker binary: `/work/target/debug/indramqtt`
- Suite: `/work/ext/paho.mqtt.testing/interoperability`
- Suite commit: `9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103`
- Broker settings: `listeners.tcp` on 127.0.0.1:1883, `auth.allow_anonymous = true`, one deny ACL entry for `test/nosubscribe` and one allow-all ACL entry.
- Method: each test runs in its own process with a 90s timeout, so one hang cannot hide the other tests. The whole-file run is also attempted with a 90s timeout and reported below.

## Whole-file runs

- `client_test.py`: completed: 5 pass, 5 fail of 10 tests
- `client_test5.py`: stopped early (timed out after 90s); last test started: `test_flow_control2`

## Results

| File | Test | Result | How run | Assertion on failure | MQTT specification section |
|---|---|---|---|---|---|
| `client_test.py` | `testBasic` | pass | suite run |  | MQTT 3.1.1 §3.1–3.2, §3.8 (CONNECT, SUBSCRIBE, PUBLISH) |
| `client_test.py` | `test_dollar_topics` | fail | suite run | AssertionError: False != True | MQTT 3.1.1 §4.7.2 [MQTT-4.7.2-1] (wildcard and $ topics) |
| `client_test.py` | `test_keepalive` | pass | suite run |  | MQTT 3.1.1 §3.1.2.10 (Keep Alive) |
| `client_test.py` | `test_offline_message_queueing` | fail | suite run | AssertionError: False != True | MQTT 3.1.1 §3.1.2.4, §4.1 (persistent session queueing) |
| `client_test.py` | `test_overlapping_subscriptions` | pass | suite run |  | MQTT 3.1.1 §3.8.4 (overlapping subscriptions) |
| `client_test.py` | `test_redelivery_on_reconnect` | fail | suite run | AssertionError: False != True | MQTT 3.1.1 §4.4 (message delivery retry) |
| `client_test.py` | `test_retained_messages` | pass | suite run |  | MQTT 3.1.1 §3.3.1.3 (RETAIN) |
| `client_test.py` | `test_subscribe_failure` | fail | suite run | AssertionError: False != True | MQTT 3.1.1 §3.9.3 [MQTT-3.9.3-2] (SUBACK return code 0x80) |
| `client_test.py` | `test_unsubscribe` | pass | suite run |  | MQTT 3.1.1 §3.10 (UNSUBSCRIBE, UNSUBACK) |
| `client_test.py` | `test_zero_length_clientid` | fail | suite run | AssertionError: False != True | MQTT 3.1.1 §3.1.3.1 [MQTT-3.1.3-6] (zero-length ClientId) |
| `client_test5.py` | `test_assigned_clientid` | error | isolated | AttributeError: 'Properties' object has no attribute 'AssignedClientIdentifier' | MQTT 5.0 §3.1.3.1.3 (Assigned Client Identifier) |
| `client_test5.py` | `test_basic` | pass | isolated |  | MQTT 5.0 §3.1–3.2, §3.8 |
| `client_test5.py` | `test_client_topic_alias` | fail | isolated | AssertionError: 0 != 1 : [] | MQTT 5.0 §3.3.2.3.4 (Topic Alias) |
| `client_test5.py` | `test_dollar_topics` | fail | isolated | AssertionError: False != True | MQTT 5.0 §4.7.2 (wildcard and $ topics) |
| `client_test5.py` | `test_flow_control1` | error | isolated | TimeoutError: timed out | MQTT 5.0 §4.9 (Receive Maximum, flow control) |
| `client_test5.py` | `test_flow_control2` | fail | isolated | test did not finish within 90s (hang) | MQTT 5.0 §4.9 [MQTT-4.9.0-1] (Receive Maximum exceeded) |
| `client_test5.py` | `test_keepalive` | pass | isolated |  | MQTT 5.0 §3.1.2.10 (Keep Alive) |
| `client_test5.py` | `test_maximum_packet_size` | pass | isolated |  | MQTT 5.0 §3.1.2.11.4 (Maximum Packet Size) |
| `client_test5.py` | `test_offline_message_queueing` | pass | isolated |  | MQTT 5.0 §4.1 (session state queueing) |
| `client_test5.py` | `test_overlapping_subscriptions` | pass | isolated |  | MQTT 5.0 §3.8.4 (overlapping subscriptions) |
| `client_test5.py` | `test_payload_format` | error | isolated | AttributeError: 'Properties' object has no attribute 'ContentType' | MQTT 5.0 §3.3.2.3.2 (Payload Format Indicator) |
| `client_test5.py` | `test_publication_expiry` | fail | isolated | AssertionError: 4 != 2 : [('TopicA', b'qos 1 - expire', 1, False, 1, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8786690>), ('TopicA', b"qos 1 - don't expire", 1, False, 2, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8786790>), ('TopicA', b'qos 2 - expire', 2, False, 3, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8786490>), ('TopicA', b"qos 2 - don't expire", 2, False, 4, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8785a50>)] | MQTT 5.0 §3.3.2.3.3 (Message Expiry Interval) |
| `client_test5.py` | `test_redelivery_on_reconnect` | pass | isolated |  | MQTT 5.0 §4.4 (message delivery retry) |
| `client_test5.py` | `test_request_response` | error | isolated | AttributeError: 'Properties' object has no attribute 'ResponseTopic' | MQTT 5.0 §3.3.2.3.5–3.6 (Response Topic, Correlation Data) |
| `client_test5.py` | `test_retained_message` | error | isolated | AttributeError: 'Properties' object has no attribute 'UserProperty' | MQTT 5.0 §3.3.1.3 (RETAIN) |
| `client_test5.py` | `test_server_keep_alive` | fail | isolated | AssertionError: False is not true | MQTT 5.0 §3.2.2.3.14 (Server Keep Alive) |
| `client_test5.py` | `test_server_topic_alias` | fail | isolated | AssertionError: 'TopicA' != '' | MQTT 5.0 §3.3.2.3.4 (Topic Alias) |
| `client_test5.py` | `test_session_expiry` | fail | isolated | AssertionError: False != True | MQTT 5.0 §3.1.2.11.2 (Session Expiry Interval) |
| `client_test5.py` | `test_shared_subscriptions` | pass | isolated |  | MQTT 5.0 §4.8.2 (Shared Subscriptions) |
| `client_test5.py` | `test_subscribe_failure` | fail | isolated | AssertionError: False != True | MQTT 5.0 §3.9.3 (SUBACK reason code) |
| `client_test5.py` | `test_subscribe_identifiers` | fail | isolated | AssertionError: 2 != 1 : [('TopicA', b'sub identifier test', 1, False, 1, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x70b83cb67f10>), ('TopicA', b'sub identifier test', 1, False, 2, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x70b83cb5e050>)] | MQTT 5.0 §3.3.2.3.8 (Subscription Identifier) |
| `client_test5.py` | `test_subscribe_options` | pass | isolated |  | MQTT 5.0 §3.8.3.1 (Subscription Options) |
| `client_test5.py` | `test_unsubscribe` | pass | isolated |  | MQTT 5.0 §3.10 (UNSUBSCRIBE, UNSUBACK) |
| `client_test5.py` | `test_user_properties` | error | isolated | AttributeError: 'Properties' object has no attribute 'UserProperty' | MQTT 5.0 §3.3.2.3.7 (User Property) |
| `client_test5.py` | `test_will_delay` | fail | isolated | AssertionError: 0.10013508796691895 != 4 within 1 delta (3.899864912033081 difference) | MQTT 5.0 §3.1.3.2.2 (Will Delay Interval) |
| `client_test5.py` | `test_will_message` | error | isolated | AttributeError: 'Properties' object has no attribute 'UserProperty' | MQTT 5.0 §3.1.2.5 (Will Message) |
| `client_test5.py` | `test_zero_length_clientid` | pass | isolated |  | MQTT 5.0 §3.1.3.1 (Client Identifier) |

## Totals

- `client_test.py`: 5 pass, 5 fail of 10 tests.
- `client_test5.py`: 10 pass, 17 fail of 27 tests.
- Both files: 15 pass, 22 fail of 37 tests.

## Failure analysis

- `client_test.py` / `test_dollar_topics`: AssertionError: False != True
  - The broker is wrong. [MQTT-4.7.2-1] forbids a match between a filter that starts with a wildcard and a topic that starts with `$`. The broker delivered `$TopicA/B` to the filter `+/+`. Correct the router match. Class: large.
- `client_test.py` / `test_offline_message_queueing`: AssertionError: False != True
  - The broker is wrong. MQTT 3.1.1 §3.1.2.4 and §4.1 require a persistent session to keep its subscriptions and to queue QoS 1 and QoS 2 messages. The broker queued nothing. Class: large.
- `client_test.py` / `test_redelivery_on_reconnect`: AssertionError: False != True
  - The broker is wrong. MQTT 3.1.1 §4.4 requires a resend of unacknowledged QoS 1 and QoS 2 messages on reconnect. The broker resent nothing. Class: large.
- `client_test.py` / `test_subscribe_failure`: AssertionError: False != True
  - The broker was wrong. [MQTT-3.9.3-2] allows only 0, 1, 2 and 0x80 in a version 3 SUBACK. The broker sent 0x87. Corrected in this task: a version 3 denial answers 0x80, with a socket regression test. This recorded row predates the fix.
- `client_test.py` / `test_zero_length_clientid`: AssertionError: False != True
  - The broker is wrong. [MQTT-3.1.3-6] and [MQTT-3.1.3-7] require a reject with CONNACK code 0x02 for a zero-length ClientId with CleanSession 0. The broker accepted it. Class: medium.
- `client_test5.py` / `test_assigned_clientid`: AttributeError: 'Properties' object has no attribute 'AssignedClientIdentifier'
  - The broker is wrong. MQTT 5.0 §3.1.3.1.3 requires an Assigned Client Identifier in CONNACK when the server accepts an empty ClientId. The broker sent none. Class: medium.
- `client_test5.py` / `test_client_topic_alias`: AssertionError: 0 != 1 : []
  - The broker is wrong. MQTT 5.0 §3.3.2.3.4 makes a Topic Alias of 0 and an unmapped alias a protocol error. The broker ignored the alias and did not disconnect. Class: medium.
- `client_test5.py` / `test_dollar_topics`: AssertionError: False != True
  - The broker is wrong. MQTT 5.0 §4.7.2 forbids a match between a wildcard filter and a `$` topic. The broker delivered `$TopicA/B` to the filter `+/+`. Class: large.
- `client_test5.py` / `test_flow_control1`: TimeoutError: timed out
  - The broker is wrong. MQTT 5.0 §4.9 requires the release of a held message after the client acknowledges one. The broker never released the held publish. Class: medium.
- `client_test5.py` / `test_flow_control2`: test did not finish within 90s (hang)
  - The broker is wrong. MQTT 5.0 §4.9 requires a DISCONNECT with reason 0x93 when the client exceeds the Receive Maximum. The broker kept the connection. Class: medium.
- `client_test5.py` / `test_payload_format`: AttributeError: 'Properties' object has no attribute 'ContentType'
  - The broker is wrong. MQTT 5.0 §3.3.2.3.2 and §3.3.2.3.9 require the Payload Format Indicator and the Content Type on delivery. The broker forwarded neither. Class: medium.
- `client_test5.py` / `test_publication_expiry`: AssertionError: 4 != 2 : [('TopicA', b'qos 1 - expire', 1, False, 1, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8786690>), ('TopicA', b"qos 1 - don't expire", 1, False, 2, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8786790>), ('TopicA', b'qos 2 - expire', 2, False, 3, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8786490>), ('TopicA', b"qos 2 - don't expire", 2, False, 4, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x7ebbd8785a50>)]
  - The broker is wrong. MQTT 5.0 §3.3.2.3.3 requires the drop of a queued message whose Message Expiry Interval passed. The broker delivered all four. Class: medium.
- `client_test5.py` / `test_request_response`: AttributeError: 'Properties' object has no attribute 'ResponseTopic'
  - The broker is wrong. MQTT 5.0 §3.3.2.3.5 and §3.3.2.3.6 require the Response Topic and the Correlation Data on delivery. The broker forwarded neither. Class: medium.
- `client_test5.py` / `test_retained_message`: AttributeError: 'Properties' object has no attribute 'UserProperty'
  - The broker is wrong. MQTT 5.0 §3.3.2.3.7 requires the User Properties on a retained message. The broker lost them. Class: medium.
- `client_test5.py` / `test_server_keep_alive`: AssertionError: False is not true
  - The broker is right. MQTT 5.0 §3.2.2.3.14 makes the Server Keep Alive an optional CONNACK property. The broker accepted the client's Keep Alive of 120 and sent no property, which the specification permits. The suite expects its own test broker to announce 60. No correction.
- `client_test5.py` / `test_server_topic_alias`: AssertionError: 'TopicA' != ''
  - The broker is wrong. MQTT 5.0 §3.3.2.3.4 lets the server use outbound topic aliases up to the client maximum. The broker sent the full topic each time. Class: medium.
- `client_test5.py` / `test_session_expiry`: AssertionError: False != True
  - The broker is wrong. MQTT 5.0 §3.1.2.11.2 requires the Session Expiry Interval and its DISCONNECT override. The session-present flag was wrong. Class: medium.
- `client_test5.py` / `test_subscribe_failure`: AssertionError: False != True
  - The broker differs. The suite expects 0x80; the broker sent 0x87. Both are legal in version 5, but the interoperability target uses 0x80. Send 0x80. Class: small.
- `client_test5.py` / `test_subscribe_identifiers`: AssertionError: 2 != 1 : [('TopicA', b'sub identifier test', 1, False, 1, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x70b83cb67f10>), ('TopicA', b'sub identifier test', 1, False, 2, <mqtt.formats.MQTTV5.MQTTV5.Properties object at 0x70b83cb5e050>)]
  - The broker differs. MQTT 5.0 §3.8.4 permits one copy per matching subscription; the suite expects one copy with every Subscription Identifier. The broker sent two copies. Coalesce the overlap. Class: medium.
- `client_test5.py` / `test_user_properties`: AttributeError: 'Properties' object has no attribute 'UserProperty'
  - The broker is wrong. MQTT 5.0 §3.3.2.3.7 requires the User Properties on delivery. The broker forwarded none. Class: medium.
- `client_test5.py` / `test_will_delay`: AssertionError: 0.10013508796691895 != 4 within 1 delta (3.899864912033081 difference)
  - The broker is wrong. MQTT 5.0 §3.1.3.2.2 requires a delay of the will by the Will Delay Interval. The broker sent the will at once. Class: medium.
- `client_test5.py` / `test_will_message`: AttributeError: 'Properties' object has no attribute 'UserProperty'
  - The broker is wrong. MQTT 5.0 §3.1.2.5 requires the User Properties of the will. The broker sent none. Class: medium.
