---
title: "Kafka connector"
description: "Send rule results to Apache Kafka."
sidebar:
  order: 51
---

# Kafka connector

The Kafka connector publishes rule results as Kafka records. The action names the topic, the key, the headers, and the strategy. The driver waits for the configured ack.

Use it when an event pipeline or a stream processor reads broker data. You get keyed records into a Kafka topic.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["kafka sink"]
    Sink -- "key + headers" --> Prod["producer"]
    Prod -- "ack" --> Topic["Kafka topic"]
```

## Before you start

- Run a Kafka cluster. Note its `bootstrap_hosts`.
- Create the target topic, or allow auto-creation.
- For a full build, enable the feature: `--features kafka`. The default build answers 400 and names the feature.

## Settings

Set `type` to `kafka` (`kafka_producer` is accepted too).

Connector settings:

| Setting | Required | Meaning |
|---|---|---|
| `bootstrap_hosts` | yes | Broker list, `host:port` pairs. |
| `authentication` | no | Only `none` is supported. SASL is refused. |

Action settings:

| Setting | Required | Meaning |
|---|---|---|
| `topic` | yes | Target topic template, `${topic}` supported. |
| `message.key` | no | Record key template (`${topic}`, `${payload}`, `${payload.<field>}`, `${client_id}`). Absent means a null key. |
| `partition_strategy` | no | `random` (default) or `key_dispatch` (sticky murmur2 hash). |
| `required_acks` | no | `all_isr` (default), `leader_only`, or `none`. |
| `kafka_headers` | no | One template rendered to a JSON object or array of key/value pairs. |
| `kafka_ext_headers` | no | Array of `{kafka_ext_header_key, kafka_ext_header_value}` templates. |
| `kafka_header_value_encode_mode` | no | `none` (default) or `json`. |
| `query_mode` | no | `async` (default) or `sync`. Delivery always awaits the broker ack. |

Example:

```json
{
  "type": "kafka",
  "name": "kafka-out",
  "bootstrap_hosts": "127.0.0.1:9092"
}
```

```json
{
  "type": "kafka",
  "name": "kafka-act",
  "connector": "kafka-out",
  "parameters": {
    "topic": "events",
    "message": {"key": "${payload.device_id}"},
    "partition_strategy": "key_dispatch",
    "required_acks": "all_isr"
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket and sends no record. The send path needs a Kafka cluster.

## Check that it works

Create the connector. The broker answers 201 with the kind when the build holds Kafka, else 400 naming the feature. Both answers print `ok`. A body without hosts answers 400 naming the field.

```toml test
[node]
id = "docs-connector-kafka"

[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.api]
bind = "0.0.0.0:18083"

[logging]
level = "warn"

[auth]
allow_anonymous = true
```

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
def post(body):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v5/connectors", data=json.dumps(body).encode(), method="POST",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as err:
        return err.code, json.load(err)
status, created = post({"name": "kafka-run-ok", "type": "kafka",
                        "bootstrap_hosts": "127.0.0.1:9092"})
assert status in (201, 400), (status, created)
if status == 201:
    assert created.get("type") == "kafka_producer", created
else:
    assert "feature" in str(created).lower(), created
status, body = post({"name": "kafka-run-bad", "type": "kafka"})
assert status == 400 and "bootstrap_hosts" in body["message"], body
print("ok")
PY
```

```output
ok
```

## Limits

- SASL is refused with 400. The driver dials plaintext with no SASL wiring yet.
- `message.value` supports only the whole message. A custom value is refused.
- Producer buffer tuning belongs to `resource_opts`. A present value is refused.

## Next steps

- Feed the action from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).

## Reference form (v1 API)

The reference form uses the v1 connector API. The v5 form above uses connector and action documents. This section keeps the reference page.

The Kafka connector publishes each rule result as one Kafka record. It batches records and places them with murmur2. It waits for the configured `acks`.

Use it when an event pipeline or a stream processor reads broker data. You get batched records in a Kafka topic through the maintained driver.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["kafka sink"]
    Sink -- "batch records" --> Prod["driver producer"]
    Prod -- "acks all" --> Topic["Kafka topic"]
    Prod -- "retryable" --> Back["backoff retry"]
    Back -- "retry" --> Prod
    Prod -- "terminal" --> Drop["fail closed"]
```

### Before you start

- Run a Kafka cluster. Note its `bootstrap_servers`.
- Create the target topic with 6 partitions, or allow auto-creation.
- For a full build, enable the feature: `--features kafka`. The default build answers 400 and names the feature.

### Settings

Set `kind` to `kafka`.

| Setting | Required | Meaning |
|---|---|---|
| `bootstrap_servers` | yes | Broker list, `host:port` pairs. An empty value is refused. |
| `topic_template` | yes | Target topic with `${topic}` substitution. An empty value is refused. `/` becomes `.`. Other chars outside `[A-Za-z0-9._-]` become `_`. The result truncates to 249 chars. An empty result or `.` or `..` becomes `indramqtt-invalid-topic`. |
| `client_id` | yes | Producer client id. The driver sends it as `client.id`. |
| `acks` | yes | `all`, `-1`, `1`, or `0`. Other values are refused. `all` and `-1` request a quorum. |
| `partition_key_field` | no | Top-level JSON field used as the record key. Default is none. A missing field or a non-JSON payload means a null key. A null key lands on partition 0. |
| `partitions` | no | Partition count hint for murmur2 modulo. Default 1. Must exceed 0. |
| `batch_max_records` | no | Flush trigger record count. Default 500. Must exceed 0. The default keeps the rule worker small. |
| `batch_max_bytes` | no | Flush trigger byte limit. Default 262144. Must exceed 0. The default keeps the rule worker small. Bytes count value plus key. |

Example:

```json
{
  "id": "kafka-sink-1",
  "kind": "kafka",
  "config": {
    "bootstrap_servers": "127.0.0.1:9092",
    "topic_template": "out-${topic}",
    "client_id": "indra",
    "acks": "all",
    "partition_key_field": "device_id",
    "partitions": 6,
    "batch_max_records": 500,
    "batch_max_bytes": 262144
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket and sends no record. The send path needs a Kafka cluster. See the failure record in [Verification failures](../_verification/failures.md).

### Check that it works

Create the connector. The broker answers 201 with the kind when the build holds Kafka, else 400 naming the feature. Both answers print `ok`.

```toml test
# Reference form run.
[node]
id = "docs-connector-kafka"

[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.api]
bind = "0.0.0.0:18083"

[logging]
level = "warn"

[auth]
allow_anonymous = true
```

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
body = json.dumps({"id": "kafka-sink-1", "kind": "kafka",
                   "config": {"bootstrap_servers": "127.0.0.1:9092",
                              "topic_template": "out-${topic}",
                              "client_id": "indra",
                              "acks": "all",
                              "partition_key_field": "device_id",
                              "partitions": 6,
                              "batch_max_records": 500,
                              "batch_max_bytes": 262144}}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v1/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=5) as res:
        status, created = res.status, json.load(res)
except urllib.error.HTTPError as err:
    status, created = err.code, json.load(err)
assert status in (201, 400), (status, created)
if status == 201:
    assert created["kind"] == "kafka", created
else:
    assert "kafka" in created["error"], created
print("ok")
PY
```

```output
ok
```

### Limits

- An empty `bootstrap_servers` or `topic_template` is refused with 400.
- A wrong `acks` value is refused. Use `all`, `-1`, `1`, or `0`.
- `partitions`, `batch_max_records`, and `batch_max_bytes` must exceed 0.
- A string key sends raw bytes. A number or bool sends its JSON spelling. An object sends its JSON form.
- Each record carries headers `mqtt.topic`, `mqtt.qos`, and `mqtt.timestamp`.
- The driver sets `message.timeout.ms` to 30000 and `socket.timeout.ms` to 30000. Idempotence turns on only with quorum `acks`.
- A flush retries a retryable fault up to 200 times. Delays run from 100 ms to 1000 ms. Terminal faults fail fast: auth, SSL, and oversize records.
- A failed flush restores rows and engages backoff. Flushes fail fast during backoff.
- The connector runs behind the rule engine bounded queue. A slow cluster sheds load; it never stalls publishers.
- The tier is community. The kind needs the `kafka` feature build.

### Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
