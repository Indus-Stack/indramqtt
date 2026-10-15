---
title: "Kafka connector"
description: "Send rule results to Apache Kafka."
sidebar:
  order: 51
---

# Kafka connector

The Kafka connector publishes rule results as Kafka records. It batches records and assigns partitions with murmur2. It waits for the configured `acks`.

Use it when an event pipeline or a stream processor reads broker data. You get batched records into a Kafka topic. The driver is the maintained Kafka client.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["kafka sink"]
    Sink -- "batch records" --> Prod["producer"]
    Prod -- "acks all" --> Topic["Kafka topic"]
```

## Before you start

- Run a Kafka cluster. Note its `bootstrap_servers`.
- Create the target topic, or allow auto-creation.
- For a full build, enable the feature: `--features kafka`. The default build answers 400 and names the feature.

## Settings

Set `kind` to `kafka`.

| Setting | Required | Meaning |
|---|---|---|
| `bootstrap_servers` | yes | Broker list, `host:port` pairs. |
| `topic_template` | yes | Target topic, with `${topic}` substitution. |
| `client_id` | yes | Producer client id. |
| `acks` | yes | `all`, `-1`, `1`, or `0`. |
| `partition_key_field` | no | Row field used as the record key. |
| `partitions` | no | Partition count hint, at least 1. |
| `batch_max_records` | no | Flush trigger record count. |
| `batch_max_bytes` | no | Flush trigger byte limit. |

Example:

```json
{
  "id": "kafka-sink-1",
  "kind": "kafka",
  "config": {
    "bootstrap_servers": "127.0.0.1:9092",
    "topic_template": "out-${topic}",
    "partition_key_field": "device_id",
    "partitions": 4,
    "client_id": "indra",
    "acks": "all"
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket and sends no record. The send path needs a Kafka cluster. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind when the build holds Kafka, else 400 naming the feature. Both answers print `ok`.

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
body = json.dumps({"id": "kafka-sink-1", "kind": "kafka",
                   "config": {"bootstrap_servers": "127.0.0.1:9092",
                              "topic_template": "out-${topic}",
                              "partition_key_field": "device_id",
                              "partitions": 4,
                              "client_id": "indra",
                              "acks": "all"}}).encode()
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

## Limits

- A wrong `acks` value is refused. Use `all`, `-1`, `1`, or `0`.
- `partitions`, `batch_max_records`, and `batch_max_bytes` must be at least 1.
- The connector runs behind the rule engine bounded queue. A slow cluster sheds load; it never stalls publishers.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
