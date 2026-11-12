---
title: "InfluxDB connector"
description: "Write rule results as InfluxDB metrics."
sidebar:
  order: 56
---

# InfluxDB connector

The InfluxDB connector writes each rule result as one line-protocol point. It buffers points and posts full batches to `/api/v2/write`. It authenticates with a token.

Use it when metrics must land in InfluxDB for dashboards. You get one HTTP post per batch with your own measurement mapping.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["influxdb sink"]
    Sink -- "batch points" --> Post["POST /api/v2/write"]
    Post -- "2xx" --> Ok["sent batches"]
    Post -- "error" --> Back["backoff retry"]
    Back -- "retry" --> Sink
    Sink -- "queue full" --> Drop["fail closed"]
```

## Before you start

- Run InfluxDB. Note the endpoint, org, bucket, and token.
- Pick the measurement template.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set `kind` to `influxdb`. The broker also accepts the alias `influx`.

| Setting | Required | Meaning |
|---|---|---|
| `endpoint` | yes | Base HTTP endpoint, `http://` or `https://`. The sink posts to `{endpoint}/api/v2/write`. |
| `bucket` | yes | Target bucket. An empty value is refused. |
| `org` | yes | Target organization. An empty value is refused. |
| `token` | yes | API token. The sink sends `Authorization: Token <token>`. An empty value is refused. |
| `measurement_template` | yes | Measurement name or `{topic}` template. An empty value is refused. Commas and spaces are escaped. |
| `precision` | yes | One of `s`, `ms`, `us`, `ns`. |
| `batch_size` | yes | Points per post. Must exceed 0. A full batch flushes at once. |
| `batch_timeout_ms` | yes | Linger before a partial batch flushes. |
| `request_timeout_ms` | no | Per-request timeout in ms. Default 5000. A value below 1 reads as 1. |

Example:

```json
{
  "id": "influx-sink-1",
  "kind": "influxdb",
  "config": {
    "endpoint": "http://127.0.0.1:8086",
    "bucket": "mqtt",
    "org": "indra",
    "token": "secret",
    "measurement_template": "mqtt_events",
    "precision": "ms",
    "batch_size": 100,
    "batch_timeout_ms": 50
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it posts nothing. The send path needs an InfluxDB server. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind.

```toml test
[node]
id = "docs-connector-influxdb"

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
body = json.dumps({"id": "influx-sink-1", "kind": "influxdb",
                   "config": {"endpoint": "http://127.0.0.1:8086",
                              "bucket": "mqtt", "org": "indra",
                              "token": "secret",
                              "measurement_template": "mqtt_events",
                              "precision": "ms", "batch_size": 100,
                              "batch_timeout_ms": 50}}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v1/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=5) as res:
    assert res.status == 201
    print(json.load(res)["kind"])
PY
```

```output
influxdb
```

## Limits

- A non-HTTP endpoint is refused with 400.
- An empty `bucket`, `org`, `token`, or `measurement_template` is refused with 400.
- A `precision` outside `s`, `ms`, `us`, `ns` is refused with 400.
- A `batch_size` of 0 is refused with 400.
- A non-UTF-8 payload is refused. A string field must hold UTF-8 text.
- An empty topic is refused.
- Each point carries the topic as tag and the payload as string field: `measurement,topic=<topic> qos=<qos>i,payload="<payload>" <timestamp>`.
- Tag keys escape `\`, `,`, `=`, and space. Field text escapes `\` and `"`.
- Timestamps rise strictly. The sink hands out increasing values, so rapid writes never share one timestamp.
- A failed post keeps buffered rows and engages backoff. Backoff waits 2s, then doubles to 30s. Flushes fail fast during backoff.
- The buffer holds 10000 rows at most. Past the bound the sink fails closed with a connection error. The bound keeps memory near 2 MiB behind the rule queue.
- A bad token fails closed as a dispatch error and keeps the row.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
