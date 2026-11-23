---
title: "Sparkplug B connector"
description: "Publish rule results as Sparkplug B frames."
sidebar:
  order: 60
---

# Sparkplug B connector

The Sparkplug B connector publishes rule results as Sparkplug Protobuf frames to an MQTT server. The connector holds the server. The action holds an optional namespace guard. The runtime uses the maintained MQTT driver.

Use it when a Tahu host or edge node must receive telemetry. You get one publish per event with the exact topic.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["sparkplug sink"]
    Sink -- "PUBLISH frame" --> Srv["MQTT server"]
    Srv -- "PUBACK" --> Done["done"]
```

## Before you start

- Run an MQTT server. Note its `mqtt_url`.
- Pick the namespace guard, for example `spBv1.0/plant1`.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set `kind` to `sparkplug_b`. The aliases `sparkplug` and `spb` select the same runtime.

| Setting | Required | Meaning |
|---|---|---|
| `mqtt_url` | yes | MQTT server. Plain `mqtt://` only. Aliases `broker_address`, `server`, `endpoint`, `url` work. |
| `client_id` | no | MQTT client id. Default derives one per process. |
| `topic_prefix` | no, on the action | Namespace guard. Must start with `spBv1.0/` and hold no wildcard. |

Example:

```json
{
  "type": "sparkplug_b",
  "name": "spb-1",
  "mqtt_url": "mqtt://127.0.0.1:1883"
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket and publishes no frame. The send path needs an MQTT server. See the failure record in [Verification failures](../_verification/failures.md).

### `mqtt_url`

Target MQTT broker URL for Sparkplug B frames.

- Name: `mqtt_url`
- Type: text
- Range: URL starting with `mqtt://`
- Default: none (required by management API builder)
- Missing-value behaviour: returns HTTP status 400 naming `mqtt_url`
- Code reference: `crates/broker-connectors-enterprise/src/sparkplug_b.rs:1469, 1531-1543; crates/broker-api/src/v5/action_sinks.rs:3396-3402`

The setting specifies the destination broker URL. Only plain `mqtt://` is accepted. An `mqtts://` value returns HTTP status 400.

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
body = json.dumps({
    "name": "spb-mqtt-conn",
    "type": "sparkplug_b",
    "mqtt_url": "mqtt://127.0.0.1:1883",
}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors",
    data=body,
    method="POST",
    headers=headers,
)
with urllib.request.urlopen(req, timeout=5) as res:
    assert res.status == 201

req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors",
    headers={"Authorization": f"Bearer {key}"},
)
with urllib.request.urlopen(req, timeout=5) as res:
    connectors = json.load(res)
conn = next(c for c in connectors if c.get("name") == "spb-mqtt-conn")
print(conn["mqtt_url"])
PY
```

```output
mqtt://127.0.0.1:1883
```

### `client_id`

Client identifier for MQTT transport connections.

- Name: `client_id`
- Type: text
- Range: non-empty string
- Default: `indra-spb-<pid>` derived from process id
- Missing-value behaviour: defaults to `indra-spb-<pid>` when absent; returns HTTP status 400 when empty
- Code reference: `crates/broker-connectors-enterprise/src/sparkplug_b.rs:1334-1337, 1473, 1545-1550; crates/broker-api/src/v5/action_sinks.rs:3403-3410`

The setting specifies the client identifier. An absent value defaults to `indra-spb-<pid>`. An empty value returns HTTP status 400.

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
body = json.dumps({
    "name": "spb-client-conn",
    "type": "sparkplug_b",
    "mqtt_url": "mqtt://127.0.0.1:1883",
    "client_id": "spb-client-1",
}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors",
    data=body,
    method="POST",
    headers=headers,
)
with urllib.request.urlopen(req, timeout=5) as res:
    assert res.status == 201

req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors",
    headers={"Authorization": f"Bearer {key}"},
)
with urllib.request.urlopen(req, timeout=5) as res:
    connectors = json.load(res)
conn = next(c for c in connectors if c.get("name") == "spb-client-conn")
print(conn["client_id"])
PY
```

```output
spb-client-1
```

### `topic_prefix`

Namespace guard for Sparkplug B action topics.

- Name: `topic_prefix`
- Type: text
- Range: path starting with `spBv1.0/` without `+` or `#` and without empty segments
- Default: none (accepts every Sparkplug topic)
- Missing-value behaviour: an absent value accepts every topic; a topic outside the prefix returns an error
- Code reference: `crates/broker-connectors-enterprise/src/sparkplug_b.rs:1450, 1513-1524, 1682-1689; crates/broker-api/src/v5/action_sinks.rs:2017-2030, 3411-3420`

The setting specifies the namespace prefix on the action. It must start with `spBv1.0/`. It must contain no `+` or `#` characters and no empty segments. An absent value accepts every Sparkplug topic. A topic outside the prefix returns an error.

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
conn_body = json.dumps({
    "name": "spb-prefix-conn",
    "type": "sparkplug_b",
    "mqtt_url": "mqtt://127.0.0.1:1883",
}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors",
    data=conn_body,
    method="POST",
    headers=headers,
)
with urllib.request.urlopen(req, timeout=5) as res:
    assert res.status == 201

act_body = json.dumps({
    "name": "spb-prefix-act",
    "type": "sparkplug_b",
    "connector": "spb-prefix-conn",
    "parameters": {"topic_prefix": "spBv1.0/plant1"},
}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/actions",
    data=act_body,
    method="POST",
    headers=headers,
)
with urllib.request.urlopen(req, timeout=5) as res:
    assert res.status == 201

req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/actions",
    headers={"Authorization": f"Bearer {key}"},
)
with urllib.request.urlopen(req, timeout=5) as res:
    actions = json.load(res)
act = next(a for a in actions if a.get("name") == "spb-prefix-act")
print(act["parameters"]["topic_prefix"])
PY
```

```output
spBv1.0/plant1
```

## Check that it works

Create the connector. The broker answers 201 with the kind. The answer prints `ok`.

```toml test
[node]
id = "docs-connector-sparkplug-b"

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
body = json.dumps({"type": "sparkplug_b", "name": "spb-1",
                   "mqtt_url": "mqtt://127.0.0.1:1883"}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=5) as res:
    status, created = res.status, json.load(res)
assert status == 201, (status, created)
assert created["type"] in ("sparkplug_b", "sparkplug", "spb"), created
print("ok")
PY
```

```output
ok
```

## Limits

- A missing `mqtt_url` is refused with 400 and names the field.
- A `mqtts://` URL is refused. Terminate TLS in a sidecar proxy.
- A payload without a `metrics` object is refused on send and counts one failed forward.
- A topic outside `topic_prefix` is refused on send and counts one failed forward.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
