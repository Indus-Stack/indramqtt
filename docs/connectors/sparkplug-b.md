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
