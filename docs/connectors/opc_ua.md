---
title: "OPC-UA connector"
description: "Write MQTT setpoints as OPC-UA node writes."
sidebar:
  order: 67
---

# OPC-UA connector

The OPC-UA connector writes each MQTT setpoint as one node write. It sends typed writes through the maintained driver. It buffers rows and flushes by count or by linger.

Use it when plant setpoints must reach an OPC-UA server. You get one write per row in each batch with your own node mapping.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["opc_ua sink"]
    Sink -- "batch rows" --> Trans["driver transport"]
    Trans -- "WriteRequest" --> Srv["OPC-UA server"]
    Trans -- "unknown node" --> Drop["fail closed"]
    Sink -- "queue full" --> Drop
    Sink -- "fault" --> Back["backoff retry"]
    Back -- "retry" --> Sink
```

## Before you start

- Run an OPC-UA server. Note the endpoint URL.
- Note one node id and one write topic.
- For a full build, enable the feature: `--features opc_ua`. The default build answers 400 and names the feature.

## Settings

Set `kind` to `opc_ua`. The broker also accepts the alias `opcua`. It stores the kind as `opc_ua`.

| Setting | Required | Meaning |
|---|---|---|
| `endpoint_url` | yes | Server URL. It must start with `opc.tcp://`. |
| `security_policy` | no | Policy name. Default `None`. Use `Basic256Sha256` or `Aes128Sha256RsaOaep` for signed paths. |
| `security_mode` | no | Mode name. Default `none`. `sign` and `signandencrypt` need a policy. |
| `auth` | no | Object with `type`. Default `anonymous`. Use `usernamepassword` with `username` and `password`. An empty username is refused. |
| `node_subscriptions` | yes | Non-empty list of node routes. Each route names one node and one write topic. |
| `node_id` | yes | Node address as `ns=<n>;<t>=<v>`. Type `i` is numeric, `s` is text, `g` is GUID, `b` is base64. Wildcards are refused. |
| `sampling_interval_ms` | yes | Poll interval in ms for the node. |
| `publish_topic_template` | yes | Topic template with `${node.sanitized_id}`. An empty value is refused. |
| `write_topic_pattern` | no | MQTT pattern with `+` and `#`. The first match wins. A topic with no match is refused. |
| `buffer_capacity` | no | Buffered rows. Default holds no bound. A full buffer fails closed. |
| `batch_size` | no | Rows per batch. Default 100. A value of 0 is refused. `null` means no count bound. |
| `linger_ms` | no | Linger before a partial batch flushes. Default 50. `null` means no time bound. |
| `timeout_ms` | no | Connect and request deadline in ms. Default 5000. A value below 1 reads as 1. |

Example:

```json
{
  "id": "opc-ua-1",
  "kind": "opc_ua",
  "config": {
    "endpoint_url": "opc.tcp://127.0.0.1:4840",
    "security_policy": "None",
    "security_mode": "none",
    "auth": {"type": "anonymous"},
    "node_subscriptions": [{"node_id": "ns=2;s=Line1.Temperature",
                            "sampling_interval_ms": 1000,
                            "publish_topic_template": "factory/line1/${node.sanitized_id}",
                            "write_topic_pattern": "factory/setpoint/+"}],
    "batch_size": 50,
    "linger_ms": 20,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket. The send path needs an OPC-UA server. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind when the build holds OPC-UA, else 400 naming the feature. Both answers print `ok`.

```toml test
[node]
id = "docs-connector-opc_ua"

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
import json, os, urllib.request, urllib.error
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
body = json.dumps({"id": "opc-ua-1", "kind": "opc_ua",
                   "config": {"endpoint_url": "opc.tcp://127.0.0.1:4840",
                              "security_policy": "None",
                              "security_mode": "none",
                              "auth": {"type": "anonymous"},
                              "node_subscriptions": [{"node_id": "ns=2;s=Line1.Temperature",
                                                      "sampling_interval_ms": 1000,
                                                      "publish_topic_template": "factory/line1/${node.sanitized_id}",
                                                      "write_topic_pattern": "factory/setpoint/+"}],
                              "batch_size": 50,
                              "linger_ms": 20,
                              "timeout_ms": 5000}}).encode()
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
    assert created["kind"] == "opc_ua", created
else:
    assert "opc_ua" in created["error"], created
print("ok")
PY
```

```output
ok
```

## Limits

- An `endpoint_url` without the `opc.tcp://` prefix is refused with 400.
- `sign` and `signandencrypt` without a policy are refused with 400.
- Certificate auth passes validation but the driver transport refuses it. It fails closed. Use anonymous or username with password.
- An empty `node_subscriptions` list is refused with 400.
- A `node_id` outside `ns=<n>;<t>=<v>` is refused. Type must be `i`, `s`, `g`, or `b`. A wildcard is refused.
- An empty `publish_topic_template` is refused with 400.
- A `batch_size` of 0 is refused with 400.
- A payload that is not UTF-8 or not JSON is refused. An array is refused as `BadTypeMismatch`. An object without a `value` key is refused as `BadTypeMismatch`, unless it holds one key. Then the sink uses the sole value.
- A whole number rides as `Int64`. A fraction rides as `Double`. Text rides as `Text`. A bool rides as `Boolean`.
- A topic with no matching `write_topic_pattern` is refused. The first match wins.
- The production path uses `DriverOpcUaTransport` through the maintained driver. The raw TCP path serves offline unit tests only.
- An unknown node fails as `BadNodeIdUnknown`. A bad value fails as `BadTypeMismatch`. Both are terminal. A lost session or channel is transient and reconnects.
- A failed flush keeps rows and engages backoff. Flushes fail fast during backoff.
- The tier is enterprise. The kind needs the `opc_ua` feature build.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
