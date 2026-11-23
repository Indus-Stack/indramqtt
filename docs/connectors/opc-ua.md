---
title: "OPC-UA connector"
description: "Write rule results to an OPC-UA server."
sidebar:
  order: 59
---

# OPC-UA connector

The OPC-UA connector writes rule results to OPC-UA nodes. The connector holds the server. Each action holds one node id. The runtime uses anonymous auth with no policy or mode.

Use it when a PLC or SCADA server must receive setpoints. You get one write per event to the named node.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["opc-ua sink"]
    Sink -- "write node" --> Srv["OPC-UA server"]
    Srv -- "BadTypeMismatch" --> Fail["failed forward"]
    Fail -- "log" --> Done["done"]
```

## Before you start

- Run an OPC-UA server. Note its `endpoint_url`.
- Pick the node id to write, for example `ns=1;i=1001`.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set `kind` to `opc_ua`. The alias `opcua` selects the same runtime.

| Setting | Required | Meaning |
|---|---|---|
| `endpoint_url` | yes | Server URL. Must start with `opc.tcp://`. Aliases `server`, `url`, `endpoint` work. |
| `node_id` | yes, on the action | Target node, for example `ns=1;i=1001`. |
| `timeout_ms` | no | Network connect / request timeout in ms. Default 5000. |

Example:

```json
{
  "type": "opc_ua",
  "name": "opc-1",
  "endpoint_url": "opc.tcp://127.0.0.1:4840"
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket and writes no node. The send path needs an OPC-UA server. See the failure record in [Verification failures](../_verification/failures.md).

### `endpoint_url`

Target OPC-UA server address.

- Name: `endpoint_url`
- Type: text
- Range: URI starting with `opc.tcp://`
- Default: none (required)
- Missing-value behaviour: returns HTTP status 400 with a message naming `endpoint_url`
- Code reference: `crates/broker-connectors-enterprise/src/opc_ua.rs:811, 851; crates/broker-api/src/v5/action_sinks.rs:3297-3308`

The setting specifies the server URL. It must start with `opc.tcp://`. An absent setting returns HTTP status 400 naming `endpoint_url`.

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
body = json.dumps({
    "name": "opc-endpoint-conn",
    "type": "opc_ua",
    "endpoint_url": "opc.tcp://127.0.0.1:4840",
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
conn = next(c for c in connectors if c.get("name") == "opc-endpoint-conn")
print(conn["endpoint_url"])
PY
```

```output
opc.tcp://127.0.0.1:4840
```

### `node_id`

Target OPC-UA node identifier for action writes.

- Name: `node_id`
- Type: text
- Range: valid OPC-UA node identifier, for example `ns=1;i=1001`
- Default: none (required action parameter in `NodeSubscriptionConfig` group)
- Missing-value behaviour: returns HTTP status 400 with message `action "<id>" is missing required field: node_id (field parameters.node_id)`
- Code reference: `crates/broker-connectors-enterprise/src/opc_ua.rs:795-797, 887; crates/broker-api/src/v5/action_sinks.rs:3329-3333`

The setting specifies the target node. It must parse as a valid node identifier. A missing setting returns HTTP status 400 naming `node_id`.

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
conn_body = json.dumps({
    "name": "opc-node-conn",
    "type": "opc_ua",
    "endpoint_url": "opc.tcp://127.0.0.1:4840",
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
    "name": "opc-node-act",
    "type": "opc_ua",
    "connector": "opc-node-conn",
    "parameters": {"node_id": "ns=1;i=1001"},
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
act = next(a for a in actions if a.get("name") == "opc-node-act")
print(act["parameters"]["node_id"])
PY
```

```output
ns=1;i=1001
```

### `timeout_ms`

Network connect / request timeout in ms.

- Name: `timeout_ms`
- Type: whole number of milliseconds
- Range: 0 or greater (0 is raised to 1 millisecond)
- Default: 5000 milliseconds
- Missing-value behaviour: defaults to 5000 milliseconds
- Code reference: `crates/broker-connectors-enterprise/src/opc_ua.rs:832, 834, 846-848`

The setting specifies the network connect and request timeout. An absent value defaults to 5000 milliseconds. A value of 0 is raised to 1 millisecond.

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
body = json.dumps({
    "name": "opc-timeout-conn",
    "type": "opc_ua",
    "endpoint_url": "opc.tcp://127.0.0.1:4840",
    "timeout_ms": 2500,
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
conn = next(c for c in connectors if c.get("name") == "opc-timeout-conn")
print(conn["timeout_ms"])
PY
```

```output
2500
```

## Check that it works

Create the connector. The broker answers 201 with the kind. The answer prints `ok`.

```toml test
[node]
id = "docs-connector-opc-ua"

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
import json, os, urllib.error, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
body = json.dumps({"type": "opc_ua", "name": "opc-1",
                   "endpoint_url": "opc.tcp://127.0.0.1:4840"}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=5) as res:
        status, created = res.status, json.load(res)
except urllib.error.HTTPError as err:
    status, created = err.code, json.load(err)
assert status == 201, (status, created)
assert created["type"] in ("opc_ua", "opcua"), created
print("ok")
PY
```

```output
ok
```

## Limits

- A missing `endpoint_url` is refused with 400 and names the field.
- A missing action `node_id` is refused with 400 and names the field.
- Only anonymous auth with no policy or mode is supported. Any other value is refused.
- A payload that is not JSON, or a value that does not fit the node type, counts one failed forward.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
