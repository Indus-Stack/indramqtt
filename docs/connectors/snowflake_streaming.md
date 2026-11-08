---
title: "Snowflake streaming name"
description: "Use the documented snowflake_streaming action name."
sidebar:
  order: 110
---

# Snowflake streaming name

The `snowflake_streaming` name is a documented Snowflake action type. It is not a connector type. The v1 and v5 connectors routes reject it. The v5 actions route accepts it and stores the posted type.

Use it when a console sends the documented streaming name to the actions route. You get a stored action row with the Snowflake parameters.

```mermaid
flowchart LR
    Tool["console or API"] -- "POST /api/v5/actions" --> Act["action store"]
    Tool -- "POST /api/v1/connectors" --> V1["v1 connector route"]
    Tool -- "POST /api/v5/connectors" --> V5["v5 connector route"]
    V1 -- "unknown kind" --> Reject["400"]
    V5 -- "unknown type" --> Reject
    Act -- "type snowflake_streaming" --> Stored["stored action"]
```

## Before you start

- Read the [Snowflake connector](./snowflake.md) page for the Snowflake fields.
- Use the v5 actions route. Both connector routes reject this name.
- This name is not a connector type. No connector row is created.

## Settings

Create the action with `POST /api/v5/actions`.

| Setting | Required | Meaning |
|---|---|---|
| `type` | yes | Action type. Use `snowflake_streaming`. |
| `name` or `id` | yes | Action id. Its length is capped at 256 bytes. |
| `enable` | no | Enable flag. Default `true`. The status reads `connected` when enabled. |
| `parameters` | no | Object stored with the action. Default `{}`. The Snowflake fields are `account`, `user`, `database`, `schema`, `table`, `private_key_pem`, `endpoint`, and `timeout_ms`. |
| `resource_opts` | no | Object stored with the action. Default `{}`. |

The action stores its parameters without validation. The v5 connector route rejects the name with `unknown connector type`. The v1 connector route rejects the name with `unknown connector kind`.

The type catalogue lists `snowflake_streaming`. The stored row keeps the type `snowflake_streaming`.

Example:

```json
{
  "type": "snowflake_streaming",
  "name": "snowflake-streaming-1",
  "enable": true,
  "parameters": {
    "account": "xy12345.us-east-1",
    "user": "indra_loader",
    "database": "IOT",
    "schema": "PUBLIC",
    "table": "TELEMETRY",
    "private_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run proves the action create and both connector refusals. It sends no row. No Snowflake service ran in this task. See the record in [Verification failures](../_verification/failures.md).

## Check that it works

The test sends the name to both connector routes and to the actions route. The connector routes answer 400. The actions route answers 201 and stores the type.

```toml test
[node]
id = "docs-connector-snowflake-streaming"

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

def call(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            raw = res.read().decode()
            return res.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as err:
        raw = err.read().decode()
        try:
            return err.code, json.loads(raw)
        except ValueError:
            return err.code, raw

s1, b1 = call("POST", "/api/v1/connectors",
              {"id": "ss-v1", "kind": "snowflake_streaming",
               "config": {"account": "xy12345.us-east-1"}})
assert s1 == 400, (s1, b1)
assert "unknown connector kind" in json.dumps(b1).lower(), b1

s2, b2 = call("POST", "/api/v5/connectors",
              {"name": "ss-v5", "type": "snowflake_streaming",
               "account": "xy12345.us-east-1"})
assert s2 == 400, (s2, b2)
assert "unknown connector type" in json.dumps(b2).lower(), b2

s3, b3 = call("POST", "/api/v5/actions",
              {"type": "snowflake_streaming", "name": "ss-action",
               "parameters": {"account": "xy12345.us-east-1",
                              "user": "indra_loader",
                              "database": "IOT",
                              "schema": "PUBLIC",
                              "table": "TELEMETRY"}})
assert s3 == 201 and b3["type"] == "snowflake_streaming", (s3, b3)
assert b3["status"] == "connected", b3

s4, b4 = call("GET", "/api/v5/actions/ss-action")
assert s4 == 200 and b4["type"] == "snowflake_streaming", (s4, b4)
assert b4["parameters"]["table"] == "TELEMETRY", b4

s5, b5 = call("GET", "/api/v5/action_types")
assert s5 == 200 and "snowflake_streaming" in b5, (s5, b5)
print("snowflake_streaming")
PY
```

```output
snowflake_streaming
```

## Limits

- The v1 connectors route rejects this name with 400.
- The v5 connectors route rejects this name with 400.
- The v5 actions route accepts this name and stores the posted type.
- The action stores `parameters` without validation.
- The status reads `connected` from the enable flag. No probe runs.
- No connector row is created, so a rule forward to a connector id cannot use this name.
- The tier of the Snowflake runtime is enterprise.

## Next steps

- Read the Snowflake fields on the [Snowflake connector](./snowflake.md) page.
- Create a forwarding target with a [Connectors](./index.md) row.
- Feed the runtime from a rule in [Actions](../rules/actions.md).
