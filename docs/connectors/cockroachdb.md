---
title: "CockroachDB connector"
description: "Write rule results as CockroachDB rows."
sidebar:
  order: 64
---

# CockroachDB connector

The CockroachDB connector runs one statement per batch. It speaks the PostgreSQL wire protocol with the maintained driver, including bound parameters. It pools one session per `pool_size` entry.

Use it when CockroachDB must hold telemetry. You get one parse per batch with your own statement.

```mermaid
flowchart LR
    Rule["rule engine"] -- "action id" --> Sink["cockroachdb sink"]
    Sink -- "batch rows" --> Prep["Parse Bind Execute"]
    Prep -- "bound params" --> Tbl["target table"]
```

## Before you start

- Run CockroachDB. Note the host, database, and user.
- Create the target table. Match its columns to the statement.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set connector `type` to `cockroachdb` (`cockroach` alias works too).

| Setting | Required | Meaning |
|---|---|---|
| `server` | yes | `Host[:Port]`. Default port is 26257. |
| `database` | yes | Database name. |
| `username` | yes | Database user. |
| `password` | no | Database password. Accepts `file:` and `env:` references. |
| `pool_size` | no | Server sessions. Default is 8. Range is 1 to 1024. |
| `ssl` | no | `{"enable": true}` uses TLS with the system roots. `verify_none` is refused. |
| `disable_prepared_statements` | no | Only `false` is accepted. Every send prepares. |
| `application_name` | no | Must be absent. The runtime never stamps it. |

Set action `type` to `cockroachdb` and `parameters.sql` to the statement.

| Marker | Value |
|---|---|
| `$1` | Topic. |
| `$2` | QoS. |
| `$3` | Payload. |

The statement must name at least one marker from `$1` to `$3`. Values bind out of band. A quote in a value never escapes into the text.

Example:

```json
{
  "name": "crdb-sink-1",
  "type": "cockroachdb",
  "connector": "crdb-conn-1",
  "parameters": {
    "sql": "INSERT INTO t_mqtt_msg(topic, qos, payload) VALUES ($1, $2, $3)"
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it touches no database. The send path needs a CockroachDB cluster.

## Check that it works

Create the connector and the action. The broker answers 201 for each.

```toml test
[node]
id = "docs-connector-cockroachdb"

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
def call(method, path, body=None):
    import urllib.error
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            assert res.status in (200, 201, 204), res.status
            return json.load(res) if res.status != 204 else {}
    except urllib.error.HTTPError as e:
        assert e.code == 404, e.code
        return {}
call("DELETE", "/api/v5/actions/crdb-sink-1")
call("DELETE", "/api/v5/connectors/crdb-conn-1")
conn = call("POST", "/api/v5/connectors", {"name": "crdb-conn-1", "type": "cockroachdb",
    "server": "127.0.0.1:26257", "database": "db", "username": "u"})
assert conn["type"] == "cockroachdb", conn
act = call("POST", "/api/v5/actions", {"name": "crdb-sink-1", "type": "cockroachdb",
    "connector": "crdb-conn-1",
    "parameters": {"sql": "INSERT INTO t(topic) VALUES ($1)"}})
print(act["status"])
PY
```

```output
disconnected
```

## Limits

- A missing `server`, `database`, or `username` is refused with 400 and names the field.
- A missing `parameters.sql` is refused with 400.
- `ssl.verify_none` and custom TLS files are refused with 400.
- A dead server counts each fire as failed. The broker keeps serving.

## Next steps

- Feed the action from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
