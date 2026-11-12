---
title: "GreptimeDB connector"
description: "Write rule results as GreptimeDB rows."
sidebar:
  order: 64
---

# GreptimeDB connector

The GreptimeDB connector writes rule results as table rows. It posts SQL inserts or line protocol. It buffers rows and flushes by count or by linger.

Use it when time-series rows must land in GreptimeDB. You get one insert per flush.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["greptimedb sink"]
    Sink -- "buffer rows" --> Flush["flush by count or linger"]
    Flush -- "SQL or influx" --> Tbl["table"]
```

## Before you start

- Run GreptimeDB. Note the server, database, and table.

## Settings

Set `kind` to `greptimedb`. The alias `greptime` selects the same runtime.

| Setting | Required | Meaning |
|---|---|---|
| `server` | no | Server `host:port`. Default `127.0.0.1:4001`. A bare host dials port 8086. |
| `database` | yes | Target database. |
| `table` | yes | Target table template. |
| `username` | no | GreptimeDB user. Needs `password` too. |
| `password` | no | GreptimeDB password. Needs `username` too. |
| `precision` | no | `s`, `ms`, `us` or `ns`. Default `ms`. |
| `format` | no | `sql` or `influx`. Default `sql`. |

The action may override `table`, `precision` and `format` per target.

### `server`

Server `host:port`. Type: string. Required: no. Default `127.0.0.1:4001`. A bare host dials port 8086.

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
status, created = post({"name": "greptime-server-ok", "type": "greptimedb",
                        "database": "public", "table": "events"})
assert status == 201, (status, created)
print("ok")
PY
```

```output
ok
```

### `database`

Target database. Type: string. Required: yes. Absent: 400 naming `database`.

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
status, body = post({"name": "greptime-db-bad", "type": "greptimedb",
                     "server": "127.0.0.1:4001"})
assert status == 400 and "database" in body["message"], body
print("ok")
PY
```

```output
ok
```

### `table`

Target table template. Type: string. Required: yes. Absent: 400 naming `table`.

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
status, body = post({"name": "greptime-table-bad", "type": "greptimedb",
                     "server": "127.0.0.1:4001", "database": "public"})
assert status == 400 and "table" in body["message"], body
print("ok")
PY
```

```output
ok
```

### `username`

GreptimeDB user. Type: string. Required: no. Needs `password` too. A half pair is 400.

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
status, body = post({"name": "greptime-user-bad", "type": "greptimedb",
                     "server": "127.0.0.1:4001", "database": "public", "table": "events",
                     "username": "u"})
assert status == 400 and "username" in body["message"], body
print("ok")
PY
```

```output
ok
```

### `password`

GreptimeDB password. Type: string. Required: no. Needs `username` too. A half pair is 400.

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
status, created = post({"name": "greptime-pw-ok", "type": "greptimedb",
                        "server": "127.0.0.1:4001", "database": "public", "table": "events",
                        "username": "u", "password": "p"})
assert status == 201, (status, created)
print("ok")
PY
```

```output
ok
```

### `precision`

Timestamp precision. Type: string. Required: no. Allowed: `s`, `ms`, `us`, `ns`. Default `ms`. A wrong value is 400.

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
status, body = post({"name": "greptime-precision-bad", "type": "greptimedb",
                     "server": "127.0.0.1:4001", "database": "public", "table": "events",
                     "precision": "fortnight"})
assert status == 400 and "precision" in body["message"], body
status, created = post({"name": "greptime-precision-ok", "type": "greptimedb",
                        "server": "127.0.0.1:4001", "database": "public", "table": "events",
                        "precision": "us"})
assert status == 201, (status, created)
print("ok")
PY
```

```output
ok
```

### `format`

Write shape. Type: string. Required: no. Allowed: `sql`, `influx`. Default `sql`. A wrong value is 400.

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
status, body = post({"name": "greptime-format-bad", "type": "greptimedb",
                     "server": "127.0.0.1:4001", "database": "public", "table": "events",
                     "format": "csv"})
assert status == 400 and "format" in body["message"], body
status, created = post({"name": "greptime-format-ok", "type": "greptimedb",
                        "server": "127.0.0.1:4001", "database": "public", "table": "events",
                        "format": "influx"})
assert status == 201, (status, created)
print("ok")
PY
```

```output
ok
```

Example:

```json
{
  "name": "greptime-sink-1",
  "type": "greptimedb",
  "server": "127.0.0.1:4001",
  "database": "public",
  "table": "sensor_events"
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it inserts nothing. The send path needs a GreptimeDB server. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind. It prints `ok`.

```toml test
[node]
id = "docs-connector-greptimedb"

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
body = json.dumps({"name": "greptime-sink-1", "type": "greptimedb",
                   "server": "127.0.0.1:4001",
                   "database": "public",
                   "table": "sensor_events"}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=5) as res:
        status, created = res.status, json.load(res)
except urllib.error.HTTPError as err:
    status, created = err.code, json.load(err)
assert status == 201, (status, created)
assert created["type"] == "greptimedb", created
print("ok")
PY
```

```output
ok
```

## Limits

- Timestamps scale to the `precision` value.
- A dead server backs off and retries. The bounded queue sheds past its bound.
- A server error code stops the batch with a failure.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).

## Reference form (v1 API)

The reference form uses the v1 connector API. The v5 form above uses connector and action documents. This section keeps the reference page.

The GreptimeDB connector writes each rule result as one time-series row. It sends SQL inserts or line protocol points over HTTP. It groups rows by table and retries faults with backoff.

Use it when metrics must land in GreptimeDB tables. You get one HTTP request per flush with your own table mapping.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["greptimedb sink"]
    Sink -- "batch rows" --> Sql["POST /v1/sql"]
    Sink -- "batch points" --> Line["POST influxdb write"]
    Sql -- "429 5xx" --> Back["backoff retry"]
    Line -- "429 5xx" --> Back
    Back -- "retry" --> Sink
    Sink -- "queue full" --> Drop["fail closed"]
```

### Before you start

- Run GreptimeDB. Note the HTTP endpoint and the database.
- Choose the table template. Match its segments to the topic.
- This kind ships in the default build. No feature flag is necessary.

### Settings

Set `kind` to `greptimedb`.

| Setting | Required | Meaning |
|---|---|---|
| `endpoint` | yes | HTTP base of the server, with or without `/v1`. The sink adds `/v1` when missing. |
| `database` | no | Target database. Default `public`. An empty value is refused. |
| `auth` | no | Object with `username` and `password`. Default is none. It sends a Basic header. |
| `format` | no | `sql_insert` default, or `influx_line_protocol`. |
| `table_template` | yes | Table name with `${topic}` and `${topic_segment_N}`. Other chars become `_`. |
| `timestamp_precision` | no | `nanosecond`, `microsecond`, `millisecond` default, or `second`. The line path sends `ns`, `u`, `ms`, `s`. |
| `batch_size` | no | Rows per flush. Default 1000. Must exceed 0. |
| `buffer_capacity` | no | Max buffered rows. Default 10000. Must exceed 0. A full buffer fails closed. |
| `timeout_ms` | no | Request deadline in ms. Default 5000. A value below 1 reads as 1. |

Example:

```json
{
  "id": "greptime-1",
  "kind": "greptimedb",
  "config": {
    "endpoint": "http://127.0.0.1:4000/v1",
    "database": "public",
    "format": "sql_insert",
    "table_template": "metrics_${topic_segment_1}",
    "timestamp_precision": "millisecond",
    "batch_size": 100,
    "buffer_capacity": 10000,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it writes no row. The send path needs a GreptimeDB server. See the failure record in [Verification failures](../_verification/failures.md).

### Check that it works

Create the connector. The broker answers 201 with the kind.

```toml test
# Reference form run.
[node]
id = "docs-connector-greptimedb"

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
body = json.dumps({"id": "greptime-1", "kind": "greptimedb",
                   "config": {"endpoint": "http://127.0.0.1:4000/v1",
                              "database": "public",
                              "format": "sql_insert",
                              "table_template": "metrics_${topic_segment_1}",
                              "timestamp_precision": "millisecond",
                              "batch_size": 100,
                              "buffer_capacity": 10000,
                              "timeout_ms": 5000}}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v1/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=5) as res:
    assert res.status == 201
    print(json.load(res)["kind"])
PY
```

```output
greptimedb
```

### Limits

- An empty `endpoint`, `database`, or `table_template` is refused with 400.
- A `batch_size` or `buffer_capacity` of 0 is refused with 400.
- A payload that is not JSON is refused. An empty field name is refused.
- The SQL path groups rows by table. It sends one INSERT per table.
- A NaN or infinite number reads as NULL in SQL. The line path skips it.
- A null field reads as NULL in SQL. The line path skips it.
- A name outside `^[a-z_][a-z0-9_]*$` is quoted in SQL. A hostile name stays one identifier.
- A 429 or 5xx status retries with backoff. Other 4xx statuses fail fast.
- Backoff waits 2s, then doubles to 30s. Rows stay queued during backoff.
- A full buffer fails closed with a connection error. Buffered rows are kept.

### Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
