---
title: "Oracle connector"
description: "Write rule results as Oracle rows."
sidebar:
  order: 69
---

# Oracle connector

The Oracle connector writes each rule result as one table row. It sends typed rows as an atomic `MERGE INTO ... USING DUAL` statement with positional binds. It groups rows by shape before each write.

Use it when relational rows must hold telemetry in Oracle Database. You get one batched write per group with your own key mapping.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["oracle sink"]
    Sink -- "batch rows" --> Merge["MERGE INTO DUAL"]
    Merge -- "ORDS POST" --> Ords["ORDS endpoint"]
    Merge -- "TNS batch" --> Db["Oracle database"]
    Merge -- "deadlock" --> Back["backoff retry"]
    Back -- "retry" --> Merge
    Sink -- "failed flush" --> Keep["restore rows"]
```

## Before you start

- Run Oracle Database or an ORDS endpoint. Note the host and the service.
- Create the target table with a primary key.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set `kind` to `oracle`. The broker also accepts the aliases `oracle_db` and `ords`. It stores the kind as `oracle`.

| Setting | Required | Meaning |
|---|---|---|
| `url` | yes | ORDS endpoint or connection string. An empty value is refused. |
| `schema` | yes | Oracle schema name. An empty value is refused. |
| `table` | yes | Target table name. An empty value is refused. |
| `username` | yes | Database username. An empty value is refused. |
| `password` | yes | Database password. The key must exist. An empty value passes validation and fails at connect. |
| `custom_upsert` | no | Custom `MERGE INTO` statement override. Default is empty, so the sink builds the statement. |
| `key_columns` | no | Match keys for the merge. Default is empty. An empty list without `custom_upsert` is refused. |
| `batch_size` | no | Rows per batch. Default 500. A value of 0 reads as 1. |
| `buffer_capacity` | no | Accepted and stored. Default `None`. The sink flushes by `batch_size`. |
| `timeout_ms` | no | Connect and request deadline in ms. Default 5000. A value below 1 reads as 1. |

Example:

```json
{
  "id": "oracle-1",
  "kind": "oracle",
  "config": {
    "url": "https://oracle-host:8080/ords/admin/_/sql",
    "schema": "ADMIN",
    "table": "telemetry",
    "username": "admin",
    "password": "secret",
    "key_columns": ["device_id"],
    "batch_size": 100,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket. The send path needs an Oracle server. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind.

```toml test
[node]
id = "docs-connector-oracle"

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
body = json.dumps({"id": "oracle-1", "kind": "oracle",
                   "config": {"url": "https://oracle-host:8080/ords/admin/_/sql",
                              "schema": "ADMIN",
                              "table": "telemetry",
                              "username": "admin",
                              "password": "secret",
                              "key_columns": ["device_id"],
                              "batch_size": 100,
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
oracle
```

## Limits

- An empty `url`, `schema`, `table`, or `username` is refused with 400.
- Empty `key_columns` without `custom_upsert` is refused with 400.
- A payload that is not JSON is refused. A payload that is not UTF-8 is refused.
- An object payload maps each key to one column. Other JSON rides in one `payload` column.
- Rows with one shape share one parse. Mixed shapes fall back to one call per row.
- Deadlocks and lost connections retry with backoff. Missing tables and bad logins fail fast.
- A wrong table fails with the server text, including `ORA-00942`.
- A failed flush restores rows in order and engages backoff. Backoff waits 2s, then doubles to 30s. Flushes fail fast during backoff.
- A partial batch flushes after 50 ms. A full batch flushes at once.
- The API path posts `statementText` with binds to ORDS through `HttpOracleTransport` with Basic auth.
- The direct path runs `MERGE` batches over TNS through `DriverOracleTransport`. It holds one cached connection and commits once per batch.
- The tier is enterprise. The kind ships in the default build.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
