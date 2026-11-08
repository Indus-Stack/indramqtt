---
title: "CockroachDB connector"
description: "Write rule results as CockroachDB table rows."
sidebar:
  order: 86
---

# CockroachDB connector

The CockroachDB connector writes each rule result as one table row. It sends PostgreSQL-style bound parameters through the maintained driver. It uses native `UPSERT INTO`, retries transaction serialization failures, and splits rows that the database rejects.

Use it when relational rows must land in a CockroachDB table. You get one statement per chunk, a column set from the payload keys, and conflict handling on the primary key.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["cockroachdb sink"]
    Sink -- "rows and 50 ms linger" --> Batch["batch of 500 rows"]
    Batch -- "bound UPSERT" --> Driver["PostgreSQL driver"]
    Driver -- "row rejected" --> Split["retry each row"]
    Driver -- "40001 serialization" --> Retry["retry the chunk"]
    Driver -- "other fault" --> Back["restore and back off"]
    Back --> Batch
```

## Before you start

- Run a CockroachDB cluster. Note the SQL connection string.
- Create the target table. Its columns must match the JSON payload keys.
- This kind ships in the default build. No feature flag is necessary.
- Set `sslmode=disable` only for a local cluster without TLS. A missing `sslmode` uses TLS.

## Settings

Set `kind` to `cockroachdb` on the v1 API. The v1 route also accepts `cockroach` and `crdb`. It stores the kind as `cockroachdb`.

| Setting | Required | Meaning |
|---|---|---|
| `connection_string` | yes | PostgreSQL URL, for example `postgresql://user:pass@host:26257/db?sslmode=disable`. An empty value is refused. The default port is 26257, the default database is `defaultdb`, and the default user is `root`. |
| `table` | yes | Target table name. An empty value is refused. The broker quotes an identifier that needs quotes. |
| `upsert_conflict_columns` | no | Primary key columns for conflict handling. The default is empty, which uses native `UPSERT INTO`. A non-empty list uses `INSERT ... ON CONFLICT (...) DO UPDATE SET ...`. With no column left to update it uses `DO NOTHING`. |
| `batch_size` | no | Rows per flush. The default is 500. The sink treats 0 as 1. |
| `max_retry_attempts` | no | Attempts for one chunk on SQLSTATE `40001`. The default is 5. The sink treats 0 as 1. |
| `buffer_capacity` | no | Accepted. The sink does not read it as a bound. The queue holds one batch. |
| `timeout_ms` | no | Connect and request deadline. The default is 5000. A value below 1 becomes 1. |

The v5 route reads `connection_string`, `server`, or `url`. The default is `postgresql://root@127.0.0.1:26257/idacs?sslmode=disable`. It reads `table` (default `sensor_events`) and `timeout_ms`, `timeout`, or `request_timeout_ms`. It fixes `upsert_conflict_columns` to empty, `batch_size` to 1, `max_retry_attempts` to 5, and `buffer_capacity` to none. It registers the name and the aliases `cockroachdb:name` and `cockroach:name`.

Example:

```json
{
  "id": "cockroach-1",
  "kind": "cockroachdb",
  "config": {
    "connection_string": "postgresql://root@10.0.0.1:26257/telemetry?sslmode=disable",
    "table": "readings",
    "upsert_conflict_columns": ["device_id"],
    "batch_size": 500,
    "max_retry_attempts": 5,
    "buffer_capacity": null,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket. The send path needs a CockroachDB cluster. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

The v1 route stores the kind as `cockroachdb`. The v5 route also builds the sink. Both answers print `ok`.

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
import json, os, urllib.request, urllib.error
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]
def call(method, path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as err:
        return err.code, json.load(err)
s1, b1 = call("POST", "/api/v1/connectors",
              {"id": "cockroach-1", "kind": "cockroachdb",
               "config": {"connection_string":
                          "postgresql://root@10.0.0.1:26257/telemetry?sslmode=disable",
                          "table": "readings",
                          "upsert_conflict_columns": ["device_id"],
                          "batch_size": 500,
                          "max_retry_attempts": 5,
                          "buffer_capacity": None,
                          "timeout_ms": 5000}})
assert s1 == 201, (s1, b1)
assert b1["kind"] == "cockroachdb", (s1, b1)
s2, b2 = call("POST", "/api/v5/connectors",
              {"name": "cockroach-2", "type": "cockroachdb",
               "server": "postgresql://root@127.0.0.1:26257/defaultdb?sslmode=disable",
               "table": "sensor_events", "timeout_ms": 5000})
assert s2 == 201, (s2, b2)
assert b2["type"] == "cockroachdb", (s2, b2)
print("ok")
PY
```

```output
ok
```

## Limits

- An empty `connection_string` or `table` is refused with 400.
- A payload must be JSON. A JSON object becomes one column per key. A non-object value becomes one `payload` column. An empty object or an empty key is a dispatch error and sends no row.
- The sink collects the union of columns in a batch in first-seen order. A row without a column binds SQL NULL for it.
- The bind parameter limit is 65535. A batch with more columns than the limit is dropped with an error. A large batch splits into statements of at most `65535 / columns` rows.
- SQLSTATE `40001` retries the chunk up to `max_retry_attempts`. The pause is 5 ms times the attempt number. After the last attempt the chunk restores and the sink backs off.
- A rejected row splits the chunk into single-row statements. SQLSTATE classes `22` and `23`, and codes `42703` and `42804`, count as rejected. The sink counts and drops a rejected single row.
- A connection or terminal fault restores the remaining rows and backs off. Backoff waits 2 seconds, doubles, and stops at 30 seconds. Flushes fail fast during backoff.
- The production path uses `PgDriverCockroachDbTransport` on the maintained driver. The hand-written TCP transport serves offline unit tests only.
- `sslmode=disable` uses plaintext. Any other explicit `sslmode`, or no `sslmode`, uses TLS with the system trust store.
- The connector runs behind the rule engine bounded queue. A slow server never stalls publishers.
- The tier is enterprise. This kind ships in the default build.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
