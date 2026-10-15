---
title: "Redis connector"
description: "Write rule results to Redis streams and keys."
sidebar:
  order: 55
---

# Redis connector

The Redis connector runs one command per row: stream append, set, hash set, or publish. It multiplexes one driver connection with pipelining. It reads AUTH and SELECT from the endpoint URL.

Use it when a cache, a stream, or a pub/sub channel must receive broker data. You get one Redis command per row.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["redis sink"]
    Sink -- "XADD SET HSET PUBLISH" --> Redis["redis server"]
    Redis -- "reply" --> Sink
```

## Before you start

- Run Redis. Note the endpoint URL.
- Pick one command and its key template.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set `kind` to `redis`.

| Setting | Required | Meaning |
|---|---|---|
| `endpoint` | yes | Endpoint URL, with AUTH and SELECT when necessary. |
| `command` | yes | One of `xadd`, `set`, `hset`, `publish` (flat field). |
| `stream_template` | for `xadd` | Stream name with `${topic}` substitution. |
| `maxlen` | no | Stream trim length for `xadd`. |
| `key_template` | for `set`/`hset` | Key name with `${topic}` substitution. |
| `ttl_seconds` | no | Expiry for `set`. |
| `field_template` | for `hset` | Hash field name. |
| `channel_template` | for `publish` | Channel name. |

Example (stream append):

```json
{
  "id": "redis-sink-1",
  "kind": "redis",
  "config": {
    "endpoint": "redis://127.0.0.1:6379",
    "command": "xadd",
    "stream_template": "events",
    "maxlen": 1000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it sends no command. The send path needs a Redis server. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind.

```toml test
[node]
id = "docs-connector-redis"

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
body = json.dumps({"id": "redis-sink-1", "kind": "redis",
                   "config": {"endpoint": "redis://127.0.0.1:6379",
                              "command": "xadd",
                              "stream_template": "events",
                              "maxlen": 1000}}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v1/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
with urllib.request.urlopen(req, timeout=5) as res:
    assert res.status == 201
    print(json.load(res)["kind"])
PY
```

```output
redis
```

## Limits

- An empty `endpoint` or key template is refused with 400.
- One connector runs one command. Use one connector per command.
- A dead server backs off and retries. The bounded queue sheds past its bound.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
