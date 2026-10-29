---
title: "Elasticsearch connector"
description: "Write rule results as documents to Elasticsearch or OpenSearch."
sidebar:
  order: 55
---

# Elasticsearch connector

The Elasticsearch connector writes one action document to an index. The
action chooses the operation, index, document id and document body. The
maintained driver carries the HTTP pool and TLS.

Use it when a rule result must become a searchable document. You get one
bulk request per batch and per-document failure counts.

```mermaid
flowchart LR
    Rule["rule engine"] -- "action parameters" --> Doc["document template"]
    Doc -- "JSON object" --> Bulk["_bulk request"]
    Bulk -- "action + index + id" --> Es["Elasticsearch / OpenSearch"]
    Es -- "per-item result" --> Count["inserted / rejected"]
```

## Before you start

- Run Elasticsearch or OpenSearch. Note the host and port.
- Pick the index. The action can use `${payload.<field>}` in the index.
- For a full build, enable the feature: `--features elasticsearch`. The
  default build answers 400 and names the feature.

## Connector settings

Set the connector type to `elasticsearch`. The aliases `elastic`, `es` and
`opensearch` select the same runtime.

| Setting | Required | Meaning |
|---|---|---|
| `server` | yes | Host and port, for example `127.0.0.1:9200`. No default. |
| `username` | no | Basic authentication user. |
| `password` | no | Basic authentication password. |
| `authentication` | no | Reference-shaped object with `username` and `password`. |

## Action settings

Set the action type to `elasticsearch`. The action owns the target and the
document.

| Setting | Required | Meaning |
|---|---|---|
| `connector` | yes | Connector name. |
| `action` | yes | `create`, `update` or `delete`. |
| `index` | yes | Index or alias template. No fixed index. |
| `id` | create: no; update/delete: yes | Document id template. |
| `doc` | no | Document template. The result must be a JSON object. |
| `overwrite` | no | `create` overwrites an existing id. Default `true`. |
| `doc_as_upsert` | no | `update` inserts a missing id. Default `false`. |
| `require_alias` | no | Require the index to be an alias. Default `false`. |
| `routing` | no | Custom routing value. |
| `max_retries` | no | Retries for one request. Default `2`. |

A document template that does not render a JSON object counts one failed
forward. The broker does not store the raw string.

Example action:

```json
{
  "name": "search-events",
  "type": "elasticsearch",
  "connector": "search",
  "parameters": {
    "action": "create",
    "index": "events-${YYYY.MM.dd}",
    "id": "${topic}-${timestamp}",
    "doc": "{\"topic\":\"${topic}\",\"payload\":${payload}}"
  }
}
```

## Limits

- One action owns one operation and one index template.
- Per-item `_bulk` failures count as rejected rows.
- Batching belongs to the action runtime. The driver writes one row per
  send.

## Try it

Run this example against a started broker. It creates a connector and
shows the refusal for a missing server.

```toml test
[node]
id = "docs-connector-elasticsearch"

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
def post(body):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/api/v5/connectors", data=json.dumps(body).encode(), method="POST",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as err:
            return err.code, json.load(err)
status, created = post({"name": "es-run-ok", "type": "elasticsearch",
                        "server": "127.0.0.1:9200"})
assert status in (201, 400), (status, created)
if status == 201:
    assert created.get("type") == "elasticsearch", created
else:
    assert "feature" in str(created).lower(), created
status, body = post({"name": "es-run-bad", "type": "elasticsearch"})
assert status == 400 and "server" in body["message"], body
print("ok")
PY
```

```output
ok
```

## Next steps

- Read [Rules](../rules/index.md) to send a rule result to this action.
- Read [Actions](../rules/actions.md) for the shared `resource_opts`.
