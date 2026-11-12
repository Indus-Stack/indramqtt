---
title: "Azure Event Grid connector"
description: "Connector and action name that this build refuses."
sidebar:
  order: 80
---

# Azure Event Grid connector

The Azure Event Grid name is refused in this build. The connector route and the action route answer 400. Nothing is stored and no rows leave the broker through it.

Use `azure_event_hub_producer` for Azure event delivery.

```mermaid
flowchart LR
    Request["connector or action create"] --> Map["KC-00 capability map"]
    Map -- "azure_event_grid: refused" --> Refuse["400 refusal"]
    Refuse -- "store nothing" --> Ext["no external system"]
    Request -- "event hub type" --> Hub["azure_eventhubs sink"]
    Hub -- "batch send" --> Cloud["event hubs"]
```

## Before you start

- Read the action list with `GET /api/v5/action_types`. This name stays absent from that list.
- Use `azure_event_hub_producer` for Azure event delivery.
- No feature flag applies. No build enables a send path for this name.

## Settings

This name takes no settings. The broker refuses the create and stores nothing.

| Setting | Required | Meaning |
|---|---|---|
| `type` | yes | It must be `azure_event_grid`. |
| `name` | yes | Action id. It must not be empty. |

Example:

```json
{
  "type": "azure_event_grid",
  "name": "azure-event-grid-1"
}
```

> [!NOTE]
> The docs run sends this shape to both routes. Both routes refuse it and store nothing. No socket opens. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector and the action. Both routes refuse the name. Both answers print `ok`.

```toml test
[node]
id = "docs-connector-azure-event-grid"

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
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5) as res:
            return res.status, json.load(res)
    except urllib.error.HTTPError as err:
        return err.code, json.load(err)
s1, b1 = call("POST", "/api/v1/connectors",
              {"id": "azure-eg-1", "kind": "azure_event_grid", "config": {}})
assert s1 == 400, (s1, b1)
assert "azure_event_grid" in json.dumps(b1), (s1, b1)
s2, b2 = call("POST", "/api/v5/actions",
              {"type": "azure_event_grid", "name": "azure-event-grid-1"})
assert s2 == 400, (s2, b2)
assert b2["code"] == "ALREADY_EXISTS", (s2, b2)
assert "azure_event_grid" in json.dumps(b2), (s2, b2)
print("ok")
PY
```

```output
ok
```

## Limits

- `POST /api/v1/connectors` with this kind answers 400. The message names `azure_event_grid` as an unknown kind.
- `POST /api/v5/actions` with this type answers 400 with code `ALREADY_EXISTS`. The message names `azure_event_grid`. Nothing is stored.
- `GET /api/v5/action_types` omits this name. No runtime stands behind it in this build.
- No row leaves the broker through this name. Use `azure_event_hub_producer` for event delivery.
- No tier applies. No feature build enables this path.

## Next steps

- Send events with [Azure Event Hubs](./index.md).
- Feed actions from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
