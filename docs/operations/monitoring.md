---
title: "Monitoring"
description: "Check health, scrape metrics, and set alarms."
sidebar:
  order: 76
---

# Monitoring

This page shows what to watch on a running broker. Use it to wire health checks and dashboards. You get early warning before clients feel a fault.

```mermaid
flowchart LR
    Probe["GET /healthz"] -- "OK" --> Alive["process serves"]
    Alive -- "scrape" --> Met["GET /api/v1/metrics"]
    Met -- "watch" --> Alarm["alarm on threshold"]
    Alarm -- "page" --> Act["operator acts"]
```

## Before you start

- Keep the API bind reachable from your monitor.
- Create an API key with 16 characters or more.
- Pick scrape intervals: 10 s for health, 15 s for metrics.

## Health

`GET /healthz` needs no credentials. It returns `OK` when the process serves. Use it for container and load-balancer probes. The image checks it every 10 s.

| Check | Target | Good |
|---|---|---|
| Liveness | `GET /healthz` | Body `OK` |
| TCP accept | Connect to `:1883` | Connect succeeds |
| API serve | `GET /api/v1/metrics` with credentials | Status 200 |

## Metrics

Scrape Prometheus metrics at `/api/v1/metrics`. It needs credentials. Tune the exposition with the `prometheus.*` settings. See each setting in the [settings reference](../reference/settings.md).

```sh no-test
# Needs a running broker and an API key. The tested block below runs this check.
curl -sf -H "Authorization: Bearer $API_KEY" http://127.0.0.1:18083/api/v1/metrics | head -5
```

## Alarms

Set each alarm on a measured signal. Start with these four.

| Alarm | Signal | Action |
|---|---|---|
| Broker down | `/healthz` fails 3 times | Restart, then read logs |
| Memory growth | Container RSS rises week on week | Check subscriber lag and offline queues |
| Auth failures rise | `401` rate jumps on the API | Rotate keys, check clients |
| Client disconnects rise | Connection churn jumps | Check keepalive, quotas, and limits |

> [!NOTE]
> The broker sheds QoS 0 past `session.max_qos0_backlog` (default 1000) for a slow subscriber. A drop in delivered QoS 0 with a live broker points at a slow subscriber, not at a broker fault.

## Check that it works

Start the broker with this file. The health endpoint returns `OK`.

```toml test
[node]
id = "docs-monitoring"

[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.api]
bind = "0.0.0.0:18083"

[logging]
level = "warn"
```

```sh test
python3 - <<'PY'
import os, socket
port = int(os.environ.get("API_PORT", "18083"))
sock = socket.create_connection(("127.0.0.1", port), timeout=5)
sock.sendall(b"GET /healthz HTTP/1.0\r\nHost: docs\r\n\r\n")
print(sock.makefile().read().strip().splitlines()[-1])
PY
```

```output
OK
```

## Next steps

- Read logs in [Logs](./logging.md).
- Fix faults in [Troubleshooting](./troubleshooting.md).
