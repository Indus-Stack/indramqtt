---
title: "Operations"
description: "Configure, run and observe the broker."
sidebar:
  order: 70
---

# Operations

This section covers daily production work. Use it to deploy, size, back up, upgrade, watch, and fix the broker. You get a broker that stays up and a plan for each fault.

```mermaid
flowchart LR
    Deploy["deployment"] -- "size" --> Size["sizing"]
    Size -- "keep" --> Back["backup"]
    Back -- "move" --> Up["upgrade"]
    Up -- "watch" --> Mon["monitoring"]
    Mon -- "read" --> Log["logs"]
    Log -- "fix" --> Fix["troubleshooting"]
    Fix -- "tune" --> Deploy
```

## Before you start

- Install the broker from [Install](../getting-started/install.md).
- Read [Configuration](./configuration.md) for the full layer model.
- Keep ports 1883 and 18083 free.

## Map

| Page | Use it when |
|---|---|
| [Deployment](./deployment.md) | You install as binary, container, or service |
| [Sizing](./sizing.md) | You pick CPU and memory from measured numbers |
| [Backup and restore](./backup.md) | You keep state and bring it back |
| [Upgrade](./upgrade.md) | You move to a new release |
| [Monitoring](./monitoring.md) | You wire health, metrics, and alarms |
| [Logs](./logging.md) | You set levels and read output |
| [Troubleshooting](./troubleshooting.md) | A check turns red |

## Validate before start

Run `--check-config` against the config directory. It prints `ok` and exits 0 for a valid directory. Fix each error it names before you start.

```sh no-test
# Reads only. It never starts the broker, so the example test does not run this duplicate.
indramqtt --check-config --config-dir /etc/indramqtt
```

## Inspect the running broker

`indra ctl status` shows broker state. It needs the API endpoint and a key:

```sh no-test
# Output lists live nodes and varies. The example test does not compare it.
indra ctl --endpoint http://localhost:18083 --api-key "$API_KEY" status
```

`explain` shows the value in operation for one setting:

```sh no-test
# Output shows local values and varies. The example test does not compare it.
indra ctl --endpoint http://localhost:18083 --api-key "$API_KEY" config explain --key node.id
```

## Observe

Scrape Prometheus metrics at `/api/v1/metrics`. It needs credentials. Set buckets and the push target with the `prometheus.*` settings. See [Monitoring](./monitoring.md).

## Back up

Keep the data directory (`node.data_dir`). It holds users, access rules, sessions and retained messages. A restart on the same directory keeps them. See [Backup](./backup.md).

## Bounds

Each accumulating queue has a bound and a finite default. The publish path sheds QoS 0 past `session.max_qos0_backlog` (default 1000) so a slow subscriber cannot grow memory without limit. QoS 1 and QoS 2 never shed.

## Check that it works

Start the broker with this file. Validate the directory. The command prints `ok`.

```toml test
[node]
id = "docs-operations"

[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.api]
bind = "0.0.0.0:18083"

[logging]
level = "warn"
```

```sh test
"${INDRAMQTT_BIN:-indramqtt}" --config-dir "${DOCS_CFG_DIR:-./docs-operations}" --check-config
```

```output
ok
```

## Next steps

- Deploy with [Deployment](./deployment.md).
- Read measured numbers in [Benchmarks](../benchmarks/index.md).
