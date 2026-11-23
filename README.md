# IndraMQTT

[![CI](https://github.com/ankur-paan/indramqtt/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/ankur-paan/indramqtt/actions/workflows/ci.yml?query=branch%3Amain)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](#licence)

IndraMQTT is a Rust broker for MQTT 5.0 and 3.1.1. It runs SQL rules as messages arrive, so you can filter a sensor reading, reshape its payload, and publish the result to another topic or send it to a connector. The broker, rule engine, and tenant isolation run in one process.

**Pre-1.0 · single-node evaluation.** The first stable release is targeted for April 2027. [Current limits](#limits-at-this-time) · [Roadmap](ROADMAP.md) · [Documentation](docs/index.md) · [Website](https://indramqtt.com)

## Get started

### Route a hot reading to another topic

On a local development machine with Docker Compose v2 (with `--wait` support), start the broker and wait for its health check. The example accepts anonymous MQTT clients. The management API requires a separate login.

```sh
git clone https://github.com/ankur-paan/indramqtt.git
cd indramqtt
docker compose up -d --build --wait
curl -fsS http://localhost:18083/healthz
```

The health check prints `OK`. Follow the [Compose admin login steps](deploy/compose/README.md#get-an-admin-token) to set `TOKEN` in your shell. Create a rule that republishes the device ID and temperature whenever a reading exceeds 80.

```sh
curl -fsS http://localhost:18083/api/v1/rules \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{
    "name": "hot-readings",
    "topic_filter": "docs/in",
    "sql_query": "SELECT device, temp FROM \"docs/in\" WHERE temp > 80.0",
    "actions": [{"type": "republish", "topic": "docs/out", "qos": 0}]
  }'
```

Use the Mosquitto command-line clients for the next two commands. In one terminal, subscribe to the output topic and leave the subscriber running.

```sh
mosquitto_sub -h localhost -t docs/out -C 1
```

In another terminal, publish two readings. Only the second passes the filter.

```sh
mosquitto_pub -h localhost -t docs/in -m '{"device":"heater-1","temp":70}'
mosquitto_pub -h localhost -t docs/in -m '{"device":"heater-1","temp":90}'
```

The subscriber prints `{"device":"heater-1","temp":90}` and exits. Open the [local dashboard](http://localhost:18083/dashboard) to inspect the broker. Run `docker compose down` when finished. [Tested rule example](docs/rules/actions.md#tested-example) · [Compose guide](deploy/compose/README.md)

## Performance

On November 14, 2026, a single IndraMQTT node delivered **48,700 messages/s with a SQL republish rule active** (median of five 120-second runs, zero observed loss). The broker had four pinned CPUs and an 8 GiB memory limit. Each of 1,000 subscribers received its publisher’s MQTT 3.1.1 messages. The rule also republished every reading to a topic with no subscriber.

In the MQTT 5 point-to-point QoS 0 test, subscribers received **all 36 million messages, with zero loss**. Delivery averaged **184,829 messages/s** over the full run. The load generator took 193.7 seconds to publish the batch, and subscribers received the last messages 1.1 seconds later.

At QoS 1, IndraMQTT delivered **79,353 messages/s** and EMQX Enterprise 6.3.0 delivered **24,947 messages/s** with its default settings. Both runs recorded zero loss. Each comparison step ran once over plaintext loopback with 256-byte payloads. With 10,000 idle clients connected for 30 seconds, broker memory was **186.3 MiB** for IndraMQTT and **842.3 MiB** for EMQX.

[November measurements and test conditions](benchmark/2026-11-14.md) · [October one-CPU comparison, including rumqttd](benchmark/README.md) · [Router and SQL microbenchmarks](BENCHMARKS.md)

## What you can evaluate

| Area | What runs on one node |
| --- | --- |
| MQTT | TCP, TLS, and WebSocket listeners, QoS 0–2, retained messages, wills, shared subscriptions, and durable sessions. [Protocol guide](docs/concepts/index.md) |
| Streaming SQL | Filter and transform messages, aggregate over time or count windows, republish results, or forward them to a sink. [SQL guide](docs/rules/sql.md) |
| Connectors | The [capability table documents 46 sinks](crates/broker-connectors/CAPABILITY.md), including Kafka, PostgreSQL, S3, and webhooks. It names the transport and tests for each sink, including opt-in service tests where available. |
| Tenants | Routing, sessions, retained messages, and rules are scoped to each tenant. [Tenant model](docs/multi-tenancy/index.md) |
| Operations | Manage the broker with the REST API, dashboard, `indra ctl`, or [MCP](docs/mcp.md). Export Prometheus metrics and configure quotas. [Operations](docs/operations/index.md) · [Configuration](docs/operations/configuration.md) |

### Limits at this time

- The broker rejects a configuration that enables clustering. A broker restart closes client sockets. Durable sessions can resume on reconnect when their state is retained. [Architecture](ARCHITECTURE.md) · [Roadmap](ROADMAP.md)
- SSO login, protocol gateways, message schema validation, and OpenTelemetry export are deferred. The schema registry can store and serve schemas. [Feature status](docs/index.md) · [Settings and runtime behavior](docs/reference/unapplied-settings.md)

## Build from source

The Docker image includes the full connector set. To run the same set from a checkout, use the Rust toolchain and native build dependencies in the [Dockerfile](Dockerfile).

```sh
cargo build --release --locked -p broker-node --features full
cargo run --release --locked -p broker-node --features full --bin indramqtt
```

Run `cargo test --workspace` to test the default build. [Contributor guide](CONTRIBUTING.md) · [Migration](docs/migration.md) · [Changelog](CHANGELOG.md)

## Licence

Choose either the [MIT licence](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE) for all code in this repository, including production use. Pre-1.0 builds still contain legacy licence checks. Some operations can ask for a key, and the dashboard can show a trial state or the labels “community” and “enterprise”. The roadmap tracks their removal.

Report defects or request features through [GitHub issues](https://github.com/ankur-paan/indramqtt/issues). For support, email [sales@i-dacs.com](mailto:sales@i-dacs.com).
