# IndraMQTT

[![Website](https://img.shields.io/badge/Website-indramqtt.com-blue?style=flat&logo=google-chrome&logoColor=white)](https://indramqtt.com)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/License-MIT%20OR%20Apache--2.0-blue.svg)](#licence)
[![Rust Version](https://img.shields.io/badge/Rust-1.80%2B-orange.svg)](https://www.rust-lang.org)
[![Automated Tests](https://img.shields.io/badge/Tests-automated-brightgreen.svg)](.github/workflows/ci.yml)

**Website**: [indramqtt.com](https://indramqtt.com)

[Benchmark](benchmark/README.md) • [Documentation](docs/index.md) • [Roadmap](ROADMAP.md) • [Architecture](ARCHITECTURE.md) • [Configuration](docs/operations/configuration.md) • [Operations](docs/operations/index.md) • [Migration](docs/migration.md) • [Changelog](CHANGELOG.md) • [Contributing](CONTRIBUTING.md)

IndraMQTT is the complete MQTT platform in one small binary: broker, SQL rules, data connectors and multi-tenancy in a single process. It is for IoT, industrial edge and cloud systems. All the code is under the MIT licence or the Apache-2.0 licence.

IndraMQTT is not stable yet. The first stable release is planned for April 2027. Read the [limits](#limits-at-this-time) before you use it.

---

## Broker Comparison at 1 CPU and 1 GB

The table shows the highest load that each broker holds for 180 s with 1 CPU and 1 GB of memory. A load counts only if no message is lost, there is no backlog, the latency stays below 1 s and the memory stays level. The test uses MQTT 5 and a payload of 256 bytes. One broker runs at a time on the same server. The date of the test is 2026-10-05.

The method, the tool, the broker settings, the MQTT 3.1.1 results and the raw data are in [`benchmark/`](benchmark/README.md).

| Scenario | IndraMQTT | Other broker A | Other broker B |
| :--- | :--- | :--- | :--- |
| Point-to-point QoS 0 | **100,000 msg/s** | 20,000 msg/s | none: memory increases |
| Point-to-point QoS 1 | **30,000 msg/s** | 7,500 msg/s | none: memory increases |
| Fan-in QoS 0 (N publishers, 1 subscriber) | 40,000 msg/s | 10,000 msg/s | **80,000 msg/s** |
| Fan-out QoS 0 (1 publisher, N subscribers) | **160,000 deliveries/s** | 20,000 deliveries/s | 80,000 deliveries/s |
| 10,000 idle connections, memory | **283 MiB** | 720 MiB | 638 MiB (161 clients not connected) |
| Memory with no clients | 76 MiB | 391 MiB | **1.6 MiB** |

> [!IMPORTANT]
> **Limits of this comparison**
> - Each result is one search and one 180 s run on a shared server.
> - The test does not use TLS. The load generator runs on the same server as the broker.
> - IndraMQTT is behind the other brokers in the fan-in scenario and in the memory with no clients.
> - With MQTT 3.1.1, IndraMQTT uses more memory for each connection than the other broker (21 KiB and 18 KiB).
> - The IndraMQTT numbers use the MQTT listener of the kernel. This listener is the default path.

---

## One Rust Process

IndraMQTT is one Rust process. The kernel holds the client sockets. It does the routing, the sessions and the rules. The container image runs one process.

Do not set `listeners.*.native` in new files. The four `native` settings are deprecated and ignored. Files that set them still load with a warning. A later release removes them.

### Connections during a restart

When the kernel stops, it closes the client sockets. Clients connect again. Durable sessions resume when the offline store keeps them. Clean sessions subscribe again when the new session has no state.

---

## Roadmap

- The target for the first stable release (1.0) is April 2027.
- All stable releases of IndraMQTT will be open source.
- The plan for each period is in [ROADMAP.md](ROADMAP.md).

---

## Licence

All the code in this repository is open source. You can use it under the MIT licence ([`LICENSE-MIT`](LICENSE-MIT)) or the Apache-2.0 licence ([`LICENSE-APACHE`](LICENSE-APACHE)). You can select one of the two.

- There is no enterprise edition and no commercial licence.
- You can use all functions in production. You do not pay for them.

The code still contains licence functions from the earlier model. Examples are the trial period, the licence key and the names "community" and "enterprise" in the API and in the dashboard. These functions are old code. We remove them step by step before the stable release. Until we remove a function, it can still ask for a licence key or show a trial state.

---

## Limits at This Time

- IndraMQTT is not stable. The behaviour and the settings can change before release 1.0.
- Most connectors have tests with a test transport only. [`crates/broker-connectors/CAPABILITY.md`](crates/broker-connectors/CAPABILITY.md) shows what each connector was tested with.
- A default build does not contain all connectors. Build with `--features full` to get all of them. The container image contains all of them.
- Single sign-on is not stable and is not available. You can store a backend, but the login endpoints answer the error that the reference API documents (404 or 400). Log in with a user name and a password, with MFA when the user has it, or use an API key.
- Protocol gateways are not available. `GET /gateways` reads `[]`. An unknown name misses with 404. Writes refuse with 400. Nothing is stored.
- Message schema validations are not available. `GET /schema_validations` reads `[]`. An unknown name misses with 404. Writes refuse with 400. The schema registry itself stays: it stores and serves schemas.
- OpenTelemetry export is not available. `GET /opentelemetry` reads the disabled config. `PUT /opentelemetry` refuses with 400. The broker runs no exporter.
- Cluster links are not available on a single node. `GET /cluster/links` reads `[]`. `POST /cluster/links` refuses with 400. Clustering stays out of the MVP: the broker fails boot when a cluster entry requests it.
- The broker applies `no_match` (allow or deny with no rule match). It applies `deny_action` (ignore or disconnect on denial). It applies `auth.allow_anonymous` (refuse anonymous CONNECT when false). It stores `auth.password_hash` without applying it (SA-21 owns it).
- The broker enforces the authorization cache policy. `enable` selects caching. `max_size` bounds entries per client. `ttl` bounds staleness. `excludes` skips listed topics. Rule, source, user and client changes clear entries.
- The broker refuses unevaluated source kinds with `400` at create and update. Nothing is stored. Stored sources from an older version stay unchanged. They deny each decision that would reach them. They raise an alarm that names the source. The operator can list, read and delete them through the API.

---

## Functions

### MQTT

- MQTT 3.1.1 and MQTT 5 on TCP and TLS.
- QoS 0, QoS 1 and QoS 2.
- Retained messages, the last will, shared subscriptions (`$share`) and delayed messages (`$delayed`).
- MQTT 5 subscription options, subscription identifiers, topic aliases, user properties and reason codes.
- WebSocket and secure WebSocket on the kernel listener.
- Sessions that stay after a disconnect, with an offline queue.

### Routing

- A radix trie matches the topic of each message with the topic filters, with the wildcards `+` and `#`.
- The main queues and buffers have limits with default values. An operator can change the limits in the configuration.
- Quotas limit the number of connections and the publish rate of a client.

### Rule engine

- The broker contains the [rekuiper](https://github.com/ankur-paan/rekuiper) SQL engine. A rule runs in the broker process, with no network hop.
- A rule can filter and change messages with `SELECT` and `WHERE` and with scalar functions.
- A rule can use time windows and count windows.
- A rule can send its result to a connector with `INTO connector("id")`, or publish it to a topic.

### Connectors

- Message systems: Kafka, RabbitMQ, Pulsar, Kinesis, Google Cloud Pub/Sub, Azure Event Hubs and an MQTT bridge.
- Databases: PostgreSQL, MySQL, Redis, MongoDB, Cassandra, Microsoft SQL Server and others.
- Time series and analytics: InfluxDB, TimescaleDB, ClickHouse, TDengine, GreptimeDB, OpenTSDB and others.
- Storage and search: S3, Azure Blob, Elasticsearch.
- Industrial: Sparkplug B, OPC UA.
- Other: HTTP webhook, a disk log with rotation.

The full list and the test level of each connector are in [`crates/broker-connectors/CAPABILITY.md`](crates/broker-connectors/CAPABILITY.md).

### Management

- A REST API and a web dashboard on port 18083.
- The command `indra ctl` for an operator.
- Prometheus metrics at `/api/v1/metrics`.
- Users, access rules, tenants and API keys.
- One configuration file (`indra.toml`), with environment variables and command flags as more layers. See [docs/operations/configuration.md](docs/operations/configuration.md).

### Cluster

- Membership with the SWIM protocol.
- Cluster metadata with Raft.
- Message transfer between nodes with QUIC.

---

## Microbenchmarks of the Router and the Rule Engine

These values are for one function in one thread, in memory. They are not network values. They change with the hardware. Run the command below on your hardware to get your values.

```bash
cargo test --release --bench broker_throughput -- --nocapture
```

| Benchmark | Measured value | What it measures |
| :--- | :--- | :--- |
| Router, topic with a match | approximately 3.05 million lookups/s | A lookup that finds 3 subscribers in 1,000 topic filters. |
| Router, topic with no match | approximately 7.80 million lookups/s | A walk of the trie for a topic that has no subscriber. |
| Rule engine, ingress | approximately 4.77 million events/s | JSON decode, a `WHERE` filter and a `SELECT` projection. Each event arrives at the sink. |

The source is [`crates/broker-node/benches/broker_throughput.rs`](crates/broker-node/benches/broker_throughput.rs). [BENCHMARKS.md](BENCHMARKS.md) gives the method.

---

## Architecture at This Time

The diagram shows the broker. Clients connect to the kernel directly.

```
                      MQTT CLIENTS
                           │
               TCP / TLS / WebSocket
                           │
       ┌──────────────────▼───────────────────┐
       │             RUST KERNEL              │
       │                                      │
       │ MQTT listeners (TCP, TLS, WS, WSS)   │
       │ Sessions                             │
      │ Router (radix trie)                  │
      │ Authentication and access rules      │
      │ Quotas and rate limits               │
      │ Retained messages                    │
      │ Offline queues                       │
      │ Rule engine (rekuiper)               │
      │ Connectors                           │
      │ Cluster (Raft, QUIC)                 │
      │ REST API and dashboard               │
      │ Metrics                              │
      └──────────────────────────────────────┘
```

- **Connection and session are different things.** The session is in the Rust kernel. It holds the subscriptions, the messages in flight and the offline queue.
- **Payloads stay as bytes.** The broker decodes a payload only when a rule reads its fields.
- **A rule runs one time for each message.** It runs on the node that receives the message, before the cluster sends the message to other nodes.

[ARCHITECTURE.md](ARCHITECTURE.md) has more detail.

---

## Repository Structure

```
indramqtt/
├── benchmark/            # Broker comparison: method, tool, raw results
├── crates/
│   ├── brokerlink/       # Frame codec for tests
│   ├── broker-protocol/  # MQTT types and the wire codec
│   ├── broker-router/    # Topic router
│   ├── broker-session/   # Sessions and QoS state
│   ├── broker-storage/   # Storage for retained messages and queues
│   ├── broker-auth/      # Authentication and access rules
│   ├── broker-config/    # Configuration layers and schema
│   ├── broker-rules/     # Rule engine
│   ├── broker-connectors/            # Connectors
│   ├── broker-connectors-enterprise/ # More connectors (name from the earlier model)
│   ├── broker-rules-enterprise/      # Window functions (name from the earlier model)
│   ├── broker-cluster/   # Cluster
│   ├── broker-api/       # REST API and dashboard
│   ├── broker-observability/         # Metrics
│   └── broker-node/      # The broker binary and the operator command
├── deploy/               # Container and Helm files
├── docs/                 # Guides
├── schemas/              # JSON schema of the configuration
├── tests/                # End-to-end tests
└── tools/                # Boot test and other scripts
```

---

## Get Started

### Run with Docker

```bash
git clone https://github.com/ankur-paan/indramqtt.git && cd indramqtt
docker compose up -d
```

Open the dashboard at `http://localhost:18083/dashboard`. MQTT clients connect to port 1883.

### Build from source

You need Rust 1.80 or later.

```bash
# Build the kernel and the operator command.
cargo build --release -p broker-node --features full

# Run the tests.
cargo test --workspace

# Start the kernel.
cargo run --release -p broker-node --bin indramqtt
```

---

## Contact

- Website: [indramqtt.com](https://indramqtt.com)
- Questions and support: [sales@i-dacs.com](mailto:sales@i-dacs.com)
- Defects and requests: [GitHub issues](https://github.com/ankur-paan/indramqtt/issues)
