# IndraMQTT Beta 2026.11

This is the first public beta of IndraMQTT. It is intended for single-node
evaluation. The first stable release is targeted for April 2027.

## Try the pinned image

The Compose example mounts a configuration that allows anonymous MQTT clients
for evaluation. Keep the management port on a trusted network and change the
default administrator password before using it beyond a local test.

```sh
git clone https://github.com/Indus-Stack/indramqtt.git
cd indramqtt
git checkout v2026.11.0-beta.1
docker pull indusstack/indramqtt:2026.11.0-beta.1
docker compose up -d --no-build --wait
curl -fsS http://localhost:18083/healthz
```

The health check prints `OK`. Follow the [Compose guide](https://github.com/Indus-Stack/indramqtt/blob/v2026.11.0-beta.1/deploy/compose/README.md)
for an MQTT publish and subscribe example and an administrator login.

## What to evaluate

- MQTT 3.1.1 and 5 on TCP, TLS, and WebSocket, including QoS 0–2, retained
  messages, wills, shared subscriptions, and durable sessions.
- Streaming SQL rules that filter, transform, republish, or forward messages.
- The connector set included in the image, with each connector's test level
  documented in the [capability table](https://github.com/Indus-Stack/indramqtt/blob/v2026.11.0-beta.1/crates/broker-connectors/CAPABILITY.md).
- Single-node operations through the REST API, dashboard, `indra` CLI, and MCP.

## Current limits

- Clustering is disabled. A restart closes client sockets; retained durable
  sessions can resume when clients reconnect.
- SSO login, protocol gateways, message schema validation, and OpenTelemetry
  export are deferred.
- Pre-1.0 builds still contain legacy licence checks and labels. Some
  operations can ask for a key or show a trial state despite the repository's
  MIT OR Apache-2.0 licence.
- APIs, settings, and data formats may change before 1.0. Keep backups of
  evaluation data.

See the [README](https://github.com/Indus-Stack/indramqtt/blob/v2026.11.0-beta.1/README.md), [changelog](https://github.com/Indus-Stack/indramqtt/blob/v2026.11.0-beta.1/CHANGELOG.md), and
[roadmap](https://github.com/Indus-Stack/indramqtt/blob/v2026.11.0-beta.1/ROADMAP.md) for details. Please report reproducible issues through
[GitHub Issues](https://github.com/Indus-Stack/indramqtt/issues).
