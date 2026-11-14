---
title: "GCP IoT connector"
description: "Bridge rule results to a Cloud IoT device endpoint."
sidebar:
  order: 62
---

# GCP IoT connector

The GCP IoT connector bridges each rule result to a Cloud IoT device endpoint. It mints a device JWT and publishes telemetry on the device topic. It renews the token before expiry.

Use it when a device registry must receive broker data as telemetry. You get one telemetry publish per result row with renewal and retry.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["gcp_iot sink"]
    Sink -- "JWT password" --> Conn["CONNECT client id"]
    Conn -- "PUBLISH QoS 0" --> Tel["/devices id events"]
    Sink -- "renew 60s early" --> Conn
    Sink -- "queue full" --> Drop["shed load"]
```

## Before you start

- Note the `project_id`, `cloud_region`, `registry_id` and `device_id`.
- Keep the device private key in PKCS#8 PEM form.
- For a full build, enable the feature: `--features gcp_iot`. The default build answers 400 and names the feature.

## Settings

Set `kind` to `gcp_iot`.

| Setting | Required | Meaning |
|---|---|---|
| `project_id` | yes | Device project. The JWT audience uses it. |
| `cloud_region` | yes | Device region. The client id uses it. |
| `registry_id` | yes | Device registry. The client id uses it. |
| `device_id` | yes | Device id. Topics and client id use it. |
| `private_key_pem` | yes | PKCS#8 key, RSA or P-256. The JWT signs with it. |
| `algorithm` | no | `RS256` default, or `ES256`. It must match the key. |
| `token_lifetime_secs` | no | JWT life in seconds. Default 3600. Valid range 60 to 86400. |
| `endpoint` | no | Bridge host. Default `mqtt.googleapis.com:8883`. `mqtt://` selects plaintext. |
| `batch_size` | no | Rows per flush. Default 250. It must be at least 1. |
| `buffer_capacity` | no | Max buffered rows. Default 10000. It must be at least 1. |
| `linger_ms` | no | Linger before a partial batch flushes. Default 50. |
| `max_retries` | no | Retries on exhaustion. Default 5. |
| `timeout_ms` | no | Connect and write deadline. Default 5000. Minimum 1. |
| `ca_bundle_pem` | no | Extra CA bundle in PEM form. Default is system roots only. |

Example:

```json
{
  "id": "gcp-iot-1",
  "kind": "gcp_iot",
  "config": {
    "project_id": "my-iot-project",
    "cloud_region": "us-central1",
    "registry_id": "telemetry-registry",
    "device_id": "edge-7",
    "private_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n",
    "algorithm": "RS256",
    "token_lifetime_secs": 3600,
    "endpoint": "mqtt.googleapis.com:8883",
    "batch_size": 250,
    "buffer_capacity": 10000,
    "linger_ms": 50,
    "max_retries": 5,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket. The send path needs a compatible endpoint. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind when the build holds the feature and the key parses, else 400. Both answers print `ok`.

```toml test
[node]
id = "docs-connector-gcp-iot"

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
body = json.dumps({"id": "gcp-iot-1", "kind": "gcp_iot",
                   "config": {"project_id": "my-iot-project",
                              "cloud_region": "us-central1",
                              "registry_id": "telemetry-registry",
                              "device_id": "edge-7",
                              "private_key_pem": "not-a-key",
                              "endpoint": "mqtt.googleapis.com:8883",
                              "batch_size": 50,
                              "linger_ms": 10,
                              "max_retries": 2,
                              "timeout_ms": 5000}}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v1/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=5) as res:
        status, created = res.status, json.load(res)
except urllib.error.HTTPError as err:
    status, created = err.code, json.load(err)
assert status in (201, 400), (status, created)
if status == 201:
    assert created["kind"] == "gcp_iot", created
else:
    assert "gcp" in created["error"].lower(), created
print("ok")
PY
```

```output
ok
```

## Limits

- A segment with `/`, `#` or `+` is refused. An empty `project_id`, `cloud_region`, `registry_id` or `device_id` is refused.
- A key that does not parse for the `algorithm` is refused.
- A `batch_size` of 0 is refused. A `ca_bundle_pem` without `BEGIN CERTIFICATE` is refused.
- Telemetry uses `/devices/{id}/events`. State uses `/devices/{id}/state`.
- TLS is the default. Plain `mqtt://` logs a warning and sends the JWT in clear.
- The bounded queue sheds past its bound. A slow endpoint never stalls publishers.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).

## Known limits in this release

The broker accepts the fields in this list. The broker does not apply them in this release.

- `batch_size` (the batch size)
- `linger_ms` (the batch wait time)
- `max_retries` (the retry count)

To set batching, use `resource_opts` in the action. The broker applies `resource_opts`.

A later release applies these fields.
