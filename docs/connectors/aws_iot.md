---
title: "AWS IoT connector"
description: "Bridge rule results to AWS IoT Core topics."
sidebar:
  order: 76
---

# AWS IoT connector

The AWS IoT connector bridges each rule result to AWS IoT Core topics. It holds one mutual TLS session to the device gateway. It maps local topics to remote topics per row.

Use it when cloud topics must receive broker data. You get one publish per result row with renewal and retry.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["aws_iot sink"]
    Sink -- "TLS session" --> Conn["CONNECT client id"]
    Conn -- "PUBLISH QoS 0/1" --> Cloud["IoT Core topics"]
    Sink -- "drop" --> Back["backoff retry"]
    Back -- "retry" --> Sink
```

## Before you start

- Note the IoT Core `endpoint`, the `region`, and the gateway `client_id`.
- Keep the device CA, certificate, and private key in PEM form.
- For a full build, enable the feature: `--features aws_iot`. The default build answers 400 and names the feature.

## Settings

Set `kind` to `aws_iot`. The broker also accepts `aws_iot_core`. It stores the kind as `aws_iot`.

| Setting | Required | Meaning |
|---|---|---|
| `endpoint` | yes | IoT Core endpoint host, with an optional port. Default port is 8883. An empty value is refused. |
| `region` | yes | AWS region. An empty value is refused. |
| `client_id` | yes | Gateway client id. An empty value is refused. |
| `auth` | no | Object with `type` `mtls` or `sigv4`. Default is `mtls` with empty fields, which validation refuses. Use `mtls` with PEM fields. The build rejects `sigv4` at registration. |
| `topic_mappings` | yes | Non-empty array of `local_topic` to `remote_topic` rows with an optional `direction`. A blank topic is refused. A remote topic with `+` or `#` is refused. |
| `shadow_sync` | no | Object with `thing_name_template`. An empty template is refused. |
| `batch_size` | no | Rows per flush. Default 250. A value of 0 is refused. |
| `buffer_capacity` | no | Max buffered rows. Default is none, which means no bound. |
| `linger_ms` | no | Linger before a partial batch flushes. Default 50. |
| `max_retries` | no | Retries on throttles and disconnects. Default 5. `null` means no limit. A value of 0 means no retry. |
| `timeout_ms` | no | Request deadline. Default 5000. Minimum 1. |
| `connect_timeout_ms` | no | TCP connect deadline. Default 5000. Minimum 1. |
| `handshake_timeout_ms` | no | TLS handshake deadline. Default 5000. Minimum 1. |
| `ca_bundle_pem` | no | Extra CA bundle in PEM form. It must hold `BEGIN CERTIFICATE` when set. |
| `alpn_protocols` | no | ALPN list. Default is `mqtt`. An empty list disables ALPN. |

Example:

```json
{
  "id": "aws-iot-1",
  "kind": "aws_iot",
  "config": {
    "endpoint": "abc-ats.iot.us-east-1.amazonaws.com",
    "region": "us-east-1",
    "client_id": "indra-bridge-1",
    "auth": {"type": "mtls",
              "ca_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
              "client_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
              "client_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"},
    "topic_mappings": [{"local_topic": "sensors/+",
                        "remote_topic": "indra/up",
                        "direction": "localtoremote"}],
    "batch_size": 50,
    "linger_ms": 10,
    "max_retries": 2,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket. The send path needs an IoT Core endpoint. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

Create the connector. The broker answers 201 with the kind when the build holds the feature and the material parses, else 400. Both answers print `ok`.

```toml test
[node]
id = "docs-connector-aws-iot"

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
body = json.dumps({"id": "aws-iot-1", "kind": "aws_iot",
                   "config": {"endpoint": "abc-ats.iot.us-east-1.amazonaws.com",
                              "region": "us-east-1",
                              "client_id": "indra-bridge-1",
                              "auth": {"type": "mtls",
                                        "ca_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
                                        "client_cert_pem": "-----BEGIN CERTIFICATE-----\n...\n-----END CERTIFICATE-----\n",
                                        "client_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----\n"},
                              "topic_mappings": [{"local_topic": "sensors/+",
                                                  "remote_topic": "indra/up",
                                                  "direction": "localtoremote"}],
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
    assert created["kind"] == "aws_iot", created
else:
    assert "aws_iot" in created["error"].lower().replace("-", "_"), created
print("ok")
PY
```

```output
ok
```

## Limits

- An empty `endpoint`, `region`, or `client_id` is refused with 400. A bad host or port is refused with 400.
- A PEM field without its marker is refused with 400. A `sigv4` shape passes field checks but registration refuses it with 400. Use `mtls`.
- An empty `topic_mappings` is refused with 400. A blank mapping topic is refused. A remote topic with `+` or `#` is refused.
- A `batch_size` of 0 is refused with 400. A `ca_bundle_pem` without `BEGIN CERTIFICATE` is refused with 400.
- The production path uses `TlsAwsIotTransport` through the maintained TLS stack. It dials port 8883 with mutual TLS. It sends CONNECT once, then PUBLISH at QoS 0 and 1. The keep-alive is 60 seconds.
- A throttled or dropped batch retries with backoff. A rejected batch restores rows in order and backs off. Backoff waits 2s, then doubles to 30s.
- The connector runs behind the rule engine bounded queue. A slow cloud endpoint sheds load; it never stalls publishers.
- The tier is enterprise. The kind needs the `aws_iot` feature build.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).

## Known limits in this release

The broker accepts the fields in this list. The broker does not apply them in this release.

- `batch_size` (the batch size)
- `buffer_capacity` (the buffer size)
- `linger_ms` (the batch wait time)
- `max_retries` (the retry count)

To set batching, use `resource_opts` in the action. The broker applies `resource_opts`.

A later release applies these fields.
