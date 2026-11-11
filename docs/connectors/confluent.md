---
title: "Confluent connector"
description: "Forward rule results to Confluent Cloud Kafka."
sidebar:
  order: 69
---

# Confluent connector

The Confluent connector produces rule results as Kafka records on Confluent Cloud. It authenticates with SASL and the API pair. The action names the topic and the key. The driver waits for the broker ack.

Use it when a Cloud-hosted pipeline reads broker data. You get each event on the topic you set.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["confluent sink"]
    Sink -- "SASL + produce" --> Cloud["Confluent Cloud"]
    Cloud -- "ack" --> Topic["Kafka topic"]
```

## Before you start

- Create a Confluent Cloud cluster and an API pair. Note the bootstrap host.
- Create the target topic.
- For a full build, enable the feature: `--features confluent`. The default build answers 400 and names the feature.

## Settings

Set `type` to `confluent`.

Connector settings:

| Setting | Required | Meaning |
|---|---|---|
| `bootstrap_hosts` | yes | Cloud host and port. |
| `api_key` | yes | API key (SASL username). |
| `api_secret` | yes | API secret (SASL password). |
| `ssl.enable` | no | TLS switch, default true. False means `SASL_PLAINTEXT` for local tests. |

Action settings:

| Setting | Required | Meaning |
|---|---|---|
| `topic` | yes | Topic template. |
| `message.key` | no | Partition key template. Absent means a null key. |
| `partition_strategy` | no | `random` (default) or `key_dispatch`. |
| `required_acks` | no | `all_isr` (default), `leader_only`, or `none`. |
| `kafka_headers` | no | One template rendered to a JSON object or array. |
| `kafka_ext_headers` | no | Array of `{kafka_ext_header_key, kafka_ext_header_value}` templates. |
| `kafka_header_value_encode_mode` | no | `none` (default) or `json`. |

Example:

```json
{
  "type": "confluent",
  "name": "cf-out",
  "bootstrap_hosts": "pkc-xxxx.us-east-1.aws.confluent.cloud:9092",
  "api_key": "key",
  "api_secret": "secret"
}
```

```json
{
  "type": "confluent",
  "name": "cf-act",
  "connector": "cf-out",
  "parameters": {
    "topic": "events",
    "message": {"key": "${client_id}"},
    "partition_strategy": "key_dispatch"
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket and sends no record. The send path needs Confluent Cloud.

## Check that it works

Create the connector. The broker answers 201 with the kind when the build holds Confluent, else 400 naming the feature. Both answers print `ok`. A body without a key answers 400 naming the field.

```toml test
[node]
id = "docs-connector-confluent"

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
status, created = post({"name": "cf-run-ok", "type": "confluent",
                        "bootstrap_hosts": "h:9092", "api_key": "k", "api_secret": "s"})
assert status in (201, 400), (status, created)
if status == 201:
    assert created.get("type") == "confluent_producer", created
else:
    assert "feature" in str(created).lower(), created
status, body = post({"name": "cf-run-bad", "type": "confluent",
                     "bootstrap_hosts": "h:9092", "api_secret": "s"})
assert status == 400 and "api_key" in body["message"], body
print("ok")
PY
```

```output
ok
```

## Limits

- The reference `authentication` object supplies the same pair as `api_key` and `api_secret`.
- OAuth is not supported. The driver always uses SASL with the API pair.
- Producer buffer tuning belongs to `resource_opts`. A present value is refused.

## Next steps

- Feed the action from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
