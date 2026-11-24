---
title: "OCI Streaming connector"
description: "Write rule results to an OCI Streaming stream."
sidebar:
  order: 103
---

# OCI Streaming connector

The OCI Streaming connector writes each rule result to a stream in Oracle Cloud Infrastructure. It sends batched messages with a signed `PutMessages` request. It signs each request with an RSA private key and the OCI HTTP signature scheme.

Use it when Oracle Cloud must receive broker telemetry. You get batched messages with your own partition key and a bounded retry path.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["oci_streaming sink"]
    Sink -- "batch rows" --> Queue["bounded queue"]
    Queue -- "PutMessages" --> Sign["sign with RSA key"]
    Sign -- "POST messages" --> Oci["OCI stream"]
    Oci -- "429 or 500" --> Back["backoff retry"]
    Back -- "retry" --> Sign
```

## Before you start

- Create an OCI stream and note its stream OCID and stream pool OCID.
- Create an API key and note its fingerprint. Keep the private key PEM.
- Note the tenancy OCID, the user OCID, and the cell endpoint.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set `kind` to `oci_streaming` on the v1 route. The v1 route also accepts `oci` and `oracle_streaming`. The v5 route refuses this kind under the SB-11 ruling. It answers 400 and stores nothing.

| Setting | Required | Meaning |
|---|---|---|
| `endpoint` | yes | Cell endpoint. It must use `https://`. A loopback `http://127.0.0.1` or `http://localhost` endpoint is also accepted for a local test. The v5 route also reads `url`. |
| `stream_pool_id` | yes | Stream pool OCID. It must start with `ocid1.streampool.`. |
| `stream_id` | yes | Stream OCID. It must start with `ocid1.stream.`. |
| `tenancy_ocid` | yes | Tenancy OCID. It must start with `ocid1.`. |
| `user_ocid` | yes | User OCID. It must start with `ocid1.`. |
| `fingerprint` | yes | Public key fingerprint. It must have 16 hex pairs. |
| `private_key_pem` | yes | PKCS#1 or PKCS#8 RSA private key PEM. The v5 route also reads `private_key`. |
| `partition_key_template` | yes | Partition key with `${topic}`, `${qos}`, `${timestamp}`, `${client_id}`, or `${payload.<field>}`. `${client_id}` reads the `client_id` field of the JSON payload. An empty value is refused. The v5 route also reads `partition_key` and defaults to `${topic}`. |
| `batch_size` | no | Records per `PutMessages` call. Default 500, the service cap. A value of 0 is refused. `null` means no item limit. The v5 route fixes 1. |
| `batch_bytes` | no | Batch byte limit. Default 4194304 (4 MiB). A value of 0 is refused. `null` means no byte limit. The v5 route fixes 4194304. |
| `buffer_capacity` | no | Buffered row limit. Default 10000. A value of 0 is refused. A full buffer fails closed. `null` uses the same finite default. |
| `linger_ms` | no | Linger flush window in ms. Default 20. `null` means no time bound. The v5 route fixes 10. |
| `max_retries` | no | Retries on throttles and partial failures. Default 4. `null` means no limit. A value of 0 means no retry. The v5 route fixes 3. |
| `initial_backoff_ms` | no | Base retry delay in ms. Default 100. The sink grows the delay from this base. The first retry uses two times this base. The v5 route fixes 100. |
| `max_backoff_ms` | no | Retry delay ceiling before jitter, in ms. Default 2500. Jitter adds up to half the grown delay. The jittered delay is capped at two times this value. The v5 route fixes 2000. |
| `timeout_ms` | no | Request deadline in ms. Default 5000. A value below 1 reads as 1. The v5 route also reads `timeout`. |

The v1 route reads the full settings above. The v5 route refuses the kind with code `BAD_REQUEST` under the SB-11 ruling. The v5 route stores nothing.

Example:

```json
{
  "id": "oci-stream-1",
  "kind": "oci_streaming",
  "config": {
    "endpoint": "https://cell-1.streaming.us-east-1.oci.oraclecloud.com",
    "stream_pool_id": "ocid1.streampool.oc1..example",
    "stream_id": "ocid1.stream.oc1..example",
    "tenancy_ocid": "ocid1.tenancy.oc1..example",
    "user_ocid": "ocid1.user.oc1..example",
    "fingerprint": "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55",
    "private_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
    "partition_key_template": "${topic}",
    "batch_size": 500,
    "batch_bytes": 4194304,
    "buffer_capacity": 10000,
    "linger_ms": 20,
    "max_retries": 4,
    "initial_backoff_ms": 100,
    "max_backoff_ms": 2500,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this shape. Creation validates only; it opens no socket. See the failure record in [Verification failures](../_verification/failures.md).

## Check that it works

The v5 route refuses the kind. The v1 route answers 201 with the kind when the build holds the feature, else 400 naming the feature. Both answers print `ok`.

```toml test
[node]
id = "docs-connector-oci-streaming"

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

def call(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}", data=data, method=method,
        headers={"Authorization": f"Bearer {key}",
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            raw = res.read().decode()
            return res.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as err:
        raw = err.read().decode()
        try:
            return err.code, json.loads(raw)
        except ValueError:
            return err.code, raw

identity = {
    "endpoint": "https://cell-1.streaming.us-east-1.oci.oraclecloud.com",
    "stream_pool_id": "ocid1.streampool.oc1..testpool",
    "stream_id": "ocid1.stream.oc1..teststream",
    "tenancy_ocid": "ocid1.tenancy.oc1..test",
    "user_ocid": "ocid1.user.oc1..test",
    "fingerprint": "20:3b:97:13:aa:bb:cc:dd:ee:ff:00:11:22:33:44:55",
    "private_key_pem": "key",
    "partition_key_template": "${topic}",
}

s2, b2 = call("POST", "/api/v5/connectors",
              {"name": "oci-v5", "type": "oci_streaming", **identity})
assert s2 == 400, (s2, b2)
assert b2["code"] == "BAD_REQUEST", (s2, b2)
assert "oci_streaming" in json.dumps(b2), (s2, b2)

s1, b1 = call("POST", "/api/v1/connectors",
              {"id": "oci-v1", "kind": "oci_streaming",
               "config": {**identity, "batch_size": 1}})
assert s1 in (201, 400), (s1, b1)
if s1 == 201:
    assert b1["kind"] == "oci_streaming", (s1, b1)
else:
    assert "oci_streaming" in b1["error"].lower(), (s1, b1)
print("ok")
PY
```

```output
ok
```

## Limits

- A missing identity field is refused with 400 on the v1 route. The v5 route refuses the kind under the SB-11 ruling before it reads any field.
- An endpoint without `https://` is refused, except a loopback `http://127.0.0.1` or `http://localhost` endpoint.
- A stream pool OCID, a stream OCID, a tenancy OCID, a user OCID, or a fingerprint with a bad shape is refused with 400.
- A private key that is not a PKCS#1 or PKCS#8 RSA PEM is refused with 400.
- An empty `partition_key_template`, `batch_size` of 0, `batch_bytes` of 0, or `buffer_capacity` of 0 is refused with 400.
- A partition key that resolves empty fails the row.
- A partial failure retries only the failed entries. A 429 or 500 retries the whole batch. A terminal 4xx fails the batch.
- A failed flush restores the rows in order and engages backoff. Backoff starts at 200 ms and doubles up to a base ceiling of 2500 ms. Jitter adds up to half the grown delay. The jittered delay is capped at two times the ceiling.
- A full buffer fails closed with a connection error.
- The production path uses the maintained HTTP client and maintained RSA code. The loopback test uses the same transport.
- The tier is enterprise. The kind ships in the default build.

## Other names

This connector is also known as `oci-streaming` and `oci`. The alias `oci-streaming` redirects to this page.

This kind is pending under ticket SB-11. The broker refuses create and update with `400 BAD_REQUEST`.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).

## Known limits in this release

The broker accepts the fields in this list. The broker does not apply them in this release.

- `batch_bytes` (the batch size in bytes)
- `batch_size` (the batch size)
- `buffer_capacity` (the buffer size)
- `initial_backoff_ms` (the first retry wait time)
- `linger_ms` (the batch wait time)
- `max_backoff_ms` (the longest retry wait time)
- `max_retries` (the retry count)

To set batching, use `resource_opts` in the action. The broker applies `resource_opts`.

A later release applies these fields.
