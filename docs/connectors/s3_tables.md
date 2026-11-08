---
title: "Amazon S3 Tables connector"
description: "Write rule results to an Amazon S3 Tables table as Iceberg data files."
sidebar:
  order: 107
---

# Amazon S3 Tables connector

The `s3_tables` connector writes each rule result to an Amazon S3 Tables table. It buffers rows and uploads one gzip NDJSON data file per partition group, under an Iceberg-style `data/` path. It signs each request with AWS Signature Version 4.

Use it when a lakehouse must read broker telemetry. You get partitioned data files, a JSON snapshot for each commit, and a bounded retry path.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["s3_tables sink"]
    Sink -- "buffer rows" --> Queue["batch queue"]
    Queue -- "partition rows" --> Gzip["gzip NDJSON"]
    Gzip -- "PUT data file" --> Store["S3 Tables bucket"]
    Store -- "429 or 5xx" --> Back["backoff retry"]
    Back -- "retry file" --> Gzip
```

## Before you start

- Create an S3 Tables table bucket and an Iceberg namespace and table.
- Keep an access key pair. Add a session token when the credentials are temporary.
- Note the table bucket ARN, the namespace, the table name, and the region.
- This kind ships in the default build. No feature flag is necessary.

## Settings

Set `kind` to `s3_tables` on the v1 route. The v1 route also accepts `s3tables` and `iceberg`. Set `type` to `s3_tables` on the v5 route. The v5 route also accepts `s3tables`. Both routes store the runtime as `s3_tables`.

| Setting | Required | Meaning |
|---|---|---|
| `table_bucket_arn` | yes | Table bucket ARN of the form `arn:aws:s3tables:{region}:{12-digit account}:bucket/{name}`. A bad shape is refused. The v5 route also reads `bucket_arn` and `arn`, and defaults to a sample ARN. |
| `namespace` | yes | Iceberg namespace. An empty value is refused. The v5 route defaults to `production_iot`. |
| `table_name` | yes | Target table. An empty value is refused. The v5 route also reads `table` and defaults to `device_events`. |
| `region` | yes | AWS region. An empty value is refused. The v5 route defaults to `us-east-1`. |
| `access_key_id` | yes | AWS access key id. An empty value is refused. The v5 route defaults to `test`. |
| `secret_access_key` | yes | AWS secret access key. An empty value is refused. The v5 route defaults to `test`. |
| `session_token` | no | STS session token for temporary credentials. Default none. The v5 route fixes it to none. |
| `endpoint` | no | Custom endpoint with a scheme. Default is the S3 Tables service for the region. The v5 route also reads `url`. |
| `partition_spec` | no | Ordered partition fields. Each field has `source_name` and `transform`. A field with an empty source is refused. Default empty. The v5 route fixes it to empty. |
| `target_format` | no | `ndjsoncompressed` for gzip NDJSON, or `parquetplaceholder`. Default `ndjsoncompressed`. The v5 route fixes `ndjsoncompressed`. |
| `batch_size` | no | Rows per sink batch. Default 1000. A value of 0 is refused. `null` means no item limit. The v5 route fixes 1. |
| `buffer_capacity` | no | Buffered row limit. Default none, so no explicit row cap. A set capacity fails closed when full. The v5 route fixes it to none. |
| `linger_ms` | no | Linger flush window in ms. Default 1000. `null` uses the 1000 ms default. The v5 route fixes 10. |
| `timeout_ms` | no | Request deadline in ms. Default 5000. A value below 1 reads as 1. The v5 route also reads `timeout` and `request_timeout_ms`. |

The v1 route reads the full settings above. The v5 route reads the table bucket ARN, the namespace, the table, the region, the endpoint, the credentials, and the timeout. It fixes the codec, the partition spec, and the batch policy.

The partition transforms are:

| Transform | Path segment |
|---|---|
| `identity` | `source=value` |
| `year` | `source_year=YYYY` |
| `month` | `source_month=YYYY-MM` |
| `day` | `source_day=YYYY-MM-DD` |
| `hour` | `source_hour=YYYY-MM-DD-HH` |

A `date` source reads the record time. Other sources read `device_id`, `client_id`, or `topic`. An empty value becomes `__null__`. A value keeps `[A-Za-z0-9._=-]`; every other character becomes `_`.

Example:

```json
{
  "id": "s3-tables-1",
  "kind": "s3_tables",
  "config": {
    "table_bucket_arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/telemetry-bucket",
    "namespace": "production_iot",
    "table_name": "device_events",
    "region": "us-east-1",
    "access_key_id": "AKID",
    "secret_access_key": "secret",
    "partition_spec": [
      { "source_name": "date", "transform": "day" },
      { "source_name": "device_id", "transform": "identity" }
    ],
    "target_format": "ndjsoncompressed",
    "batch_size": 1000,
    "linger_ms": 1000,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates the connector on both routes. It sends one message to a local S3 receiver. The receiver checks the data path, the gzip body, and the signature header. The real S3 Tables service, the table commit, and the signature check are not verified. See the record in [Verification failures](../_verification/failures.md).

## Check that it works

The test starts a local S3 receiver. It creates the connector on the v1 and v5 routes. It creates a forwarding rule and publishes one message. It reads the gzip data file back. The receiver prints `delivered` when the file arrives.

```toml test
[node]
id = "docs-connector-s3-tables"

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
import gzip, json, os, socket, struct, threading, time, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

received = []

class Handler(BaseHTTPRequestHandler):
    def do_PUT(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        received.append((self.path, {k.lower(): v for k, v in self.headers.items()}, body))
        out = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)
    def log_message(self, *args):
        pass

server = HTTPServer(("127.0.0.1", 0), Handler)
store_port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()

mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
api_port = int(os.environ.get("API_PORT", "18083"))
key = os.environ["API_KEY"]

def call(method, path, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{api_port}{path}", data=data, method=method,
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

common = {
    "table_bucket_arn": "arn:aws:s3tables:us-east-1:123456789012:bucket/telemetry-bucket",
    "namespace": "production_iot",
    "table_name": "device_events",
    "region": "us-east-1",
    "endpoint": f"http://127.0.0.1:{store_port}",
    "access_key_id": "AKID",
    "secret_access_key": "secret",
    "timeout_ms": 10000,
}

v1_config = {
    **common,
    "partition_spec": [
        {"source_name": "date", "transform": "day"},
        {"source_name": "device_id", "transform": "identity"},
    ],
    "target_format": "ndjsoncompressed",
    "batch_size": 1,
    "linger_ms": 10,
}

s1, b1 = call("POST", "/api/v1/connectors",
              {"id": "s3t-v1", "kind": "s3_tables", "config": v1_config})
assert s1 == 201 and b1["kind"] == "s3_tables", (s1, b1)

s2, b2 = call("POST", "/api/v5/connectors",
              {"name": "s3t-v5", "type": "s3_tables", **common})
assert s2 == 201 and b2["type"] == "s3_tables", (s2, b2)

s3, b3 = call("POST", "/api/v5/connectors",
              {"name": "s3t-bad", "type": "s3_tables",
               **{**common, "table_bucket_arn": "not-an-arn"}})
assert s3 == 400, (s3, b3)

s4, b4 = call("POST", "/api/v1/rules",
              {"name": "s3t-rule", "topic_filter": "docs/st/in",
               "actions": [{"type": "forwardconnector",
                            "connector_id": "s3t-v1"}]})
assert s4 == 201, (s4, b4)

def packet(kind, body):
    return bytes([kind, len(body)]) + body

def field(data):
    return struct.pack(">H", len(data)) + data

pub = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
pub.sendall(packet(0x10, b"\x00\x04MQTT\x04\x02\x00\x3c" + field(b"s3t-pub")))
assert pub.recv(4) == b"\x20\x02\x00\x00"
payload = b'{"value": 9, "device_id": "sensor-42"}'
pub.sendall(packet(0x30, field(b"docs/st/in") + payload))

deadline = time.time() + 15
while not received and time.time() < deadline:
    time.sleep(0.1)
assert received, "no data file arrived"
path, headers, body = received[0]
parts = path.strip("/").split("/")
assert parts[0] == "data", path
assert len(parts) == 4, path
assert parts[1].startswith("date_day="), path
assert parts[2] == "device_id=sensor-42", path
assert parts[3].endswith(".data.gz"), path
assert headers.get("content-encoding") == "gzip", headers
assert headers.get("content-type") == "application/x-ndjson", headers
assert headers.get("authorization", "").startswith("AWS4-HMAC-SHA256"), headers
line = gzip.decompress(body).decode().strip()
row = json.loads(line)
assert row["topic"] == "docs/st/in", row
assert row["qos"] == 0, row
assert row["payload"]["value"] == 9, row
print("delivered")
PY
```

```output
delivered
```

## Limits

- The v1 route refuses a bad table bucket ARN, an empty namespace, and an empty table name with 400. It also refuses an empty region, an empty access key id, and an empty secret access key.
- A partition field with an empty `source_name` is refused. A `batch_size` of 0 is refused.
- `identity` partitions read only `device_id`, `client_id`, and `topic`. A missing value becomes `__null__`.
- A payload that is not UTF-8 fails the row. A non-JSON payload is stored as a JSON string.
- The v5 route fixes one row per batch, gzip NDJSON, no session token, and no partition spec. It uses the local override only when you set `endpoint`.
- A custom endpoint takes `PUT {scheme}://{host}/{key}`. The production path first reads the warehouse bucket from `GET /get-table`, then writes to the S3 data host.
- The sink stores the JSON snapshot document for each commit in memory. It does not upload a metadata file.
- A 403 and other 4xx faults are terminal. A 429 or a 5xx fault retries with backoff. A failed flush restores the unsent rows in order and engages backoff.
- A full `buffer_capacity` fails closed with a connection error. The v5 route leaves the capacity unset.
- The tier is enterprise. The kind ships in the default build.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).
