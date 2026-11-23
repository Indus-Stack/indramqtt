---
title: "Snowflake connector"
description: "Stream rule results into a Snowflake channel."
sidebar:
  order: 65
---

# Snowflake connector

The Snowflake connector streams rule results through the Snowflake streaming rows API with key-pair authentication.

Use it for warehouse rows in Snowflake. The action names the database, schema and pipe.

```mermaid
flowchart LR
    Rule["rule engine"] -- "action database and pipe" --> Action["snowflake action"]
    Action -- "rows" --> Sink["snowflake sink"]
    Sink -- "key-pair JWT" --> Stream["Snowflake channel"]
```

## Before you start

- Create a Snowflake account, user and RSA key pair.
- Create the database, schema and pipe.
- This kind is not verified against the live service in the MVP.

## Settings

| Connector setting | Required | Meaning |
|---|---|---|
| `account` | yes | Snowflake account identifier. |
| `pipe_user` | yes | User for the pipe. |
| `private_key` | yes | PKCS#8 RSA private key PEM. |
| `server` | yes | Snowflake server endpoint. |

| Action setting | Required | Meaning |
|---|---|---|
| `database` | yes | Target database. |
| `schema` | yes | Target schema. |
| `pipe` | yes | Target pipe. |

Example action:

```json
{
  "name": "sf-events",
  "type": "snowflake_streaming",
  "connector": "sf-account",
  "parameters": {"database": "TELEMETRY", "schema": "PUBLIC", "pipe": "EVENTS_PIPE"}
}
```

## Limits

- A missing account, user, key, server, database, schema or pipe answers 400 and names the field.
- The reference `pipe` becomes the streaming channel name. This mapping is marked for a later parity check.
- A failed row batch counts one failed forward on the action.

## Other names

This connector is also known as `snowflake_streaming` and `snowflake_aggregated`. Old links to `/connectors/snowflake_streaming/` redirect here.

The v5 actions route lists `snowflake_streaming` as a valid action type. Action create requires connector reference, pipe, database, and schema.

## Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).

## Check that it works

Create the connector. The broker answers 201 with the kind, else 400 names the missing field. Both answers print `ok`.

```toml test
[node]
id = "docs-connector-snowflake"

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
body = json.dumps({"name": "docs-sf-1", "type": "snowflake",
                   "account": "acct", "pipe_user": "user",
                   "private_key": "not-a-key",
                   "server": "http://127.0.0.1:9"}).encode()
req = urllib.request.Request(
    f"http://127.0.0.1:{port}/api/v5/connectors", data=body, method="POST",
    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=5) as res:
        status, created = res.status, json.load(res)
except urllib.error.HTTPError as err:
    status, created = err.code, json.load(err)
assert status in (201, 400), (status, created)
if status == 201:
    assert created["type"] == "snowflake", created
else:
    assert "snowflake" in created["message"], created
print("ok")
PY
```

```output
ok
```

## Reference form (v1 API)

The reference form uses the v1 connector API. The v5 form above uses connector and action documents. This section keeps the reference page.

The `snowflake` connector writes each rule result to a Snowflake table. It buffers rows and sends one JSON row array per table group to the Snowpipe Streaming rows API. It authorizes each flush with an RS256 JWT assertion minted from a private key.

Use it when a warehouse must read broker telemetry. You get column mappings, rows grouped by table, and a bounded retry path.

```mermaid
flowchart LR
    Rule["rule engine"] -- "INTO connector id" --> Sink["snowflake sink"]
    Sink -- "buffer rows" --> Queue["batch queue"]
    Queue -- "group by table" --> Rows["JSON row array"]
    Rows -- "RS256 JWT" --> Api["channel rows endpoint"]
    Api -- "429 or 503" --> Back["backoff retry"]
    Back -- "retry rows" --> Rows
    Api -- "2xx" --> Done["offset committed"]
```

### Before you start

- Run a Snowflake account with a warehouse, a database, a schema, and a table.
- Create a key-pair user. Keep the RSA private key in PEM form.
- Note the account identifier, the user, the database, the schema, and the table.
- Create the target table, or let the table template name it.
- This kind ships in the default build. No feature flag is necessary.

### Settings

Set `kind` to `snowflake` on the v1 route. The v1 route also accepts `snow` and `sf`. Set `type` to `snowflake` on the v5 route. Both routes store the runtime as `snowflake`.

| Setting | Required | Meaning |
|---|---|---|
| `account` | yes | Snowflake account identifier, for example `xy12345.us-east-1`. An empty value is refused. |
| `user` | yes | Snowflake user. An empty value is refused. The v5 route also reads `username`. |
| `database` | yes | Target database. An empty value is refused. |
| `schema` | yes | Target schema. An empty value is refused. |
| `table_template` | yes | Table name template. It accepts `${topic}`, `${client_id}`, `${qos}`, and `${timestamp}`. The rendered name is uppercased. Characters outside `[A-Za-z0-9_]` become `_`. An empty result is refused. The v5 route also reads `table`. |
| `private_key_pem` | yes | RSA private key in PEM form. An empty or unparsable key is refused. The v5 route also reads `private_key`. |
| `endpoint` | no | Endpoint override with `http://` or `https://`. Default is `https://{account}.snowflakecomputing.com`. The v5 route also reads `url`. |
| `role` | no | Snowflake role. The sink sends it as `X-Snowflake-Role` when set. Default none. The v5 route fixes it to none. |
| `channel` | no | Streaming channel name. Default `INDRA_CHANNEL`. An empty value is refused. The v5 route fixes `INDRA_CHANNEL`. |
| `column_mappings` | no | Object of column name to value template. Each value accepts `${topic}`, `${client_id}`, `${qos}`, `${timestamp}`, and `${payload.<field>}`. Default empty, which uses the payload object keys. The v5 route fixes an empty map. |
| `batch_size` | no | Rows per flush. Default 500. A value of 0 is refused. `null` means no row limit. The v5 route fixes 1. |
| `batch_bytes` | no | Byte limit for one buffer. Default 4194304 (4 MiB). A value of 0 is refused. `null` means no byte limit. The v5 route fixes 4194304. |
| `linger_ms` | no | Age in ms that makes a buffered row stale. Default 20. `null` means no time bound. The v5 route fixes 10. |
| `max_retries` | no | Retries of one failed batch. Default 4. `null` means no retry limit. A value of 0 means no retry. The v5 route fixes 3. |
| `initial_backoff_ms` | no | Base retry delay in ms. Default 100. A value below 1 reads as 1. The v5 route fixes 100. |
| `max_backoff_ms` | no | Retry delay ceiling in ms. Default 3000. A value below 1 reads as 1. The v5 route fixes 2000. |
| `timeout_ms` | no | Request deadline in ms. Default 5000. A value below 1 reads as 1. The v5 route also reads `timeout`. |

The v1 route reads the full settings above. The v5 route reads the account, the user, the database, the schema, the table, the key, the endpoint, and the timeout. It fixes the role, the channel, the column map, and the batch policy.

Without `column_mappings`, an object payload becomes one column per key. A non-object payload becomes one `VALUE` column. Column names are uppercased. Every row carries `_MQTT_TOPIC` and `_MQTT_TIMESTAMP`. A mapped value that parses as a JSON number, boolean, or null keeps that type. Other text is a string.

The request goes to `{endpoint}/v1/data/streaming/channels/{channel}/rows`. It carries a Bearer JWT with claims `iss` and `sub` equal to `{ACCOUNT}.{USER}`. The token expires after 3600 seconds. The body is a JSON array of row objects. The sink sends one call per table group in a flush.

Example:

```json
{
  "id": "snowflake-1",
  "kind": "snowflake",
  "config": {
    "account": "xy12345.us-east-1",
    "user": "indra_loader",
    "database": "IOT",
    "schema": "PUBLIC",
    "table_template": "TELEMETRY_${topic}",
    "private_key_pem": "-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----",
    "role": "LOADER",
    "channel": "DOCS_CHANNEL",
    "column_mappings": {
      "device_id": "${client_id}",
      "temperature": "${payload.temperature}"
    },
    "batch_size": 1,
    "batch_bytes": 4194304,
    "linger_ms": 10,
    "max_retries": 3,
    "initial_backoff_ms": 100,
    "max_backoff_ms": 2000,
    "timeout_ms": 5000
  }
}
```

> [!NOTE]
> The docs run creates this connector on both routes. It sends one message through a forwarding rule to a local receiver. The real Snowflake service, its authentication, and its row ingest are not verified. See the record in [Verification failures](../_verification/failures.md).

### Check that it works

The test starts a local rows receiver. It creates the connector on the v1 and v5 routes. It creates a forwarding rule and publishes one message. The receiver checks the channel path, the JWT claims, the role header, and the row body.

```toml test
# Reference form run.
[node]
id = "docs-connector-snowflake"

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
import base64, json, os, socket, struct, threading, time, urllib.request, urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer

received = []

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length)
        received.append((self.path,
                         {k.lower(): v for k, v in self.headers.items()}, body))
        out = b"{}"
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)
    def log_message(self, *args):
        pass

server = HTTPServer(("127.0.0.1", 0), Handler)
snow_port = server.server_address[1]
threading.Thread(target=server.serve_forever, daemon=True).start()

mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
api_port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]

pem = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCgQlU8hDdoMjP5
QU2fhr0g+2n5HQSvgQaDKkpZFftqbrMixmFg3pGQN+GZp9vIscT+BlNrJueigXpn
pRbhI0RRj8EvVl+4Or+0hdzeLDmfGl/9SIhnyiRsJ7YDeO3uZq/Cff2zMeXqqbk4
RgKwbQksnJPOOYgVOfPrLjHJdEkNcSxT44tyOVrhBVISYm+zUw4By4GSQXp4RoTa
8UFX/gHoa+31um/9yZfDf9ekzelBys+4iSBeJ6imdStCjt8K+71yxewMSMD6HiCj
2GWvp+mPixqf4GcwaiqoFgJj+IKspc3eyozqJY/+610aaSw/ooO2AFXfErJlJEBP
H5hITBUlAgMBAAECggEAErJqe1r5k+B3i9cAlWIE4royTOwDxe4JsnfWoLod0PcF
U0NNzR1qYicC3Qhmbe2/i9t1FAU/9QeiHkF2f+G7cMCSy1EKbdX807TiZdFHD7bm
CAjUUTeWNEAVziXnrG6yhsBoPuXNaylOALC6U5cFAP1riR3RMJjISmHjURuOAlFI
W8tlKq77I5a4L93IW+2/elDPTjhUYsQnSEtJPWG/BizSVihHSiHh2lAN0JLZChWk
6J2e2FaZYC28Swu/V+GLXLg0Ai8GkSZNYTqOf8HqnkB7X3G7+OMnLPW+0I5GPOh+
9aX9WXhGAI6gxJf6UjcD5axHV+mdfgQnxBsJY4HxqQKBgQDfUbtYzfLBxnnxk0H7
N+lhdiANA/YXiR4OljziTSTjnnSDdabktr3ubIofpEwjvAqKIdn5ruyqdT/9GOZt
/7sai0aqyIzGlQiLhkxHvHBIxGajRBbPq2BhLqQYBeL0Xy4oMcD5NjGoUuhcck0b
bw6h935CIJYzJQtx+K/U4I2DwwKBgQC3timBQPbq+wJx4yuyf+VOy4r9qW0/4DUM
pw3qo1tqOk1hKN6pZazov0qfrGEKIOG4Ws0rLCKgwKwocdfFfzPwTgg8srl5r5XM
k5r97mHNYqSlboAE2YIM+CziUmVqklkMqQ4Hs38jk3tswt6yY+syrfZbp/Rhh/XK
pVv1itr89wKBgQDBi1VihsNw+7IuE2Eo9/E1faoTfa5oAXdiTwUfYJqrB2aVlH77
VAHSRJGFEODIS62av/Hpepg0t3+ovE7hYLTpMXIii8OuS/Xm7pLnzUJHXqhRsa5P
d4kFUOX4yAlFn8QiI9TKaBSrfIdTr+Bx+VNmPlhXuWRTmTSNJ2pEhgU//wKBgAt7
Vxy88rG8/mofyJtfYvWJwyYXcLyNRsODrVr82rnI6w0ngMMVl7j0O7W/EFGRvInJ
IwmPuJpTcG8WrmWpjZV3SwyAHxd74eDnWMiGHZa4k5HDVjz3Wyl0WVnLzIrcmrQv
3LCeh1Ox5AToKQL9O7XvKXaRCLUPykzgCN9PzmABAoGAUW/AIrYerL0oU0KBRZ5B
6NX+5++R6jugN/spvVs4OLwPM5a6ud6Bq/+BA3aT7NWdfypVgybotEytsb/y1oe2
XdFhno110rcMp6WoQHM4dCWw3cmRNbMv2aleY2FrTZlpxEXeA47iF5/if1VHldmR
Y7LzJJ6LCjfUFy8dMINZC7M=
-----END PRIVATE KEY-----
"""

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
    "account": "xy12345.us-east-1",
    "user": "docs_loader",
    "database": "IOT",
    "schema": "PUBLIC",
    "private_key_pem": pem,
    "endpoint": f"http://127.0.0.1:{snow_port}",
    "timeout_ms": 10000,
}
v1_config = {
    **common,
    "table_template": "TELEMETRY_${topic}",
    "role": "LOADER",
    "channel": "DOCS_CHANNEL",
    "column_mappings": {
        "device_id": "${client_id}",
        "temperature": "${payload.temperature}",
    },
    "batch_size": 1,
    "batch_bytes": 4194304,
    "linger_ms": 10,
    "max_retries": 3,
    "initial_backoff_ms": 100,
    "max_backoff_ms": 2000,
}

s1, b1 = call("POST", "/api/v1/connectors",
              {"id": "snow-v1", "kind": "snowflake", "config": v1_config})
assert s1 == 201 and b1["kind"] == "snowflake", (s1, b1)

s2, b2 = call("POST", "/api/v5/connectors",
              {"name": "snow-v5", "type": "snowflake",
               **{**common, "table": "TELEMETRY_DOCS"}})
assert s2 == 201 and b2["type"] == "snowflake", (s2, b2)

s3, b3 = call("POST", "/api/v5/connectors",
              {"name": "snow-bad", "type": "snowflake",
               **{k: v for k, v in common.items() if k != "private_key_pem"},
               "table": "TELEMETRY_DOCS"})
assert s3 == 400 and "private_key_pem" in json.dumps(b3), (s3, b3)

s4, b4 = call("POST", "/api/v1/rules",
              {"name": "snow-rule", "topic_filter": "docs/snow/in",
               "actions": [{"type": "forwardconnector",
                            "connector_id": "snow-v1"}]})
assert s4 == 201, (s4, b4)

def packet(kind, body):
    return bytes([kind, len(body)]) + body

def field(data):
    return struct.pack(">H", len(data)) + data

pub = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
pub.sendall(packet(0x10, b"\x00\x04MQTT\x04\x02\x00\x3c" + field(b"snow-pub")))
assert pub.recv(4) == b"\x20\x02\x00\x00"
payload = b'{"temperature": 21.5, "client_id": "sensor-1"}'
pub.sendall(packet(0x30, field(b"docs/snow/in") + payload))

deadline = time.time() + 15
while not received and time.time() < deadline:
    time.sleep(0.1)
assert received, "no rows request arrived"
path, headers, body = received[0]
assert path == "/v1/data/streaming/channels/DOCS_CHANNEL/rows", path
assert headers.get("x-snowflake-role") == "LOADER", headers
assert headers.get("content-type") == "application/json", headers
auth = headers.get("authorization", "")
assert auth.startswith("Bearer "), auth
token = auth[len("Bearer "):]
parts = token.split(".")
assert len(parts) == 3, parts
segment = parts[1] + "=" * (-len(parts[1]) % 4)
claims = json.loads(base64.urlsafe_b64decode(segment))
assert claims["iss"] == "XY12345.US-EAST-1.DOCS_LOADER", claims
assert claims["sub"] == claims["iss"], claims
assert claims["exp"] - claims["iat"] == 3600, claims
rows = json.loads(body)
assert len(rows) == 1, rows
row = rows[0]
assert row["DEVICE_ID"] == "sensor-1", row
assert row["TEMPERATURE"] == 21.5, row
assert row["_MQTT_TOPIC"] == "docs/snow/in", row
print("delivered")
PY
```

```output
delivered
```

### Limits

- An empty account, user, database, schema, table, or key is refused with 400.
- A private key that is not an RSA PEM is refused at creation.
- An endpoint without `http://` or `https://` is refused. An empty channel is refused.
- A payload must be UTF-8 JSON. A non-UTF-8 or non-JSON payload fails the row.
- The table name is uppercased. A character outside `[A-Za-z0-9_]` becomes `_`.
- The sink groups rows by table. A flush sends one rows call per group. It commits one offset token per flush after all groups succeed.
- 429, 503, and transport faults retry the batch up to `max_retries` times. The first retry waits at least 200 ms. The delay doubles to the ceiling and adds jitter.
- Other non-2xx statuses are terminal. A failed batch restores its rows in order and engages backoff.
- The connector runs behind the rule engine bounded queue. A slow warehouse never stalls publishers.
- The tier is enterprise. The kind ships in the default build.

### Next steps

- Feed the connector from a rule in [Actions](../rules/actions.md).
- See every kind in [Connectors](./index.md).

## Known limits in this release

The broker accepts the fields in this list. The broker does not apply them in this release.

- `batch_bytes` (the batch size in bytes)
- `batch_size` (the batch size)
- `initial_backoff_ms` (the first retry wait time)
- `linger_ms` (the batch wait time)
- `max_backoff_ms` (the longest retry wait time)
- `max_retries` (the retry count)

To set batching, use `resource_opts` in the action. The broker applies `resource_opts`.

A later release applies these fields.
