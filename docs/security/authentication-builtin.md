---
title: Built-in database example
description: Worked example that creates a user and connects.
sidebar:
  order: 33
---

# Built-in database example

The built-in database holds MQTT users in the broker. It needs no external server. Use it for fleets that the broker manages alone. It stores users, checks passwords, and delivers messages.

```mermaid
sequenceDiagram
    participant Ops as Operator
    participant Api as Management API
    participant Kernel as Kernel
    Ops->>Api: POST /api/v1/auth/users sensor-01
    Api-->>Ops: 201
    Ops->>Kernel: CONNECT sensor-01 + password
    Kernel-->>Ops: CONNACK ok or denied
```

## Before you start

- Read [Authentication](./authentication.md).
- Keep ports 1883 and 18083 free.
- Pick a password with a letter and a digit.

## Worked example

Start with a closed broker.

```toml test
[node]
id = "docs-builtin"

[listeners.tcp]
bind = "0.0.0.0:1883"

[listeners.ws]
bind = "0.0.0.0:8083"
path = "/mqtt"

[listeners.api]
bind = "0.0.0.0:18083"

[logging]
level = "warn"

[auth]
enabled = true
allow_anonymous = false
```

Validate the directory before you start. The command prints `ok`.

```sh test
"${INDRAMQTT_BIN:-indramqtt}" --config-dir "${DOCS_CFG_DIR:-./docs-start}" --check-config
```

```output
ok
```

## Check that it works

Create the user `sensor-01`. The call answers 201. It also grants publish and subscribe access to `sensors/01`.

```sh test
python3 - <<'PY'
import json, os, urllib.request
port = os.environ.get("API_PORT", "18083")
key = os.environ["API_KEY"]

def call(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=5) as res:
        return res.status

status = call("POST", "/api/v1/auth/users",
              {"username": "sensor-01", "password": "Sensor-docs-pw-1"})
assert status == 201
for action in ("publish", "subscribe"):
    assert call("POST", "/api/v1/auth/acls",
                {"client_pattern": "*", "action": action,
                 "topic_pattern": "sensors/01", "allow": True}) == 201
print(status)
PY
```

```output
201
```

Connect as `sensor-01`. The right password opens access. A wrong password fails. The check prints `ok` and then `denied`.

```sh test
python3 - <<'PY'
import os, socket, struct
mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))

def packet(kind, body):
    return bytes([kind, len(body)]) + body

def field(data):
    return struct.pack(">H", len(data)) + data

def try_connect(client_id, username, password):
    sock = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
    sock.settimeout(5)
    body = (b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id)
            + field(username) + field(password))
    sock.sendall(packet(0x10, body))
    try:
        reply = sock.recv(4)
    except OSError:
        return None
    return sock if reply == b"\x20\x02\x00\x00" else None

topic = b"sensors/01"
sub = try_connect(b"docs-sensor-sub", b"sensor-01", b"Sensor-docs-pw-1")
assert sub is not None
sub.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
assert sub.recv(5)[:1] == b"\x90"
pub = try_connect(b"docs-sensor-pub", b"sensor-01", b"Sensor-docs-pw-1")
assert pub is not None
pub.sendall(packet(0x30, field(topic) + b"hello-sensor"))
sub.settimeout(5)
assert sub.recv(100).endswith(b"hello-sensor")
print("ok")
print("denied" if try_connect(b"docs-sensor-bad", b"sensor-01", b"wrong-password") is None else "open")
PY
```

```output
ok
denied
```

With zero users the broker is open. Once a user exists, anonymous CONNECT fails while users exist, unless the broker runs with `--allow-anonymous`.

## Limits

- Stored verifiers cover legacy SHA-256 hex, bcrypt, PBKDF2 and Argon2id. New passwords hash under the configured policy.
- Bcrypt passwords hold at most 72 bytes. The bcrypt primitive truncates input at 72 bytes, so longer values fail closed rather than verify by prefix.
- The broker holds at most 100000 users. One import batch holds at most 10000 entries.
- Concurrent expensive checks share one verification per credential. 32 concurrent checks cap memory and CPU from memory-hard verification. Past 32 concurrent checks new CONNECTs fail closed.
- Registry writes persist across restarts. A failed save answers HTTP 500 and never stays silent.

## Next steps

- Grant topics with [Authorization](./authorization.md).
- Use tokens with the [JWT example](./authentication-jwt.md).
- Manage operators with [Management users](./management-access.md).
