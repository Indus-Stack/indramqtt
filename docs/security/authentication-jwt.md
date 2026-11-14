---
title: JWT example
description: Worked example that checks bearer tokens against JWKS.
sidebar:
  order: 34
---

# JWT example

JWT authentication suits clients that already hold tokens. The client sends the token as the MQTT password. The broker checks the signature against a JWKS endpoint. Use it when an issuer already signs device tokens. It uses tokens instead of passwords. The issuer manages key rotation.

```mermaid
sequenceDiagram
    participant Dev as Device
    participant Kernel as Kernel
    participant Iss as JWKS endpoint
    Dev->>Kernel: CONNECT + token as password
    Kernel->>Iss: fetch keys (cached 300 s)
    Iss-->>Kernel: key set
    Kernel->>Kernel: check signature, exp, iss, aud
    Kernel-->>Dev: CONNACK ok or denied
```

## Before you start

- Read [Authentication](./authentication.md).
- Run an issuer with a JWKS endpoint over HTTPS.
- Keep ports 1883 and 18083 free.

## Worked example

Point the broker at the JWKS endpoint of the issuer.

```toml test
[node]
id = "docs-jwt"

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
allow_anonymous = true

[jwks]
url = "https://issuer.example.com/.well-known/jwks.json"
issuer = "https://issuer.example.com/"
audience = "mqtt-clients"
clock_skew_secs = 60
```

Validate the directory before you start. The command prints `ok`.

```sh test
"${INDRAMQTT_BIN:-indramqtt}" --config-dir "${DOCS_CFG_DIR:-./docs-start}" --check-config
```

```output
ok
```

## Check that it works

Connect with a JWT-shaped password while the issuer is unreachable. The broker fails closed and denies the CONNECT. The check prints `denied`.

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

token = b"eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJkZXYifQ.c2ln"
print("denied" if try_connect(b"docs-jwt-bad", b"device-01", token) is None else "open")
PY
```

```output
denied
```

Make a loopback issuer for the positive check. The test writes a test CA and key to `/tmp`. The CA is test-only. The next broker trusts the CA. The command prints `ok`.

```sh test
python3 - <<'PY'
import pathlib
cert = "-----BEGIN CERTIFICATE-----\nMIIDJTCCAg2gAwIBAgIUHGyuPek7PZ/bqQKavEGif5TDKBwwDQYJKoZIhvcNAQEL\nBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkyNDAwMzAwNVoXDTM2MDky\nMTAwMzAwNVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\nAAOCAQ8AMIIBCgKCAQEAtdkihZVps667YXbUFntwwvjsoiWqrXqZmHuSh24KpDuu\nvvfzM+VuaO0O52OMJkDCbZE2GZz7mSQDaVNRfYG2z4MhQa95x4+AOBIA0f695Frh\n4plsm9zOnHdSCm/UD1UrkAvfwDuxC7vdEcsVtXAH024Y8O5+ogzcorjDwTB2BTLV\n6PLnPfwDgdsIkvH93dE2CNZ701fsxyepGa/hBEUx0IrYLroOxGZRXEs6fViGdVO4\nMffBZ9qFHJvRQNEf1UXscqkRPA5JkIQfbLw4xaGcbf1HB8ce42gD8GtMIS3czADi\nTzcbrd7SXq8alw73TW3DgLvuxypxDFvzI+59xLhqLQIDAQABo28wbTAdBgNVHQ4E\nFgQUrTQoqWVIzqrZqiKz6XbHAAx0eCowHwYDVR0jBBgwFoAUrTQoqWVIzqrZqiKz\n6XbHAAx0eCowDwYDVR0TAQH/BAUwAwEB/zAaBgNVHREEEzARhwR/AAABgglsb2Nh\nbGhvc3QwDQYJKoZIhvcNAQELBQADggEBAJPAiEsiOQylm9mooNhwchbZR60RLWM4\nHOQ2k1MEtX8BWpfHEFpgmlLKCDfaMlLSoTl5JHsZdinDAXoTexrN8ifMx9fOQttO\nQF50qYweJzkjOsmZmZd522t6covj7+odIG/FEqfXQHa9BisoFPox74B1GyDYd9K2\nBORFO61WQZhhtSltUSyhXAWLgxJTaqJWsSavf9qYWPs5oeOvs4aLvBzLhtx12NLI\nJL9Qi5rzXxzal7vqZYmiRMq8pH3Gi7DKpjScwRd/to07AetvvOGoyfdIcZS8vlF0\n16i1jz/RPXS8eTPl/XmQBz0bXYpTVJGkcx5lnGZLma7oNXCP+dp/v8s=\n-----END CERTIFICATE-----\n"
key = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC12SKFlWmzrrth\ndtQWe3DC+OyiJaqtepmYe5KHbgqkO66+9/Mz5W5o7Q7nY4wmQMJtkTYZnPuZJANp\nU1F9gbbPgyFBr3nHj4A4EgDR/r3kWuHimWyb3M6cd1IKb9QPVSuQC9/AO7ELu90R\nyxW1cAfTbhjw7n6iDNyiuMPBMHYFMtXo8uc9/AOB2wiS8f3d0TYI1nvTV+zHJ6kZ\nr+EERTHQitguug7EZlFcSzp9WIZ1U7gx98Fn2oUcm9FA0R/VRexyqRE8DkmQhB9s\nvDjFoZxt/UcHxx7jaAPwa0whLdzMAOJPNxut3tJerxqXDvdNbcOAu+7HKnEMW/Mj\n7n3EuGotAgMBAAECggEAIsdSWOYIfzrtz2ggi+Qz3rYo26IEkIUgFw+bKJedJWfc\ntd1KACTjBuI/tXVOeopsJPReumtRmypOFLjAnxZN1kYn+B4NVmNVjGO1EHR98MyI\n4wOgx/Zk9XvEjwZwMjaBzFzZADTqWWomj56dmkPA22j1EC8svOVk1SItHiecisWp\noGSNJ/gDkAx0kK3c5YFobBAZZyP4nIIdpXBiAHypk4DT1cSIHEy4dFN9JVzLP/0k\nmD7jsovLvn4R8a6lSnMvescUoliHqE7kTOTd3OSLiXri+EjfvkS61zyjEB9DL+pH\ni4dVxWUJUBDnWUbNFqhYaJ03pXnXGkRbTlAJfe286QKBgQDdinXuCNHUTzQfDA5C\nkpnXndj1Sl+7f7aKSK1iLzf0Iy9X8AO9gamxNa68b3uY0dSnE1eICkOEqfcM64sI\nhV5wcR0tLER7fAIBkhzZZh3zlvYUn99WSb4QjR7oAUlLT0OSsoFidOaXXiTL1GQR\nndLplurj1mCZIu4KCpgFXNbhTwKBgQDSIiaWV71p0JLNUWjd592VnZdqCbztAsKR\n5ZjUrchJZb23F8aT6LrQFgceaNWvfdNd+AjB71la1y0xfIR9AOSLcprQDrPUBuT1\nYIrPYrunkrhZbvm3N1S3oGtESayzfYbJ6urh2i8JBXqC1s2SCdoFzmojmWHfBHEG\nFohZz4fFwwKBgQDYjEsRzVUtLe5ImsQllp8B/6zet0A0SnXnXXr9CiKrZOkWD+nY\nBzIToeGXF3G8wv4WAfYBZ+bveiOeYW6ZeaQCTM60JR0bhu9/EY9ZgVOtktYe+taX\nxaUfEJIxPXCjSGtIrWuDDbmII+Hby1O1VIuhAH/BDP+HMHl1Hz3RoBn6SQKBgErc\nh8q/72cnO2WSPz3vQO3weuT4GyqE9TRtC4mZb+VWLcRw3/oJy6QedOLMjnQ663Zq\nyxPsZXULe7pJlhnCm6liZu0Aj+hVnHQetNU1Y41LpAmYk7ZGLBRPPmfRp4k6iy6c\nVpmn1WHtZbv/MrV4dQfkhcOw/UEqn+l/VYxJdyFpAoGBAI+8qzjBnHSDUr3uDX7e\n21MBoYGPzGlYwom5zEhNM2xbHvZ6Dt+c5aB308q4Wq88At6eLFtFAaTJwNFEgz/3\n/yp8G6egdnUbScWzpG5qQQ4LiBjjzHrP7Y1LGWMBeDuaD74M2uByICuEyZmJUTTT\n2rpyA7NR3vcZpXi/ZeWghtZb\n-----END PRIVATE KEY-----\n"
pathlib.Path("/tmp/docs-jwks-ca.pem").write_text(cert)
pathlib.Path("/tmp/docs-jwks-key.pem").write_text(key)
print("ok")
PY
```

```output
ok
```

Point a second broker at the loopback issuer. The broker trusts the test CA.

```toml test
[node]
id = "docs-jwt-live"

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
allow_anonymous = true

[jwks]
url = "https://127.0.0.1:18543/jwks.json"
issuer = "https://issuer.example"
audience = "indra-mqtt"
clock_skew_secs = 60
ca_cert = "/tmp/docs-jwks-ca.pem"
```

Connect with a valid token from the live loopback issuer. The broker checks the signature against the live endpoint. The test connects, publishes and delivers through the broker. The issuer is a real HTTPS server on loopback with real RSA keys. The check prints `ok`.

```sh test
python3 - <<'PY'
import base64, http.server, json, os, socket, ssl, struct, subprocess, tempfile, threading, time
mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
def b64u(data):
    return base64.urlsafe_b64encode(data).decode().rstrip("=")
def packet(kind, body):
    head = bytes([kind])
    left = len(body)
    while True:
        part = left % 128
        left //= 128
        if left > 0:
            part |= 0x80
        head += bytes([part])
        if left == 0:
            break
    return head + body
def field(data):
    return struct.pack(">H", len(data)) + data
tmp = tempfile.mkdtemp()
sign_key = os.path.join(tmp, "sign.key")
subprocess.run(["openssl", "genrsa", "-out", sign_key, "2048"], check=True, capture_output=True)
mod_hex = subprocess.run(["openssl", "rsa", "-in", sign_key, "-modulus", "-noout"], capture_output=True, text=True, check=True).stdout.strip().split("=")[1]
n_b64 = b64u(bytes.fromhex(mod_hex))
e_b64 = b64u(bytes.fromhex("010001"))
jwks = {"keys": [{"kty": "RSA", "kid": "docs-key-1", "use": "sig", "alg": "RS256", "n": n_b64, "e": e_b64}]}
class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(jwks).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)
    def log_message(self, *args):
        pass
server = http.server.HTTPServer(("127.0.0.1", 18543), Handler)
context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
context.load_cert_chain("/tmp/docs-jwks-ca.pem", "/tmp/docs-jwks-key.pem")
server.socket = context.wrap_socket(server.socket, server_side=True)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
time.sleep(0.5)
now = int(time.time())
header = b64u(json.dumps({"alg": "RS256", "kid": "docs-key-1", "typ": "JWT"}, separators=(",", ":")).encode())
payload = b64u(json.dumps({"sub": "device-01", "iss": "https://issuer.example", "aud": "indra-mqtt", "iat": now, "exp": now + 3600}, separators=(",", ":")).encode())
signing_input = f"{header}.{payload}".encode()
signature = subprocess.run(["openssl", "dgst", "-sha256", "-sign", sign_key], input=signing_input, capture_output=True, check=True).stdout
token = f"{header}.{payload}.{b64u(signature)}".encode()
def try_connect(client_id, username, password):
    sock = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
    sock.settimeout(5)
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(password)
    sock.sendall(packet(0x10, body))
    try:
        reply = sock.recv(4)
    except OSError:
        return None
    return sock if reply == b"\x20\x02\x00\x00" else None
topic = b"docs/jwt-live"
sub = try_connect(b"docs-jwt-live-sub", b"device-01", token)
assert sub is not None
sub.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
assert sub.recv(5)[:1] == b"\x90"
pub = try_connect(b"docs-jwt-live-pub", b"device-01", token)
assert pub is not None
pub.sendall(packet(0x30, field(topic) + b"hello-jwt-live"))
sub.settimeout(5)
assert sub.recv(100).endswith(b"hello-jwt-live")
server.shutdown()
print("ok")
PY
```

```output
ok
```

An expired token fails. A forged token fails. A token without a key id fails. The broker logs each denial.

## Limits

- An empty `jwks.url` disables JWT. A non-HTTPS URL refuses startup.
- The broker accepts RS256 and ES256 only. Other algorithms fail closed.
- The cache holds at most 32 keys. Each entry lives 300 seconds. Background refresh runs every 300 seconds. Five minutes bound key staleness, and the unknown-key trigger covers fast rotation.
- A CONNECT waits at most 5000 ms for a refresh. Five seconds tolerate a slow endpoint and keep a stalled accept bounded. A fetch fault marks the endpoint unhealthy and refuses even cache hits.
- Tokens tolerate 60 seconds of clock skew. One minute covers issuer and broker drift. `exp`, `iss` and `aud` must all check out.

## Next steps

- Hold passwords with the [built-in example](./authentication-builtin.md).
- Grant topics with [Authorization](./authorization.md).
- See every setting in the [settings reference](../reference/settings.md).
