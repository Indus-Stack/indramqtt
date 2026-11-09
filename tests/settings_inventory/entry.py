"""Real process, HTTP, file and MQTT checks for the settings inventory."""
from pathlib import Path
import base64
import contextlib
import datetime
import hashlib
import gzip
import http.server
import hmac
import json
import os
import queue
import secrets
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import threading
import time
import tomllib
import unittest
import uuid
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
BIN = Path(sys.argv.pop(1)).resolve()

def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def word(value):
    raw = value.encode() if isinstance(value, str) else value
    return struct.pack("!H", len(raw)) + raw


def frame(kind, body):
    remaining = len(body)
    head = bytearray([kind])
    while True:
        digit = remaining % 128
        remaining //= 128
        head.append(digit | (128 if remaining else 0))
        if not remaining:
            return bytes(head) + body


def exact(sock, length):
    raw = b""
    while len(raw) < length:
        chunk = sock.recv(length - len(raw))
        if not chunk:
            raise AssertionError("unexpected close")
        raw += chunk
    return raw


def packet(sock):
    head = exact(sock, 1)[0]
    remaining = 0
    scale = 1
    while True:
        digit = exact(sock, 1)[0]
        remaining += (digit & 127) * scale
        if not digit & 128:
            return head, exact(sock, remaining)
        scale *= 128
        if scale > 128 ** 3:
            raise AssertionError("invalid remaining length")


class Broker:
    def __init__(self, base):
        self.base = base
        self.cfg = base / "config"
        self.data = base / "data"
        self.cfg.mkdir()
        # Hold the three ports until start. A receiver, a peer or a
        # second broker can take a port between construction and start.
        self.reserved = []
        for _ in range(3):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            self.reserved.append(sock)
        self.api, self.mqtt, self.ws = [
            sock.getsockname()[1] for sock in self.reserved
        ]
        self.child = None
        self.sockets = []
        self.key = secrets.token_hex(24)
        self.password = secrets.token_hex(20)
        self.extra_env = {}

    def configure(self, anonymous=True, enabled=True, extra="", superusers=None):
        text = f'''[node]
id="az-01-entry"
data_dir="{self.data}"
[listeners.tcp]
bind="127.0.0.1:{self.mqtt}"
[listeners.ws]
bind="127.0.0.1:{self.ws}"
[listeners.tls]
enabled=false
[listeners.wss]
enabled=false
[listeners.api]
bind="127.0.0.1:{self.api}"
[logging]
level="error"
[auth]
enabled={str(enabled).lower()}
allow_anonymous={str(anonymous).lower()}
password_hash="bcrypt"
superusers={json.dumps(superusers or [])}
[quotas]
default_max_connections=1
default_max_publish_rate=1
burst_multiplier=1.0
'''
        (self.cfg / "indra.toml").write_text(text + extra)

    def start(self, wait_for_api=True):
        # Release the held ports just before the broker binds them.
        for sock in self.reserved:
            sock.close()
        self.reserved = []
        env = {key: value for key, value in os.environ.items() if not key.startswith("INDRA_")}
        env["INDRA_API_KEYS"] = self.key
        env.update(self.extra_env)
        self.child = subprocess.Popen(
            [str(BIN), "--config-dir", str(self.cfg)], cwd=ROOT, env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.child.poll() is not None:
                raw = self.child.communicate()[1].decode(errors="replace")
                for secret in [self.key, self.password, getattr(self, "admin_password", "")]:
                    if secret:
                        raw = raw.replace(secret, "[redacted]")
                raise AssertionError("broker exited at boot: " + raw[-3000:])
            try:
                if wait_for_api:
                    status, _ = self.http("GET", "/healthz", auth=False)
                    if status == 200:
                        return
                else:
                    probe = socket.create_connection(("127.0.0.1", self.mqtt), timeout=10)
                    probe.close()
                    return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(0.05)
        raise AssertionError("boot timeout")

    def stop(self):
        for sock in self.reserved:
            sock.close()
        self.reserved = []
        for sock in self.sockets:
            sock.close()
        self.sockets = []
        if self.child is not None:
            self.child.terminate()
            try:
                self.last_output = self.child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                self.child.kill()
                self.last_output = self.child.communicate(timeout=10)
            self.child = None

    def http(self, method, path, body=None, auth=True, authorization=None):
        headers = {"Content-Type": "application/json"}
        if auth:
            headers["Authorization"] = "Bearer " + (self.key if auth is True else auth)
        if authorization is not None:
            headers["Authorization"] = authorization
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.api}" + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=headers, method=method,
        )
        try:
            response = urllib.request.urlopen(request, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            raw = response.read()
            result = json.loads(raw) if raw.startswith((b"{", b"[")) else raw.decode()
            return response.status, result

    def connect(self, client, authenticated=False, keepalive=60, clean=True, username="audituser"):
        sock = socket.create_connection(("127.0.0.1", self.mqtt), timeout=10)
        self.sockets.append(sock)
        flags = (2 if clean else 0) | (192 if authenticated else 0)
        body = b"\x00\x04MQTT\x04" + bytes([flags]) + struct.pack("!H", keepalive) + word(client)
        if authenticated:
            body += word(username) + word(self.password)
        sock.sendall(frame(16, body))
        head, ack = packet(sock)
        if head != 32 or len(ack) != 2:
            raise AssertionError("invalid CONNACK")
        return sock, ack.hex()


    def connect_v5(self, client):
        sock = socket.create_connection(("127.0.0.1", self.mqtt), timeout=10)
        self.sockets.append(sock)
        sock.sendall(frame(16, b"\x00\x04MQTT\x05\x02\x00\x3c\x00" + word(client)))
        head, ack = packet(sock)
        if head != 32 or ack[:2] != b"\x00\x00":
            raise AssertionError("v5 connection refused")
        pos, length, scale = 2, 0, 1
        while True:
            digit = ack[pos]
            pos += 1
            length += (digit & 127) * scale
            if not digit & 128:
                break
            scale *= 128
            if scale > 128 ** 3:
                raise AssertionError("invalid CONNACK properties length")
        if pos + length != len(ack):
            raise AssertionError("invalid CONNACK properties")
        properties = {}
        integer_widths = {0x11: 4, 0x13: 2, 0x21: 2, 0x22: 2, 0x24: 1,
                          0x25: 1, 0x27: 4, 0x28: 1, 0x29: 1, 0x2a: 1}
        while pos < len(ack):
            key = ack[pos]
            pos += 1
            width = integer_widths.get(key)
            if width is None:
                raise AssertionError("unexpected CONNACK property " + hex(key))
            if pos + width > len(ack) or key in properties:
                raise AssertionError("invalid CONNACK property")
            properties[key] = int.from_bytes(ack[pos:pos + width], "big")
            pos += width
        return sock, properties

    def subscribe(self, sock, topic, qos=0):
        sock.sendall(frame(130, b"\x00\x01" + word(topic) + bytes([qos])))
        head, body = packet(sock)
        if head != 144 or body[:2] != b"\x00\x01":
            raise AssertionError("invalid SUBACK")
        return body[2:].hex()


def seed_admin(broker):
    broker.data.mkdir(exist_ok=True)
    broker.admin_password = secrets.token_hex(20)
    salt = secrets.token_bytes(16)
    salted = hashlib.pbkdf2_hmac("sha256", broker.admin_password.encode(), salt, 4096)
    stored = hashlib.sha256(hmac.digest(salted, b"Client Key", "sha256")).digest()
    server = hmac.digest(salted, b"Server Key", "sha256")
    encoded = "$scram-sha-256$4096$" + "$".join(
        base64.b64encode(value).decode() for value in [salt, stored, server]
    )
    (broker.data / "state.toml").write_text(
        "[[admin_users.users]]\nusername=\"inventoryadmin\"\n"
        + "password_hash=" + json.dumps(encoded) + "\nrole=\"administrator\"\n"
        + "must_change_password=false\n"
    )

def login(broker, username, password):
    status, body = broker.http("POST", "/api/v5/login",
                               {"username": username, "password": password}, auth=False)
    if status != 200 or not isinstance(body.get("token"), str):
        raise AssertionError("login failed; status=" + str(status))
    return body["token"]

def publish(broker, sock, topic, payload, retain=False):
    sock.sendall(frame(0x31 if retain else 0x30, word(topic) + payload))
    sock.sendall(b"\xc0\x00")
    head, body = packet(sock)
    if head != 0xd0 or body:
        raise AssertionError("publisher barrier failed")

class HttpReceiver:
    def __init__(self, response_body=b""):
        self.requests = queue.Queue()
        self.request_headers = queue.Queue()
        self.request_methods = queue.Queue()
        requests = self.requests
        request_headers = self.request_headers
        request_methods = self.request_methods

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length", "0"))
                requests.put((self.path, self.rfile.read(length)))
                request_headers.put({key.lower(): value for key, value in self.headers.items()})
                request_methods.put(self.command)
                self.send_response(200)
                self.send_header("Content-Length", str(len(response_body)))
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                if response_body:
                    self.wfile.write(response_body)

            do_PUT = do_POST

            def log_message(self, *_args):
                pass

        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = "http://127.0.0.1:" + str(self.server.server_port) + "/inventory"

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise AssertionError("test receiver did not stop")

class SilentPeer:
    def __init__(self):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.stopped = threading.Event()
        self.clients = []
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
                self.clients.append(client)
            except TimeoutError:
                pass

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=5)
        self.listener.close()
        for client in self.clients:
            client.close()
        if self.thread.is_alive():
            raise AssertionError("silent peer did not stop")


class PgAuthenticationPeer:
    """Observe real driver startup; refuse authentication, never execute SQL."""
    def __init__(self, tls_context=None):
        self.listener = socket.socket()
        self.listener.bind(("127.0.0.1", 0))
        self.listener.listen(8)
        self.listener.settimeout(0.1)
        self.port = self.listener.getsockname()[1]
        self.records = queue.Queue()
        self.tls_context = tls_context
        self.tls_sessions = 0
        self.errors = queue.Queue()
        self.clients = []
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.accept, daemon=True)
        self.thread.start()

    def accept(self):
        while not self.stopped.is_set():
            try:
                client, _ = self.listener.accept()
            except TimeoutError:
                continue
            except OSError:
                if self.stopped.is_set():
                    return
                self.errors.put("accept failed")
                return
            self.clients.append(client)
            try:
                with contextlib.ExitStack() as streams:
                    client = streams.enter_context(client)
                    client.settimeout(3)
                    first = client.recv(4)
                    if not first:
                        # Boot may probe TCP reachability without starting
                        # a database session. Partial frames still fail.
                        continue
                    size = int.from_bytes(first + exact(client, 4 - len(first)), "big")
                    if not 8 <= size <= 65536:
                        raise AssertionError("invalid startup length")
                    body = exact(client, size - 4)
                    probes = 0
                    while body in [struct.pack("!I", 80877103), struct.pack("!I", 80877104)]:
                        probes += 1
                        if probes > 2:
                            raise AssertionError("too many encryption probes")
                        if body == struct.pack("!I", 80877103) and self.tls_context is not None:
                            client.sendall(b"S")
                            client = streams.enter_context(self.tls_context.wrap_socket(client, server_side=True))
                            self.clients.append(client)
                            self.tls_sessions += 1
                        else:
                            client.sendall(b"N")
                        size = int.from_bytes(exact(client, 4), "big")
                        if not 8 <= size <= 65536:
                            raise AssertionError("invalid startup length")
                        body = exact(client, size - 4)
                    if body[:4] != struct.pack("!I", 196608):
                        raise AssertionError("unexpected startup protocol")
                    parts = body[4:].rstrip(b"\x00").split(b"\x00")
                    if len(parts) % 2:
                        raise AssertionError("invalid startup fields")
                    fields = {parts[index].decode(): parts[index + 1].decode()
                              for index in range(0, len(parts), 2)}
                    client.sendall(b"R" + struct.pack("!II", 8, 3))
                    tag = exact(client, 1)
                    size = int.from_bytes(exact(client, 4), "big")
                    if tag != b"p" or not 5 <= size <= 65536:
                        raise AssertionError("invalid password frame")
                    credential = exact(client, size - 4)
                    if not credential.endswith(b"\x00"):
                        raise AssertionError("invalid password terminator")
                    self.records.put({"user": fields.get("user"), "database": fields.get("database"),
                                      "application_name": fields.get("application_name"),
                                      "password_sha256": hashlib.sha256(credential[:-1]).hexdigest()})
                    refusal = b"SFATAL\x00C28P01\x00Minventory refusal\x00\x00"
                    client.sendall(b"E" + struct.pack("!I", len(refusal) + 4) + refusal)
            except (OSError, AssertionError, UnicodeError) as error:
                if not self.stopped.is_set():
                    self.errors.put(type(error).__name__)

    def close(self):
        self.stopped.set()
        self.listener.close()
        for client in self.clients:
            client.close()
        self.thread.join(timeout=5)
        if self.thread.is_alive():
            raise AssertionError("database test peer did not stop")


def opaque_security_fields(url):
    # The current API accepts opaque keys even outside their backend variant.
    # Inventory acceptance is not a statement that this is a valid contract body.
    return {"url": url, "method": "post", "headers": {"X-Inventory": "requested"},
            "request_timeout": "1ms", "pool_size": 1, "ssl": {"enable": True},
            "query": "inventory-query", "cmd": "inventory-command", "server": "127.0.0.1:1",
            "username": "inventory", "password": secrets.token_hex(20),
            "password_hash_algorithm": {"name": "bcrypt"}, "inventory_unknown": "requested"}


class InventoryEntry(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory(prefix="az01-entry-")
        self.broker = Broker(Path(self.scratch.name))
        self.addCleanup(self.scratch.cleanup)
        self.addCleanup(self.broker.stop)

    def assert_fields_kept(self, actual, expected):
        for key, value in expected.items():
            self.assertTrue(key in actual, key)
            self.assertTrue(actual[key] == value, key)

    def boot(self, **kwargs):
        self.broker.configure(**kwargs)
        self.broker.start()
        return self.broker

    def assert_sigv4_request(self, method, path, headers, body, access_key, secret_key, region, service):
        authorization = headers["authorization"]
        self.assertTrue(authorization.startswith("AWS4-HMAC-SHA256 "))
        parts = dict(item.split("=", 1) for item in authorization.split(" ", 1)[1].split(", "))
        self.assertEqual(set(parts), {"Credential", "SignedHeaders", "Signature"})
        credential = parts["Credential"].split("/")
        self.assertEqual(len(credential), 5)
        self.assertTrue(credential[0] == access_key, "selected access key differs")
        self.assertEqual(credential[2:], [region, service, "aws4_request"])
        self.assertEqual(credential[1], headers["x-amz-date"][:8])
        signed = parts["SignedHeaders"].split(";")
        canonical_headers = "".join(name + ":" + " ".join(headers[name].split()) + "\n" for name in signed)
        url = urllib.parse.urlsplit(path)
        query = urllib.parse.urlencode(sorted(urllib.parse.parse_qsl(url.query, keep_blank_values=True)),
                                       quote_via=urllib.parse.quote, safe="~")
        payload_hash = headers.get("x-amz-content-sha256", hashlib.sha256(body).hexdigest())
        canonical = "\n".join([method, url.path or "/", query, canonical_headers, parts["SignedHeaders"], payload_hash])
        scope = "/".join(credential[1:])
        to_sign = "\n".join(["AWS4-HMAC-SHA256", headers["x-amz-date"], scope,
                                hashlib.sha256(canonical.encode()).hexdigest()])
        key = ("AWS4" + secret_key).encode()
        for value in credential[1:]:
            key = hmac.new(key, value.encode(), hashlib.sha256).digest()
        expected = hmac.new(key, to_sign.encode(), hashlib.sha256).hexdigest()
        self.assertTrue(hmac.compare_digest(parts["Signature"], expected), "selected secret did not sign the request")

    def assert_forward_waits_past_configured_deadline(self, broker, connector, rule_id, payload, invalid):
        peer = SilentPeer()
        self.addCleanup(peer.close)
        config = {**connector, "endpoint": "http://127.0.0.1:" + str(peer.port), "timeout_ms": 1}
        endpoint = "/api/v5/connectors/" + config["name"]
        self.assertEqual(broker.http("PUT", endpoint, config)[0], 200)
        topic = "inventory/deadline/" + rule_id
        self.assertEqual(broker.http("POST", "/api/v5/rules", {
            "id": rule_id, "sql": 'SELECT * FROM "' + topic + '"',
            "actions": [config["type"] + ":" + config["name"]]})[0], 201)
        for attempt in range(2):
            if attempt:
                broker.stop()
                broker.start()
            status, stored = broker.http("GET", endpoint)
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(broker.http("PUT", endpoint, {**config, **invalid})[0], 400)
            self.assertTrue(broker.http("GET", endpoint) == (200, stored))
            before = len(peer.clients)
            pub, ack = broker.connect(rule_id + "-" + str(attempt))
            self.assertEqual(ack, "0000")
            pub.sendall(frame(0x30, word(topic) + payload))
            deadline, request, buffers = time.monotonic() + 5, b"", {}
            while time.monotonic() < deadline and not request:
                for connection in list(peer.clients)[before:]:
                    connection.settimeout(0.05)
                    try:
                        raw = connection.recv(8192)
                    except TimeoutError:
                        continue
                    buffers[connection] = buffers.get(connection, b"") + raw
                    head = buffers[connection]
                    self.assertLessEqual(len(head), 65536, "observer HTTP header bound exceeded")
                    if b"\r\n\r\n" in head and head.startswith((b"POST ", b"PUT ")):
                        request = head
                        break
                if not request:
                    time.sleep(0.02)
            self.assertTrue(request, "no HTTP request reached the silent loopback peer")
            time.sleep(0.5)
            status, answer = broker.http("GET", "/api/v5/rules/" + rule_id + "/metrics")
            self.assertEqual(status, 200)
            metrics = answer["metrics"]
            self.assertEqual(metrics["matched"], 1)
            for name in ("actions.total", "actions.failed", "actions.success"):
                self.assertEqual(metrics[name], 0)

    def create_forwarding_action_rule(self, broker, connector, rule_id, topic):
        # Equal names let the current connector-key dispatch reach the sink.
        # The fixture already uses MV-01's connector/action/rule entry shape.
        name = connector["name"]
        kind = connector["type"]
        self.assertEqual(broker.http("POST", "/api/v5/actions", {
            "type": kind, "name": name, "connector": name, "parameters": {}, "enable": True})[0], 201)
        status, rule = broker.http("POST", "/api/v5/rules", {
            "id": rule_id, "sql": 'SELECT * FROM "' + topic + '"', "actions": [kind + ":" + name]})
        self.assertEqual(status, 201)
        return rule["id"]

    def test_anonymous_flag_and_password_algorithm(self):
        b = self.boot(anonymous=False)
        # `auth.allow_anonymous = false` refuses an anonymous CONNECT even
        # while the user store is empty: the flag is applied before the
        # session is created (3.1.1 return code 5).
        _, ack = b.connect("anonymous-flag")
        self.assertEqual(ack, "0005", "anonymous must be refused with the flag off")
        # Version 5.0 refuses with reason code 0x87 Not authorized.
        sock = socket.create_connection(("127.0.0.1", b.mqtt), timeout=10)
        try:
            sock.sendall(frame(16, b"\x00\x04MQTT\x05\x02\x00\x3c\x00" + word("anonymous-v5")))
            head, ack = packet(sock)
            self.assertEqual(head, 32, "the kernel answers a refused v5 CONNECT")
            # CONNACK body: [session_present, reason_code, property_length, ...].
            self.assertEqual(ack[1], 0x87, "v5 anonymous CONNECT is refused with 0x87")
        finally:
            sock.close()
        b.stop()
        b.start()
        _, ack = b.connect("anonymous-after-restart")
        self.assertEqual(ack, "0005", "anonymous is still refused after a restart")
        # The file selects bcrypt: a user created through the API gets a
        # bcrypt verifier, and the credential binds through the socket.
        status, _ = b.http("POST", "/api/v1/auth/users",
                           {"username": "audituser", "password": b.password})
        self.assertEqual(status, 201)
        state = tomllib.loads((b.data / "state.toml").read_text())
        verifier = state["mqtt_users"]["users"][0]["password_hash"]
        self.assertTrue(verifier.startswith("$2b$"), "bcrypt file value applies to new verifiers")
        self.assertNotIn(b.password, verifier, "the stored verifier is not plaintext")
        status, listed = b.http("GET", "/api/v1/auth/users")
        self.assertEqual(status, 200)
        self.assertNotIn("password_hash", json.dumps(listed), "the user list carries no hash")
        _, ack = b.connect("bcrypt-ok", True)
        self.assertEqual(ack, "0000", "the bcrypt credential binds")
        raw = socket.create_connection(("127.0.0.1", b.mqtt), timeout=10)
        b.sockets.append(raw)
        flags = 2 | 192
        body = b"\x00\x04MQTT\x04" + bytes([flags]) + struct.pack("!H", 60) + word("bcrypt-bad")
        body += word("audituser") + word("wrong-password-value")
        raw.sendall(frame(16, body))
        _, bad_ack = packet(raw)
        self.assertEqual(bad_ack.hex(), "0004", "a wrong password is refused")
        _, ack = b.connect("anonymous-with-users")
        self.assertEqual(ack, "0005")
        # Version 5.0 still refuses anonymous while users exist.
        sock = socket.create_connection(("127.0.0.1", b.mqtt), timeout=10)
        try:
            sock.sendall(frame(16, b"\x00\x04MQTT\x05\x02\x00\x3c\x00" + word("anonymous-v5-users")))
            head, ack = packet(sock)
            self.assertEqual(head, 32, "the kernel answers a refused v5 CONNECT")
            self.assertEqual(ack[1], 0x87, "v5 anonymous CONNECT is refused with 0x87")
        finally:
            sock.close()
        # The flag toggles: an empty store with the flag on accepts.
        probe_base = Path(self.scratch.name) / "allow-on"
        probe_base.mkdir()
        probe = Broker(probe_base)
        probe.configure(anonymous=True)
        probe.start()
        try:
            _, ack = probe.connect("anonymous-allowed")
            self.assertEqual(ack, "0000", "anonymous binds with the flag on and no users")
        finally:
            probe.stop()
        # The file selects sha256: a new user gets a 64-char hex verifier.
        # The old bcrypt verifier still binds after the switch and restart.
        b.stop()
        text = (b.cfg / "indra.toml").read_text()
        text = text.replace('password_hash="bcrypt"', 'password_hash="sha256"')
        (b.cfg / "indra.toml").write_text(text)
        b.start()
        _, ack = b.connect("bcrypt-after-switch", True)
        self.assertEqual(ack, "0000", "an old verifier keeps working after the switch")
        status, _ = b.http("POST", "/api/v1/auth/users",
                           {"username": "sha-user", "password": b.password})
        self.assertEqual(status, 201)
        state = tomllib.loads((b.data / "state.toml").read_text())
        rows = {row["username"]: row["password_hash"] for row in state["mqtt_users"]["users"]}
        sha_verifier = rows["sha-user"]
        self.assertEqual(len(sha_verifier), 64, "sha256 selection stores 64 hex chars")
        self.assertNotIn(b.password, sha_verifier, "the stored verifier is not plaintext")
        _, ack = b.connect("sha-ok", True, username="sha-user")
        self.assertEqual(ack, "0000", "the sha256 credential binds")
        b.stop()
        b.start()
        _, ack = b.connect("sha-after-restart", True, username="sha-user")
        self.assertEqual(ack, "0000", "the sha256 credential binds after a restart")
        _, ack = b.connect("bcrypt-after-restart", True)
        self.assertEqual(ack, "0000", "the bcrypt credential binds after a restart")
        # An unreadable verifier never grants access (fail closed).
        b.stop()
        raw_state = (b.data / "state.toml").read_text()
        self.assertIn(sha_verifier, raw_state, "the sha verifier is stored")
        raw_state = raw_state.replace(sha_verifier, "$bogus$unreadable-verifier")
        (b.data / "state.toml").write_text(raw_state)
        b.start()
        raw = socket.create_connection(("127.0.0.1", b.mqtt), timeout=10)
        b.sockets.append(raw)
        body = b"\x00\x04MQTT\x04" + bytes([flags]) + struct.pack("!H", 60) + word("locked-client")
        body += word("sha-user") + word(b.password)
        raw.sendall(frame(16, body))
        _, locked_ack = packet(raw)
        self.assertEqual(locked_ack.hex(), "0004", "an unreadable verifier is refused")
        _, ack = b.connect("bcrypt-still-ok", True)
        self.assertEqual(ack, "0000", "a valid credential still binds")
        # Unsupported algorithm fails the start, fail closed.
        b.stop()
        bad = (b.cfg / "indra.toml").read_text().replace(
            'password_hash="sha256"', 'password_hash="argon2id"')
        (b.cfg / "indra.toml").write_text(bad)
        with self.assertRaises(AssertionError) as ctx:
            b.start()
        self.assertIn("must be one of", str(ctx.exception))
        self.assertIn("auth.password_hash", str(ctx.exception))
        b.child = None

    # // AZ-01 inventory: current behaviour; owner MV-09a
    def test_disabled_auth_and_superuser_are_not_applied(self):
        b = self.boot(enabled=False, superusers=["audituser"])
        status, _ = b.http("POST", "/api/v1/auth/users",
                           {"username": "audituser", "password": b.password})
        self.assertEqual(status, 201)
        _, ack = b.connect("disabled-auth-anonymous")
        self.assertEqual(ack, "0005")
        status, _ = b.http("POST", "/api/v1/auth/acls",
                           {"client_pattern": "other", "action": "all",
                            "topic_pattern": "other/#", "allow": True})
        self.assertEqual(status, 201)
        sock, ack = b.connect("superuser-client", True)
        self.assertEqual(ack, "0000")
        self.assertEqual(b.subscribe(sock, "inventory/topic"), "80")

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_file_default_quotas_are_not_inherited(self):
        b = self.boot()
        status, created = b.http("POST", "/api/v1/auth/users",
                                 {"username": "audituser", "password": b.password})
        self.assertEqual(status, 201)
        self.assertTrue(all(created["quotas"][key] is None for key in
                            ["max_connections", "max_publish_rate", "max_publish_burst"]))
        _, first = b.connect("first", True)
        _, second = b.connect("second", True)
        self.assertEqual([first, second], ["0000", "0000"])
        status, _ = b.http("POST", "/api/v1/auth/users",
                           {"username": "audituser", "password": b.password,
                            "quotas": {"max_connections": 1}})
        self.assertEqual(status, 201)
        _, third = b.connect("third", True)
        self.assertEqual(third, "0000")
        _, fourth = b.connect("fourth", True)
        self.assertEqual(fourth, "0003")

    def test_failed_authorization_save_keeps_live_policy(self):
        b = self.boot()
        status, _ = b.http("POST", "/api/v1/auth/acls",
                           {"client_pattern": "other", "action": "all",
                            "topic_pattern": "other/#", "allow": True})
        self.assertEqual(status, 201)
        sock, ack = b.connect("policy-client")
        self.assertEqual(ack, "0000")
        self.assertEqual(b.subscribe(sock, "inventory/probe"), "80")
        status, _ = b.http("PUT", "/api/v5/authorization/settings",
                           {"no_match": "deny", "deny_action": "ignore"})
        self.assertEqual(status, 200)
        status, _ = b.http("PUT", "/api/v5/authorization/settings", {"no_match": "invalid"})
        self.assertEqual(status, 400)
        self.assertEqual(b.subscribe(sock, "inventory/refused"), "80")
        saved = b.base / "saved-data"
        b.data.rename(saved)
        try:
            # A failed persist keeps the old live decision: nothing is
            # applied, the old state stays enforced (D2).
            status, _ = b.http("PUT", "/api/v5/authorization/settings",
                               {"no_match": "allow", "deny_action": "ignore"})
            self.assertEqual(status, 500)
            status, current = b.http("GET", "/api/v5/authorization/settings")
            self.assertEqual(status, 200)
            self.assertEqual(current["no_match"], "deny")
            self.assertEqual(b.subscribe(sock, "inventory/failed"), "80")
        finally:
            self.assertFalse(b.data.exists())
            saved.rename(b.data)
        # A later successful save must not commit the refused value.
        status, _ = b.http("POST", "/api/v1/auth/users",
                           {"username": "after-failed-save", "password": b.password})
        self.assertEqual(status, 201)
        state = tomllib.loads((b.data / "state.toml").read_text())
        self.assertEqual(state["authz_settings"]["no_match"], "deny")
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/authorization/settings")
        self.assertEqual(status, 200)
        self.assertEqual(current["no_match"], "deny")
        # Use an authenticated CONNECT. Anonymous fails when users exist.
        sock, ack = b.connect("policy-after-restart", True, username="after-failed-save")
        self.assertEqual(ack, "0000")
        self.assertEqual(b.subscribe(sock, "inventory/failed"), "80")

    # // AZ-01 inventory: current behaviour; owner MV-10
    def test_api_only_metrics_fields_are_refused_in_file(self):
        b = self.broker
        b.configure()
        original = (b.cfg / "indra.toml").read_text()
        for key, text in [
            ("push_gateway.headers", "[prometheus.push_gateway.headers]\nX-Inventory=\"value\"\n"),
            ("namespaced_metrics_limiter", "[prometheus.namespaced_metrics_limiter]\nrate=\"1/30s\"\n"),
        ]:
            with self.subTest(field=key):
                (b.cfg / "indra.toml").write_text(original + text)
                env = {k: v for k, v in os.environ.items() if not k.startswith("INDRA_")}
                probe = subprocess.run([str(BIN), "--config-dir", str(b.cfg), "--check-config"],
                                       cwd=ROOT, env=env, capture_output=True, text=True, timeout=20)
                self.assertEqual(probe.returncode, 2)
                self.assertIn("unknown setting", probe.stderr)
                self.assertIn("prometheus." + key, probe.stderr)

    # MV-05: dashboard user scopes are stored and they limit the token.
    def test_dashboard_scopes_are_stored_and_limit_token(self):
        b = self.broker
        b.configure()
        seed_admin(b)
        b.start()
        admin = login(b, "inventoryadmin", b.admin_password)
        user_password = secrets.token_hex(20)
        status, created = b.http("POST", "/api/v5/users",
                                {"username": "limiteduser", "password": user_password,
                                 "role": "viewer", "scopes": []}, auth=admin)
        self.assertEqual(status, 200)
        self.assertEqual(created["scopes"], [])
        token = login(b, "limiteduser", user_password)
        status, _ = b.http("GET", "/api/v5/metrics", auth=token)
        self.assertEqual(status, 403)
        status, _ = b.http("PUT", "/api/v5/users/limiteduser",
                           {"scopes": ["monitoring"]}, auth=admin)
        self.assertEqual(status, 200)
        status, users = b.http("GET", "/api/v5/users", auth=admin)
        self.assertEqual(status, 200)
        row = next(user for user in users if user["username"] == "limiteduser")
        self.assertEqual(row["scopes"], ["monitoring"])
        b.stop()
        b.start()
        token = login(b, "limiteduser", user_password)
        status, _ = b.http("GET", "/api/v5/metrics", auth=token)
        self.assertEqual(status, 200)

    def test_unsupported_authorization_source_is_refused_and_denies(self):
        # A source kind this build does not evaluate is refused at create:
        # 400 naming the kind, nothing stored, the old state stays active.
        b = self.boot()
        status, _ = b.http("POST", "/api/v5/authorization/sources",
                           {"type": "http", "enable": True,
                            "config": "{\"url\":\"http://127.0.0.1:1/inventory\"}"})
        self.assertEqual(status, 400, "an unsupported source kind is refused")
        status, sources = b.http("GET", "/api/v5/authorization/sources")
        self.assertEqual(status, 200)
        self.assertEqual([s["type"] for s in sources["sources"]], ["built_in_database"])
        # An update of the same kind is refused the same way.
        status, _ = b.http("PUT", "/api/v5/authorization/sources/built_in_database",
                           {"enable": True})
        self.assertEqual(status, 204, "the built-in source still updates")
        status, _ = b.http("PUT", "/api/v5/authorization/sources/http",
                           {"enable": True})
        self.assertEqual(status, 400, "an unsupported source update is refused")

        # Old stored state: an unsupported source in the stored order
        # denies every decision that would reach it; no fall-through to a
        # later allow. The record stays, an alarm is raised, and the
        # operator can still delete it through the API while MQTT access
        # stays denied (D2).
        receiver = HttpReceiver(b'{"result":"deny"}')
        self.addCleanup(receiver.close)
        source_config = opaque_security_fields(receiver.url)
        config = (b.cfg / "indra.toml").read_text()
        config += (
            "[[authz_sources.sources]]\n"
            "type = \"http\"\n"
            "enable = true\n"
            "config = " + json.dumps(
                json.dumps(source_config)) + "\n"
            "[[authz_sources.sources]]\n"
            "type = \"built_in_database\"\n"
            "enable = true\n"
        )
        (b.cfg / "indra.toml").write_text(config)
        b.stop()
        b.start()
        # The empty built-in rule table must not bypass the unsupported
        # source: the decision reaches it and is denied (D2).
        empty_sock, _ = b.connect("unsupported-empty-rules")
        self.assertEqual(b.subscribe(empty_sock, "inventory/empty"), "80")
        status, _ = b.http("POST", "/api/v1/auth/acls",
                           {"client_pattern": "other", "action": "all",
                            "topic_pattern": "other/#", "allow": True})
        self.assertEqual(status, 201)
        status, _ = b.http("PUT", "/api/v5/authorization/settings",
                           {"no_match": "allow", "deny_action": "ignore"})
        self.assertEqual(status, 200)
        status, sources = b.http("GET", "/api/v5/authorization/sources")
        self.assertEqual(status, 200)
        self.assertEqual(sources["sources"][0]["type"], "http")
        self.assert_fields_kept(sources["sources"][0], source_config)
        status, alarms = b.http("GET", "/api/v5/alarms")
        self.assertEqual(status, 200)
        self.assertIn("unsupported_authorization_source",
                      [alarm["name"] for alarm in alarms["data"]])
        sock, _ = b.connect("unsupported-chain")
        # The decision reaches the unsupported source first: deny, no call.
        self.assertEqual(b.subscribe(sock, "inventory/source"), "80")
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=0.2)
        before = (b.data / "state.toml").read_bytes()
        b.stop()
        b.start()
        self.assertTrue((b.data / "state.toml").read_bytes() == before, "source state changed at restart")
        sock, _ = b.connect("unsupported-chain-restart")
        self.assertEqual(b.subscribe(sock, "inventory/source"), "80")
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=0.2)
        # Chain cases: with the built-in source first, its allow rule
        # decides (an earlier supported source stands) and a no-match
        # decision that reaches the later unsupported source is denied.
        config = (b.cfg / "indra.toml").read_text()
        config = config.replace(
            "[[authz_sources.sources]]\n"
            "type = \"http\"\n"
            "enable = true\n"
            "config = " + json.dumps(json.dumps(source_config)) + "\n"
            "[[authz_sources.sources]]\n"
            "type = \"built_in_database\"\n"
            "enable = true\n",
            "[[authz_sources.sources]]\n"
            "type = \"built_in_database\"\n"
            "enable = true\n"
            "[[authz_sources.sources]]\n"
            "type = \"http\"\n"
            "enable = true\n"
            "config = " + json.dumps(json.dumps(source_config)) + "\n",
        )
        (b.cfg / "indra.toml").write_text(config)
        b.stop()
        b.start()
        sock, _ = b.connect("other")
        self.assertEqual(b.subscribe(sock, "other/#"), "00")
        self.assertEqual(b.subscribe(sock, "inventory/source2"), "80")
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=0.2)
        # Operator recovery: delete the unsupported source while access
        # stays denied; afterwards the built-in allow rule decides again.
        status, _ = b.http("DELETE", "/api/v5/authorization/sources/http")
        self.assertEqual(status, 204)
        self.assertEqual(b.subscribe(sock, "inventory/recovered"), "00")
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=0.2)
        status, alarms = b.http("GET", "/api/v5/alarms")
        self.assertEqual(status, 200)
        self.assertNotIn("unsupported_authorization_source",
                         [alarm["name"] for alarm in alarms["data"]])
        # Unknown kinds fail with NOT_FOUND, not the refusal.
        status, _ = b.http("PUT", "/api/v5/authorization/sources/no-such-kind",
                            {"enable": True})
        self.assertEqual(status, 404)
        status, _ = b.http("POST", "/api/v5/authorization/sources",
                            {"type": "no-such-kind", "enable": True})
        self.assertEqual(status, 404)
        # Publish follows the same chain: the allow rule matches.
        psub, _ = b.connect("chain-pub-sub")
        self.assertEqual(b.subscribe(psub, "other/#"), "00")
        ppub, _ = b.connect("chain-pub")
        publish(b, ppub, "other/hello", b"hi")
        self.assertEqual(packet(psub), (0x30, word("other/hello") + b"hi"))
        dsub, _ = b.connect("chain-deny-sub")
        self.assertEqual(b.subscribe(dsub, "inventory/denied"), "00")
        # Two tenants stay apart on the chain path.
        b.stop()
        tcfg = (b.cfg / "indra.toml").read_text()
        tcfg += ('[[tenants.rules]]\nlistener="*"\nattribute="tenant"\n'
                 'expression="${username}"\n')
        (b.cfg / "indra.toml").write_text(tcfg)
        b.start()
        self.assertEqual(b.http("POST", "/api/v5/mt/ns/alice")[0], 204)
        self.assertEqual(b.http("POST", "/api/v5/mt/ns/bob")[0], 204)
        self.assertEqual(b.http("POST", "/api/v1/auth/users?tenant=alice",
                                {"username": "alice", "password": b.password})[0], 201)
        self.assertEqual(b.http("POST", "/api/v1/auth/users?tenant=bob",
                                {"username": "bob", "password": b.password})[0], 201)
        self.assertEqual(b.http("POST", "/api/v1/auth/acls?tenant=alice",
                                {"client_pattern": "*", "action": "all",
                                 "topic_pattern": "inventory/shared", "allow": True})[0], 201)
        asock, ack = b.connect("shared-tenant-client", True, username="alice")
        self.assertEqual(ack, "0000")
        bsock, ack = b.connect("shared-tenant-client", True, username="bob")
        self.assertEqual(ack, "0000")
        self.assertEqual(b.subscribe(asock, "inventory/shared"), "00")
        self.assertEqual(b.subscribe(bsock, "inventory/shared"), "80")
        apub, ack = b.connect("shared-tenant-pub", True, username="alice")
        self.assertEqual(ack, "0000")
        publish(b, apub, "inventory/shared", b"apart")
        self.assertEqual(packet(asock), (0x30, word("inventory/shared") + b"apart"))
        bsock.settimeout(0.3)
        with self.assertRaises(TimeoutError):
            packet(bsock)
        bsock.settimeout(10)

    # // AZ-01 inventory: current behaviour; owner MV-09a
    def test_authentication_cache_ttl_and_memory_are_stored_only(self):
        b = self.boot()
        options = {"node_cache": {"enable": True, "max_count": 13,
                   "cache_ttl": "1ms", "cleanup_interval": "1ms",
                   "stat_update_interval": "1ms", "max_memory": "1B"},
                   "builtin_record_count_refresh_interval": "1ms",
                   "ignore_backend_failures": True}
        status, _ = b.http("PUT", "/api/v5/authentication/settings", options)
        self.assertEqual(status, 204)
        status, _ = b.http("POST", "/api/v1/auth/users",
                           {"username": "audituser", "password": b.password})
        self.assertEqual(status, 201)
        _, ack = b.connect("cache-record", True)
        self.assertEqual(ack, "0000")
        time.sleep(0.1)
        status, stats = b.http("GET", "/api/v5/authentication/node_cache/status")
        self.assertEqual(status, 200)
        self.assertEqual(stats["count"], 1)
        self.assertEqual(stats["max_count"], 13)
        status, current = b.http("GET", "/api/v5/authentication/settings")
        self.assertEqual(status, 200)
        self.assertEqual(current["node_cache"], options["node_cache"])
        status, _ = b.http("PUT", "/api/v5/authentication/settings",
                           {"node_cache": {"cache_ttl": "invalid"}})
        self.assertEqual(status, 400)
        status, kept = b.http("GET", "/api/v5/authentication/settings")
        self.assertEqual(status, 200)
        self.assertEqual(kept, current)
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/authentication/settings")
        self.assertEqual(status, 200)
        self.assertEqual(current["node_cache"], options["node_cache"])

    def test_authorization_cache_policy_applies_and_invalidates(self):
        b = self.boot()
        status, _ = b.http(
            "POST", "/api/v5/authorization/sources/built_in_database/rules/all",
            {"rules": [{"topic": "inventory/cache/#", "permission": "allow", "action": "all"},
                        {"topic": "inventory/pub/#", "permission": "allow", "action": "all"},
                        {"topic": "inventory/will/#", "permission": "allow", "action": "all"},
                        {"topic": "inventory/delayed-inner", "permission": "allow", "action": "all"},
                        {"topic": "inventory/excluded/#", "permission": "allow", "action": "all"}]})
        self.assertEqual(status, 204)
        settings = {"no_match": "deny", "deny_action": "ignore",
                    "cache": {"enable": True, "max_size": 32, "ttl": "1m", "excludes": []}}
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        status, current = b.http("GET", "/api/v5/authorization/settings")
        self.assertEqual(status, 200)
        self.assertEqual(current["no_match"], "deny")
        self.assertEqual(current["deny_action"], "ignore")
        self.assertEqual(current["cache"]["enable"], True)
        self.assertEqual(current["cache"]["ttl"], "1m")
        self.assertEqual(current["cache"]["excludes"], [])
        sock, _ = b.connect("cache-policy")
        self.assertEqual(b.subscribe(sock, "inventory/cache/one"), "00")
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertTrue(any(entry["topic"] == "inventory/cache/one"
                            and entry["result"] == "allow" for entry in cached),
                        "the allowed decision is cached on the client")
        self.assertEqual(b.subscribe(sock, "inventory/other"), "80")
        # Publish QoS 0 through the socket reaches the subscriber.
        sub, _ = b.connect("cache-sub")
        self.assertEqual(b.subscribe(sub, "inventory/pub/zero"), "00")
        pub, _ = b.connect("cache-pub")
        publish(b, pub, "inventory/pub/zero", b"zero")
        self.assertEqual(packet(sub), (0x30, word("inventory/pub/zero") + b"zero"))
        status, cached = b.http("GET", "/api/v5/clients/cache-pub/authorization/cache")
        self.assertEqual(status, 200)
        self.assertTrue(any(entry["topic"] == "inventory/pub/zero"
                            and entry["result"] == "allow" for entry in cached),
                        "the publish decision is cached")
        # Publish QoS 1 through the socket acks and delivers.
        self.assertEqual(b.subscribe(sub, "inventory/pub/one", qos=1), "01")
        pub.sendall(frame(0x32, word("inventory/pub/one") + b"\x00\x0a" + b"one"))
        self.assertEqual(packet(pub), (0x40, b"\x00\x0a"))
        head, body = packet(sub)
        self.assertEqual(head, 0x32)
        sub.sendall(frame(0x40, body[len(word("inventory/pub/one")):len(word("inventory/pub/one")) + 2]))
        # Publish QoS 2 through the socket completes the handshake.
        self.assertEqual(b.subscribe(sub, "inventory/pub/two"), "00")
        pub.sendall(frame(0x34, word("inventory/pub/two") + b"\x00\x0b" + b"two"))
        self.assertEqual(packet(pub), (0x50, b"\x00\x0b"))
        pub.sendall(frame(0x62, b"\x00\x0b"))
        self.assertEqual(packet(pub), (0x70, b"\x00\x0b"))
        self.assertEqual(packet(sub), (0x30, word("inventory/pub/two") + b"two"))
        # Version 5 publish through the socket delivers.
        v5pub = socket.create_connection(("127.0.0.1", b.mqtt), timeout=10)
        b.sockets.append(v5pub)
        v5pub.sendall(frame(16, b"\x00\x04MQTT\x05\x02\x00\x3c\x00" + word("cache-v5-pub")))
        head, ack = packet(v5pub)
        self.assertEqual(head, 32)
        v5sub, props = b.connect_v5("cache-v5-sub")
        v5sub.sendall(frame(0x82, b"\x00\x02\x00" + word("inventory/pub/v5") + b"\x00"))
        head, body = packet(v5sub)
        self.assertEqual(head, 0x90)
        self.assertEqual(body[-1], 0x00)
        v5pub.sendall(frame(0x30, word("inventory/pub/v5") + b"\x00" + b"v5hello"))
        head, body = packet(v5sub)
        self.assertIn(head, (0x30, 0x31, 0x32, 0x33))
        # Version 3.1.1 and version 5 connects both work here.
        _, ack = b.connect("cache-v4-again")
        self.assertEqual(ack, "0000")
        # Delayed publish authorizes the inner topic and delivers.
        status, delayed_cfg = b.http("GET", "/api/v5/mqtt/delayed")
        self.assertEqual(status, 200)
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/delayed",
                                {"enable": True, "max_delayed_messages": 100})[0], 200)
        dsub, _ = b.connect("cache-delayed-sub")
        self.assertEqual(b.subscribe(dsub, "inventory/delayed-inner"), "00")
        publish(b, pub, "$delayed/1/inventory/delayed-inner", b"later")
        head, body = packet(dsub)
        self.assertEqual(head, 0x30)
        self.assertEqual(body, word("inventory/delayed-inner") + b"later")
        # Will authorizes the will topic and delivers on drop.
        wsub, _ = b.connect("cache-will-sub")
        self.assertEqual(b.subscribe(wsub, "inventory/will/one"), "00")
        wpayload = b"gone"
        wsock = socket.create_connection(("127.0.0.1", b.mqtt), timeout=10)
        b.sockets.append(wsock)
        wflags = 0x02 | 0x04
        wbody = (b"\x00\x04MQTT\x04" + bytes([wflags]) + struct.pack("!H", 60)
                 + word("cache-will-pub") + word("inventory/will/one") + word(wpayload))
        wsock.sendall(frame(16, wbody))
        self.assertEqual(packet(wsock), (32, b"\x00\x00"))
        wsock.shutdown(socket.SHUT_RDWR)
        wsock.close()
        head, body = packet(wsub)
        self.assertEqual(head, 0x30)
        self.assertEqual(body, word("inventory/will/one") + wpayload)
        # TTL bounds staleness: a short TTL expires the entry.
        settings["cache"]["ttl"] = "1s"
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        self.assertEqual(b.subscribe(sock, "inventory/cache/ttl-one"), "00")
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertTrue(any(entry["topic"] == "inventory/cache/ttl-one" for entry in cached))
        time.sleep(1.3)
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertFalse(any(entry["topic"] == "inventory/cache/ttl-one" for entry in cached),
                         "the TTL expired the entry")
        self.assertEqual(b.subscribe(sock, "inventory/cache/ttl-one"), "00")
        settings["cache"]["ttl"] = "1m"
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        # Max size bounds the per-client cache: oldest goes first.
        settings["cache"]["max_size"] = 1
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        self.assertEqual(b.subscribe(sock, "inventory/cache/a"), "00")
        self.assertEqual(b.subscribe(sock, "inventory/cache/b"), "00")
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(len(cached), 1)
        self.assertEqual(cached[0]["topic"], "inventory/cache/b")
        settings["cache"]["max_size"] = 32
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        # Excludes skip caching for listed topics.
        settings["cache"]["excludes"] = ["inventory/excluded/#"]
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        self.assertEqual(b.subscribe(sock, "inventory/excluded/one"), "00")
        self.assertEqual(b.subscribe(sock, "inventory/cache/kept"), "00")
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertFalse(any(entry["topic"] == "inventory/excluded/one" for entry in cached))
        self.assertTrue(any(entry["topic"] == "inventory/cache/kept" for entry in cached))
        settings["cache"]["excludes"] = []
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        # A rule change invalidates the cached decision: the next
        # operation uses the new policy, no stale allow.
        status, _ = b.http(
            "POST", "/api/v5/authorization/sources/built_in_database/rules/all",
            {"rules": [{"topic": "inventory/other", "permission": "allow",
                        "action": "subscribe"}]})
        self.assertEqual(status, 204)
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(cached, [], "the rule change invalidated the cache")
        self.assertEqual(b.subscribe(sock, "inventory/cache/one"), "80")
        self.assertEqual(b.subscribe(sock, "inventory/other"), "00")
        # A legacy ACL write invalidates the cache too: a cached allow
        # ends after the legacy rule is deleted (AZ-02).
        status, _ = b.http(
            "POST", "/api/v1/auth/acls",
            {"client_pattern": "*", "action": "subscribe",
             "topic_pattern": "inventory/legacy/#", "allow": True})
        self.assertEqual(status, 201)
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(cached, [], "the legacy ACL create invalidated the cache")
        self.assertEqual(b.subscribe(sock, "inventory/legacy/one"), "00")
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertTrue(any(entry["topic"] == "inventory/legacy/one"
                            and entry["result"] == "allow" for entry in cached),
                        "the legacy allow is cached")
        status, acls = b.http("GET", "/api/v1/auth/acls")
        self.assertEqual(status, 200)
        legacy = next(index for index, acl in enumerate(acls)
                      if acl["topic_pattern"] == "inventory/legacy/#")
        status, _ = b.http("DELETE", f"/api/v1/auth/acls/{legacy}")
        self.assertEqual(status, 204)
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(cached, [], "the legacy ACL delete invalidated the cache")
        self.assertEqual(b.subscribe(sock, "inventory/legacy/one"), "80")
        # A source change invalidates: same enable still clears.
        status, _ = b.http("PUT", "/api/v5/authorization/sources/built_in_database",
                            {"enable": True})
        self.assertEqual(status, 204)
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(cached, [], "the source change invalidated the cache")
        # A user change invalidates: a new user clears entries.
        status, _ = b.http("POST", "/api/v1/auth/users",
                            {"username": "cacheuser", "password": b.password})
        self.assertEqual(status, 201)
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(cached, [], "the user change invalidated the cache")
        # A client rule change invalidates: a new rule clears entries.
        self.assertEqual(b.subscribe(sock, "inventory/other"), "00")
        status, _ = b.http("POST", "/api/v5/authorization/sources/built_in_database/rules/clients",
                            {"clientid": "cache-policy", "topic": "inventory/other",
                             "permission": "allow", "action": "subscribe"})
        self.assertEqual(status, 204)
        status, cached = b.http("GET", "/api/v5/clients/cache-policy/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(cached, [], "the client change invalidated the cache")
        # Existing session uses the new policy on its next operation.
        status, _ = b.http(
            "POST", "/api/v5/authorization/sources/built_in_database/rules/all",
            {"rules": [{"topic": "inventory/other", "permission": "deny",
                        "action": "subscribe"},
                       {"topic": "inventory/pub/zero", "permission": "allow",
                        "action": "publish"}]})
        self.assertEqual(status, 204)
        self.assertEqual(b.subscribe(sock, "inventory/other"), "80")
        publish(b, pub, "inventory/pub/zero", b"after-change")
        self.assertEqual(packet(sub), (0x30, word("inventory/pub/zero") + b"after-change"))
        # Cache disabled stays a stored and reported policy; decisions are
        # still enforced.
        settings["cache"]["enable"] = False
        status, _ = b.http("PUT", "/api/v5/authorization/settings", settings)
        self.assertEqual(status, 200)
        status, current = b.http("GET", "/api/v5/authorization/settings")
        self.assertEqual(status, 200)
        self.assertEqual(current["cache"]["enable"], False)
        self.assertEqual(b.subscribe(sock, "inventory/other"), "80")
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/authorization/settings")
        self.assertEqual(status, 200)
        self.assertEqual(current["no_match"], "deny")
        self.assertEqual(current["cache"]["enable"], False)

    def test_authorization_settings_change_clears_cache_and_denies_at_once(self):
        b = self.boot()
        status, _ = b.http(
            "POST", "/api/v5/authorization/sources/built_in_database/rules/all",
            {"rules": [{"topic": "sa22/allow/#", "permission": "allow",
                        "action": "all"}]})
        self.assertEqual(status, 204)
        status, _ = b.http("PUT", "/api/v5/authorization/settings",
                           {"no_match": "allow", "deny_action": "ignore"})
        self.assertEqual(status, 200)
        sub, _ = b.connect("sa22-sub")
        self.assertEqual(b.subscribe(sub, "sa22/other/one"), "00")
        status, cached = b.http("GET", "/api/v5/clients/sa22-sub/authorization/cache")
        self.assertEqual(status, 200)
        self.assertTrue(any(entry["topic"] == "sa22/other/one" for entry in cached))
        status, _ = b.http("PUT", "/api/v5/authorization/settings",
                           {"no_match": "deny", "deny_action": "ignore"})
        self.assertEqual(status, 200)
        status, cached = b.http("GET", "/api/v5/clients/sa22-sub/authorization/cache")
        self.assertEqual(status, 200)
        self.assertEqual(cached, [])
        self.assertEqual(b.subscribe(sub, "sa22/other/two"), "80")

    def test_authorization_denied_publish_will_v5_and_disconnect(self):
        b = self.boot()
        status, _ = b.http(
            "POST", "/api/v5/authorization/sources/built_in_database/rules/all",
            {"rules": [{"topic": "sa22/allow/#", "permission": "allow",
                        "action": "all"}]})
        self.assertEqual(status, 204)
        status, _ = b.http("PUT", "/api/v5/authorization/settings",
                           {"no_match": "deny", "deny_action": "ignore"})
        self.assertEqual(status, 200)
        sub, _ = b.connect("sa22-deny-sub")
        self.assertEqual(b.subscribe(sub, "sa22/allow/one"), "00")
        self.assertEqual(b.subscribe(sub, "sa22/deny/one"), "80")
        pub, _ = b.connect("sa22-deny-pub")
        sub.settimeout(0.5)
        pub.sendall(frame(0x30, word("sa22/deny/one") + b"blocked"))
        with self.assertRaises(TimeoutError):
            packet(sub)
        sub.settimeout(10)
        pub.sendall(frame(0x32, word("sa22/deny/one") + b"\x00\x09" + b"blocked"))
        head, body = packet(pub)
        self.assertEqual(head, 0x40)
        sub.settimeout(0.5)
        with self.assertRaises(TimeoutError):
            packet(sub)
        sub.settimeout(10)
        wsub, _ = b.connect("sa22-will-sub")
        self.assertEqual(b.subscribe(wsub, "sa22/allow/will"), "00")
        wsock = socket.create_connection(("127.0.0.1", b.mqtt), timeout=10)
        b.sockets.append(wsock)
        wflags = 0x02 | 0x04
        wbody = (b"\x00\x04MQTT\x04" + bytes([wflags]) + struct.pack("!H", 60)
                 + word("sa22-will-pub") + word("sa22/deny/will") + word(b"gone"))
        wsock.sendall(frame(16, wbody))
        self.assertEqual(packet(wsock), (32, b"\x00\x00"))
        wsock.shutdown(socket.SHUT_RDWR)
        wsock.close()
        wsub.settimeout(0.5)
        with self.assertRaises(TimeoutError):
            packet(wsub)
        wsub.settimeout(10)
        v5sub, _ = b.connect_v5("sa22-v5-sub")
        v5sub.sendall(frame(0x82, b"\x00\x03\x00" + word("sa22/deny/v5") + b"\x00"))
        head, body = packet(v5sub)
        self.assertEqual(head, 0x90)
        self.assertEqual(body[-1], 0x87)
        status, _ = b.http("PUT", "/api/v5/authorization/settings",
                           {"no_match": "deny", "deny_action": "disconnect"})
        self.assertEqual(status, 200)
        disc, _ = b.connect("sa22-disc")
        self.assertEqual(b.subscribe(disc, "sa22/deny/disc"), "80")
        disc.settimeout(3)
        with self.assertRaises(AssertionError):
            packet(disc)

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_retainer_expiry_and_disc_settings_do_not_preserve_retained_message(self):
        b = self.boot()
        options = {"enable": True, "msg_expiry_interval": "1ms",
                   "msg_expiry_interval_override": "1ms", "allow_never_expire": False,
                   "msg_clear_interval": "1ms", "msg_clear_limit": 1,
                   "flow_control": {"batch_read_number": 1, "batch_deliver_number": 1,
                                    "batch_deliver_limiter": "1/s"},
                   "delivery_rate": "1/s", "max_publish_rate": "1/s",
                   "backend": {"storage_type": "disc"}}
        status, _ = b.http("PUT", "/api/v5/mqtt/retainer", options)
        self.assertEqual(status, 200)
        pub, _ = b.connect("retained-publisher")
        publish(b, pub, "inventory/retained", b"retained", True)
        time.sleep(0.1)
        sub, _ = b.connect("retained-reader")
        self.assertEqual(b.subscribe(sub, "inventory/retained"), "00")
        head, body = packet(sub)
        self.assertEqual(head, 0x31)
        self.assertEqual(body, word("inventory/retained") + b"retained")
        status, current = b.http("GET", "/api/v5/mqtt/retainer")
        self.assertEqual(status, 200)
        self.assertEqual(current["backend"]["storage_type"], "disc")
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/mqtt/retainer")
        self.assertEqual(status, 200)
        self.assertEqual(current["backend"]["storage_type"], "ram")
        status, messages = b.http("GET", "/api/v5/mqtt/retainer/messages")
        self.assertEqual(status, 200)
        self.assertEqual(messages["meta"]["count"], 0)

    # // AZ-01 inventory: current behaviour; owner MV-08
    def test_source_accepts_missing_connector_and_keeps_unused_parameters(self):
        b = self.boot()
        config = {"type": "console", "name": "inventorysource", "enable": True,
                  "connector": "missing", "parameters": {"topic": "inventory/#"},
                  "resource_opts": {"health_check_interval": "1ms", "health_check_interval_jitter": "1ms",
                                    "health_check_timeout": "1ms", "inventory_unknown": "requested"},
                  "inventory_unknown": {"policy": "requested"},
                  "description": "inventory", "tags": ["audit"]}
        status, created = b.http("POST", "/api/v5/sources", config)
        self.assertEqual(status, 201)
        self.assertEqual(created["status"], "connected")
        self.assertEqual(created["connector"], "missing")
        pub, _ = b.connect("source-publisher")
        publish(b, pub, "inventory/source", b"input")
        status, source = b.http("GET", "/api/v5/sources/inventorysource")
        self.assertEqual(status, 200)
        self.assertEqual(source["parameters"], config["parameters"])
        self.assertEqual(source["resource_opts"], config["resource_opts"])
        self.assertEqual(source["inventory_unknown"], config["inventory_unknown"])
        status, _ = b.http("PUT", "/api/v5/sources/inventorysource",
                           {**config, "parameters": "invalid"})
        self.assertEqual(status, 400)
        b.stop()
        b.start()
        status, source = b.http("GET", "/api/v5/sources/inventorysource")
        self.assertEqual(status, 200)
        self.assertEqual(source["connector"], "missing")
        self.assertEqual(source["parameters"], config["parameters"])
        self.assertEqual(source["resource_opts"], config["resource_opts"])
        self.assertEqual(source["inventory_unknown"], config["inventory_unknown"])

    # // AZ-01 inventory: current behaviour; owner MV-10
    def test_disabled_api_listener_serves_nothing(self):
        b = self.broker
        b.configure()
        path = b.cfg / "indra.toml"
        path.write_text(path.read_text().replace("[listeners.api]\n", "[listeners.api]\nenabled=false\n"))
        b.start(wait_for_api=False)
        _, ack = b.connect("disabled-api")
        self.assertEqual(ack, "0000")
        with self.assertRaises(OSError):
            b.http("GET", "/api/v5/listeners")
        b.stop()
        b.start(wait_for_api=False)
        _, ack = b.connect("disabled-api-restart")
        self.assertEqual(ack, "0000")
        with self.assertRaises(OSError):
            b.http("GET", "/api/v5/metrics")

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_metrics_settings_do_not_change_scrape_policy(self):
        b = self.boot()
        status, _ = b.http("GET", "/api/v5/prometheus/stats", auth=False)
        self.assertEqual(status, 401)
        options = {"enable_basic_auth": False, "latency_buckets": "1ms,2ms",
                   "collectors": {"mnesia": "disabled", "vm_dist": "disabled",
                                  "vm_memory": "disabled", "vm_msacc": "disabled",
                                  "vm_statistics": "disabled", "vm_system_info": "disabled"},
                   "namespaced_metrics_limiter": {"rate": "1/30s"}}
        status, _ = b.http("PUT", "/api/v5/prometheus", options)
        self.assertEqual(status, 200)
        status, _ = b.http("GET", "/api/v5/prometheus/stats", auth=False)
        self.assertEqual(status, 401)
        for _ in range(3):
            status, text = b.http("GET", "/api/v5/prometheus/namespaced_stats")
            self.assertEqual(status, 200)
            self.assertIn("# TYPE", text)
            self.assertNotIn("_bucket{", text)
        status, current = b.http("GET", "/api/v5/prometheus")
        self.assertEqual(status, 200)
        self.assertFalse(current["enable_basic_auth"])
        self.assertEqual(current["collectors"], options["collectors"])
        self.assertEqual(current["namespaced_metrics_limiter"], options["namespaced_metrics_limiter"])
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/prometheus")
        self.assertEqual(status, 200)
        self.assertFalse(current["enable_basic_auth"])
        status, _ = b.http("GET", "/api/v5/prometheus/stats", auth=False)
        self.assertEqual(status, 401)

    # // AZ-01 inventory: current behaviour; owner new: listener lifecycle
    def test_created_listener_is_only_a_transient_record(self):
        b = self.boot()
        fixture = ROOT / "crates/broker-node/tests/k1_05"
        ports = set()
        while len(ports) < 4:
            ports.add(port())
        tcp_defaults = {"backlog": 8, "active_n": 10, "buffer": "4KB", "high_watermark": "1MB",
                        "keepalive": "none", "nodelay": True, "nolinger": False, "reuseaddr": True,
                        "send_timeout": "15s", "send_timeout_close": True}
        ssl_defaults = {"cacertfile": "", "ciphers": [], "versions": ["tlsv1.3", "tlsv1.2"],
                        "verify": "verify_none", "depth": 10, "reuse_sessions": False,
                        "secure_renegotiate": True, "honor_cipher_order": True, "client_renegotiation": True,
                        "enable_crl_check": False, "fail_if_no_peer_cert": False, "gc_after_handshake": False,
                        "handshake_timeout": "15s", "hibernate_after": "5s", "log_level": "notice",
                        "session_tickets": "disabled", "cacerts": False}
        websocket_defaults = {"allow_origin_absence": True, "check_origin_enable": False,
                              "check_origins": "http://localhost:18083, http://127.0.0.1:18083",
                              "compress": False, "fail_if_no_subprotocol": True, "idle_timeout": "7200s",
                              "max_frame_size": "infinity", "mqtt_path": "/mqtt", "mqtt_piggyback": "multiple",
                              "proxy_address_header": "", "proxy_port_header": "",
                              "supported_subprotocols": "mqtt, mqtt-v3, mqtt-v3.1.1, mqtt-v5", "validate_utf8": True}
        identifiers = []
        for kind, bind_port in zip(["tcp", "ssl", "ws", "wss"], ports):
            with self.subTest(kind=kind):
                name = "inventory" + kind
                identifier = kind + ":" + name
                config = {"id": identifier, "name": name, "type": kind, "enable": True,
                          "bind": "127.0.0.1:" + str(bind_port), "acceptors": 2,
                          "max_connections": 1, "zone": "default", "mountpoint": "",
                          "tcp_options": tcp_defaults, "ssl_options": dict(ssl_defaults),
                          "websocket": websocket_defaults}
                if kind in ["ssl", "wss"]:
                    config["ssl_options"].update({"certfile": str(fixture / "server-cert.pem"),
                                                  "keyfile": str(fixture / "server-key.pem")})
                status, created = b.http("POST", "/api/v5/listeners", config)
                self.assertEqual(status, 200)
                self.assertEqual(created["acceptors"], 2)
                self.assertEqual(created["bind"], config["bind"])
                self.assertEqual(created["max_connections"], 1)
                self.assertEqual(created["tcp_options"]["backlog"], 8)
                self.assert_fields_kept(created["tcp_options"], tcp_defaults)
                self.assert_fields_kept(created["websocket"], websocket_defaults)
                if "ssl_options" in config:
                    self.assert_fields_kept(created["ssl_options"], config["ssl_options"])
                with self.assertRaises(ConnectionRefusedError):
                    socket.create_connection(("127.0.0.1", bind_port), timeout=2)
                path = "/api/v5/listeners/" + identifier
                self.assertEqual(b.http("POST", path + "/start", {})[0], 400)
                self.assertEqual(b.http("PUT", path, {**config, "max_connections": 0})[0], 400)
                status, kept = b.http("GET", path)
                self.assertEqual(status, 200)
                self.assertEqual(kept["bind"], config["bind"])
                self.assertEqual(kept["max_connections"], 1)
                identifiers.append(identifier)
        b.stop()
        b.start()
        for identifier in identifiers:
            self.assertEqual(b.http("GET", "/api/v5/listeners/" + identifier)[0], 404)

    # // AZ-01 inventory: current behaviour; owner MV-11c
    # MV-11c now applies max_offline_queue at boot; the other two legs
    # still record gaps (max_inflight_messages, keep_alive_grace).
    def test_file_session_limits_do_not_set_live_bounds(self):
        b = self.boot(extra="[session]\nmax_offline_queue=1\nmax_inflight_messages=1\nkeep_alive_grace=3.0\n")
        status, actual = b.http("GET", "/api/v5/configs/global_zone")
        self.assertEqual(status, 200)
        self.assertEqual(actual["mqtt"]["max_mqueue_len"], 1)
        self.assertEqual(actual["mqtt"]["max_inflight"], 100)
        self.assertEqual(actual["mqtt"]["keepalive_multiplier"], 1.5)
        for key in ["max_offline_queue", "max_inflight_messages", "keep_alive_grace"]:
            status, loaded = b.http("GET", "/api/v1/config/explain?key=session." + key)
            self.assertEqual(status, 200)
            self.assertEqual(loaded["value"], "3.0" if key == "keep_alive_grace" else "1")
        sock, ack = b.connect("idle-grace", keepalive=2)
        self.assertEqual(ack, "0000")
        sock.settimeout(5.5)
        self.assertEqual(sock.recv(1), b"")

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_retainer_rate_settings_do_not_limit_socket_delivery(self):
        b = self.boot()
        options = {"enable": True, "delivery_rate": "1/s", "max_publish_rate": "1/s",
                   "flow_control": {"batch_read_number": 1, "batch_deliver_number": 1,
                                    "batch_deliver_limiter": "1/s"}}
        status, _ = b.http("PUT", "/api/v5/mqtt/retainer", options)
        self.assertEqual(status, 200)
        pub, _ = b.connect("rate-publisher")
        start = time.monotonic()
        for i in range(3):
            publish(b, pub, "inventory/rate/" + str(i), bytes([i]), True)
        sub, _ = b.connect("rate-reader")
        sub.settimeout(1.5)
        self.assertEqual(b.subscribe(sub, "inventory/rate/#"), "00")
        received = set()
        for _ in range(3):
            head, body = packet(sub)
            self.assertEqual(head, 0x31)
            received.add(body)
        self.assertEqual(received, {word("inventory/rate/" + str(i)) + bytes([i]) for i in range(3)})
        self.assertLess(time.monotonic() - start, 1.5)
        status, current = b.http("GET", "/api/v5/mqtt/retainer")
        self.assertEqual(status, 200)
        for key in ["delivery_rate", "max_publish_rate", "flow_control"]:
            self.assertEqual(current[key], options[key])

    # // AZ-01 inventory: current behaviour; owner new (outbound request filter)
    def test_rule_request_filter_does_not_block_loopback_delivery(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        policy = {"enable": True, "allow_cidrs": ["192.0.2.0/24"],
                  "deny_cidrs": ["127.0.0.0/8"], "deny_hosts": ["127.0.0.1"]}
        status, _ = b.http("PUT", "/api/v5/rule_engine", {"ssrf": policy})
        self.assertEqual(status, 200)
        status, _ = b.http("POST", "/api/v1/connectors",
                           {"id": "filtertarget", "kind": "webhook",
                            "config": {"url": receiver.url, "batch_size": 1,
                                       "linger_ms": 1, "max_retries": 0}})
        self.assertEqual(status, 201)
        status, _ = b.http("POST", "/api/v1/rules",
                           {"name": "filterrule", "topic_filter": "inventory/filter",
                            "actions": [{"type": "forwardconnector", "connector_id": "filtertarget"}]})
        self.assertEqual(status, 201)
        pub, _ = b.connect("request-filter")
        publish(b, pub, "inventory/filter", b'{"value":17}')
        path, body = receiver.requests.get(timeout=10)
        self.assertEqual(path, "/inventory")
        self.assertEqual(body, b'{"value":17}')
        status, current = b.http("GET", "/api/v5/rule_engine")
        self.assertEqual(status, 200)
        self.assertEqual(current["ssrf"], policy)
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/rule_engine")
        self.assertEqual(status, 200)
        self.assertEqual(current["ssrf"], policy)

    # // AZ-01 inventory: current behaviour; owner new (rule trace filter)
    def test_rule_trace_has_no_capture_while_topic_trace_captures(self):
        b = self.boot()
        status, rule = b.http("POST", "/api/v5/rules",
                              {"id": "inventoryrule", "sql": 'SELECT * FROM "inventory/trace"',
                               "actions": [{"function": "console"}]})
        self.assertEqual(status, 201)
        for name, kind, value in [("ruletrace", "ruleid", rule["id"]),
                                   ("topictrace", "topic", "inventory/trace")]:
            status, _ = b.http("POST", "/api/v5/trace",
                               {"name": name, "type": kind, kind: value,
                                "payload_encode": "text", "formatter": "json"})
            self.assertEqual(status, 200)
        pub, _ = b.connect("trace-publisher")
        publish(b, pub, "inventory/trace", b'{"value":23}')
        status, detail = b.http("GET", "/api/v5/trace/topictrace/log_detail")
        self.assertEqual(status, 200)
        self.assertGreater(detail[0]["size"], 0)
        status, detail = b.http("GET", "/api/v5/trace/ruletrace/log_detail")
        self.assertEqual(status, 200)
        self.assertEqual(detail[0]["size"], 0)
        status, rules = b.http("GET", "/api/v5/rules/inventoryrule/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(rules["metrics"]["matched"], 1)
        b.stop()
        b.start()
        status, traces = b.http("GET", "/api/v5/trace")
        self.assertEqual(status, 200)
        self.assertEqual(traces, [])

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_enabled_metrics_push_starts_no_request(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        gateway = {"enable": True, "url": receiver.url, "interval": "1ms",
                   "method": "post", "job_name": "inventory", "headers": {"X-Inventory": "audit"}}
        status, _ = b.http("PUT", "/api/v5/prometheus", {"push_gateway": gateway})
        self.assertEqual(status, 200)
        pub, _ = b.connect("push-metrics")
        publish(b, pub, "inventory/metrics", b"input")
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=2)
        status, current = b.http("GET", "/api/v5/prometheus")
        self.assertEqual(status, 200)
        self.assertEqual(current["push_gateway"], gateway)
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/prometheus")
        self.assertEqual(status, 200)
        self.assertEqual(current["push_gateway"], gateway)
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=2)

    # // AZ-01 inventory: current behaviour; owner MV-12a
    def test_transformation_decoder_and_encoder_are_only_stored(self):
        b = self.boot()
        config = {"name": "inventorycodec", "topics": ["inventory/encoded"],
                  "failure_action": "drop", "enable": True, "operations": [],
                  "payload_decoder": {"type": "json"}, "payload_encoder": {"type": "json"},
                  "log_failure": {"level": "none"}, "description": "inventory", "tags": ["audit"],
                  "inventory_unknown": {"policy": "requested"}}
        status, _ = b.http("POST", "/api/v5/message_transformations", config)
        self.assertEqual(status, 201)
        sub, _ = b.connect("codec-reader")
        self.assertEqual(b.subscribe(sub, "inventory/encoded"), "00")
        pub, _ = b.connect("codec-publisher")
        publish(b, pub, "inventory/encoded", b"not JSON")
        head, body = packet(sub)
        self.assertEqual(head, 0x30)
        self.assertEqual(body, word("inventory/encoded") + b"not JSON")
        status, stored = b.http("GET", "/api/v5/message_transformations/transformation/inventorycodec")
        self.assertEqual(status, 200)
        for key in ["payload_decoder", "payload_encoder", "log_failure", "description", "tags", "inventory_unknown"]:
            self.assertEqual(stored[key], config[key])
        status, _ = b.http("PUT", "/api/v5/message_transformations",
                           [{**config, "operations": [{"key": "not_a_field", "value": "x"}]}])
        self.assertEqual(status, 400)
        status, kept = b.http("GET", "/api/v5/message_transformations/transformation/inventorycodec")
        self.assertEqual(status, 200)
        self.assertEqual(kept, stored)
        b.stop()
        b.start()
        sub, _ = b.connect("codec-after-restart")
        self.assertEqual(b.subscribe(sub, "inventory/encoded"), "00")
        pub, _ = b.connect("codec-publisher-restart")
        publish(b, pub, "inventory/encoded", b"still not JSON")
        head, body = packet(sub)
        self.assertEqual(head, 0x30)
        self.assertEqual(body, word("inventory/encoded") + b"still not JSON")

    # // AZ-01 inventory: current behaviour; owner MV-09a
    def test_authenticator_order_skips_unsupported_source_when_builtin_exists(self):
        receiver = HttpReceiver(b'{"result":"deny"}')
        self.addCleanup(receiver.close)
        b = self.boot()
        external = {"id": "password_based:http", "mechanism": "password_based",
                    "backend": "http", "enable": True, **opaque_security_fields(receiver.url)}
        status, _ = b.http("POST", "/api/v5/authentication", external)
        self.assertEqual(status, 200)
        _, ack = b.connect("external-only", True)
        self.assertEqual(ack, "0004")
        builtin = {"id": "password_based:built_in_database", "mechanism": "password_based",
                   "backend": "built_in_database", "enable": True,
                   "password_hash_algorithm": {"name": "bcrypt", "salt_rounds": 12}}
        status, _ = b.http("POST", "/api/v5/authentication", builtin)
        self.assertEqual(status, 200)
        status, chain = b.http("GET", "/api/v5/authentication")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in chain], [external["id"], builtin["id"]])
        self.assert_fields_kept(chain[0], external)
        _, ack = b.connect("external-before-builtin", True)
        self.assertEqual(ack, "0000")
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=0.2)
        status, _ = b.http("PUT", "/api/v5/authentication/order", [{"id": external["id"]}])
        self.assertEqual(status, 400)
        b.stop()
        b.start()
        _, ack = b.connect("external-before-builtin-restart", True)
        self.assertEqual(ack, "0000")
        with self.assertRaises(queue.Empty):
            receiver.requests.get(timeout=0.2)
        status, chain = b.http("GET", "/api/v5/authentication")
        self.assertEqual(status, 200)
        self.assertEqual([row["id"] for row in chain], [external["id"], builtin["id"]])
        self.assert_fields_kept(chain[0], external)

    # // AZ-01 inventory: current behaviour; owner MV-09a
    def test_ldap_file_deadlines_are_replaced_by_runtime_defaults(self):
        b = self.broker
        for scheme in ["ldap", "ldaps"]:
            with self.subTest(scheme=scheme):
                peer = SilentPeer()
                try:
                    b.configure(extra=f"[ldap]\nserver_url=\"{scheme}://127.0.0.1:{peer.port}\"\n"
                                + "base_dn=\"dc=inventory\"\nbind_dn=\"cn=inventory,dc=inventory\"\n"
                                + "bind_password=" + json.dumps(b.password)
                                + "\npool_size=1\nconnect_timeout_ms=100\nread_timeout_ms=100\n")
                    b.start()
                    started = time.monotonic()
                    _, ack = b.connect("ldap-deadline-" + scheme, True)
                    elapsed = time.monotonic() - started
                    self.assertEqual(ack, "0004")
                    self.assertGreater(elapsed, 1.0)
                    self.assertLess(elapsed, 9.0)
                    self.assertGreaterEqual(len(peer.clients), 1)
                    for key in ["pool_size", "connect_timeout_ms", "read_timeout_ms"]:
                        status, loaded = b.http("GET", "/api/v1/config/explain?key=ldap." + key)
                        self.assertEqual(status, 200)
                        self.assertEqual(loaded["value"], "1" if key == "pool_size" else "100")
                finally:
                    b.stop()
                    peer.close()

    # // AZ-01 inventory: current behaviour; owner new (ban state)
    def test_future_ban_applies_now_and_is_lost_at_restart(self):
        b = self.boot()
        config = {"as": "clientid", "who": "future-banned", "at": int(time.time()) + 86400,
                  "until": "infinity", "by": "inventory", "reason": "audit"}
        status, _ = b.http("POST", "/api/v5/banned", config)
        self.assertEqual(status, 200)
        _, ack = b.connect("future-banned")
        self.assertEqual(ack, "0005")
        _, ack = b.connect("other-client")
        self.assertEqual(ack, "0000")
        status, _ = b.http("POST", "/api/v5/banned", {**config, "until": {}})
        self.assertEqual(status, 400)
        status, current = b.http("GET", "/api/v5/banned")
        self.assertEqual(status, 200)
        self.assertEqual(current["data"][0]["who"], config["who"])
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/banned")
        self.assertEqual(status, 200)
        self.assertEqual(current["data"], [])
        _, ack = b.connect("future-banned")
        self.assertEqual(ack, "0000")

    def test_auto_subscribe_respects_the_same_topic_acl(self):
        b = self.boot()
        status, _ = b.http("POST", "/api/v1/auth/acls",
                           {"client_pattern": "allowed-publisher", "action": "publish",
                            "topic_pattern": "inventory/protected", "allow": True})
        self.assertEqual(status, 201)
        pub, ack = b.connect("allowed-publisher")
        self.assertEqual(ack, "0000")
        entries = [{"topic": "inventory/protected", "qos": 0, "rh": 0, "rap": 0, "nl": 0}]
        status, _ = b.http("PUT", "/api/v5/mqtt/auto_subscribe", entries)
        self.assertEqual(status, 200)
        sub, ack = b.connect("denied-subscriber")
        self.assertEqual(ack, "0000")
        # The ACL does not grant subscribe: the auto-subscribe is refused
        # and not applied, so the publish reaches no socket.
        self.assertEqual(b.subscribe(sub, "inventory/protected"), "80")
        publish(b, pub, "inventory/protected", b"protected payload")
        sub.settimeout(0.3)
        with self.assertRaises(TimeoutError):
            packet(sub)
        sub.settimeout(10)
        # Grant subscribe to the same client: the next bind applies the
        # auto-subscribe and delivery reaches the socket.
        status, _ = b.http("POST", "/api/v1/auth/acls",
                           {"client_pattern": "denied-subscriber", "action": "subscribe",
                            "topic_pattern": "inventory/protected", "allow": True})
        self.assertEqual(status, 201)
        sub2, ack = b.connect("denied-subscriber")
        self.assertEqual(ack, "0000")
        publish(b, pub, "inventory/protected", b"allowed payload")
        head, body = packet(sub2)
        self.assertEqual(head, 0x30)
        self.assertEqual(body, word("inventory/protected") + b"allowed payload")
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/mqtt/auto_subscribe")
        self.assertEqual(status, 200)
        self.assertEqual(current, [])

    # // AZ-01 inventory: current behaviour; owner MV-01
    def test_action_parameters_have_no_action_owned_sink(self):
        b = self.boot()
        status, _ = b.http("POST", "/api/v5/connectors", {"type": "console", "name": "inventoryconnector"})
        self.assertEqual(status, 201)
        # // AZ-01 inventory: current behaviour; owner RO-01
        resource_options = {"batch_size": 17, "batch_time": "5ms", "dispatch_strategy": "random",
                            "health_check_interval": "1ms", "health_check_interval_jitter": "1ms",
                            "health_check_timeout": "1ms", "inflight_window": 1,
                            "max_buffer_bytes": "1KB", "query_mode": "sync", "request_ttl": "1ms",
                            "worker_pool_size": 2}
        config = {"type": "console", "name": "inventoryaction", "enable": True,
                  "connector": "inventoryconnector", "parameters": {"template": "${payload}"},
                  "resource_opts": resource_options, "description": "inventory", "tags": ["audit"],
                  "fallback_actions": ["missingfallback"], "inventory_unknown": {"policy": "requested"}}
        status, stored = b.http("POST", "/api/v5/actions", config)
        self.assertEqual(status, 201)
        for key in ["connector", "parameters", "resource_opts", "description", "tags", "fallback_actions", "inventory_unknown"]:
            self.assertEqual(stored[key], config[key])
        status, _ = b.http("POST", "/api/v5/rules",
                           {"id": "inventoryactionrule", "sql": 'SELECT * FROM "inventory/action"',
                            "actions": ["console:inventoryaction"]})
        self.assertEqual(status, 201)
        pub, _ = b.connect("action-publisher")
        publish(b, pub, "inventory/action", b'{"value":31}')
        status, stats = b.http("GET", "/api/v5/actions/inventoryaction/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(stats["metrics"], {"total": 1, "success": 0, "failed": 1})
        status, _ = b.http("PUT", "/api/v5/actions/inventoryaction", {**config, "parameters": "invalid"})
        self.assertEqual(status, 400)
        status, kept = b.http("GET", "/api/v5/actions/inventoryaction")
        self.assertEqual(status, 200)
        self.assertEqual(kept, stored)
        b.stop()
        b.start()
        pub, _ = b.connect("action-publisher-restart")
        publish(b, pub, "inventory/action", b'{"value":32}')
        status, stats = b.http("GET", "/api/v5/actions/inventoryaction/metrics")
        self.assertEqual(status, 200)
        self.assertEqual(stats["metrics"], {"total": 1, "success": 0, "failed": 1})
        status, kept = b.http("GET", "/api/v5/actions/inventoryaction")
        self.assertEqual(status, 200)
        for key in ["id", "name", "type", "enable", "connector", "parameters", "resource_opts",
                    "description", "tags", "fallback_actions", "inventory_unknown"]:
            self.assertEqual(kept[key], stored[key])

    # // AZ-01 inventory: current behaviour; owner MV-06a
    def test_failed_engine_save_splits_reported_policy_from_rule_dispatch(self):
        b = self.boot()
        status, _ = b.http("PUT", "/api/v5/rule_engine", {"ignore_sys_message": False})
        self.assertEqual(status, 200)
        status, _ = b.http("POST", "/api/v5/rules",
                           {"id": "inventorysystem", "sql": 'SELECT * FROM "$SYS/inventory"',
                            "actions": [{"function": "republish", "args": {
                                "topic": "inventory/engine-out", "payload": "engine-output",
                                "qos": 0, "retain": False, "user_properties": ""}}]})
        self.assertEqual(status, 201)
        sub, _ = b.connect("engine-reader")
        self.assertEqual(b.subscribe(sub, "inventory/engine-out"), "00")
        pub, _ = b.connect("engine-publisher")

        def output():
            publish(b, pub, "$SYS/inventory", b'{"value":37}')
            head, body = packet(sub)
            self.assertEqual(head, 0x30)
            self.assertEqual(body, word("inventory/engine-out") + b"engine-output")

        output()
        saved = b.base / "saved-data"
        b.data.rename(saved)
        try:
            status, _ = b.http("PUT", "/api/v5/rule_engine", {"ignore_sys_message": True})
            self.assertEqual(status, 500)
            status, current = b.http("GET", "/api/v5/rule_engine")
            self.assertEqual(status, 200)
            self.assertTrue(current["ignore_sys_message"])
            output()
        finally:
            self.assertFalse(b.data.exists())
            saved.rename(b.data)
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/rule_engine")
        self.assertEqual(status, 200)
        self.assertFalse(current["ignore_sys_message"])
        sub, _ = b.connect("engine-reader-restart")
        self.assertEqual(b.subscribe(sub, "inventory/engine-out"), "00")
        pub, _ = b.connect("engine-publisher-restart")
        output()

    # // AZ-01 inventory: current behaviour; owner MV-10
    def test_stored_connector_boot_keeps_the_configured_node_identity(self):
        b = self.boot()
        status, first = b.http("POST", "/api/v5/connectors",
                               {"type": "console", "name": "identityconnector"})
        self.assertEqual(status, 201)
        self.assertEqual(first["node_status"][0]["node"].split("@")[0], "az-01-entry")
        b.stop()
        b.start()
        status, current = b.http("GET", "/api/v5/connectors/identityconnector")
        self.assertEqual(status, 200)
        self.assertEqual(current["node_status"][0]["node"].split("@")[0], "az-01-entry")
        status, loaded = b.http("GET", "/api/v1/config/explain?key=node.id")
        self.assertEqual(status, 200)
        self.assertEqual(loaded["value"], '"az-01-entry"')

    # // AZ-01 inventory: current behaviour; owner MV-01
    def test_failed_connector_save_switches_delivery_before_restart(self):
        old = HttpReceiver()
        new = HttpReceiver()
        self.addCleanup(old.close)
        self.addCleanup(new.close)
        b = self.boot()
        config = {"type": "webhook", "name": "inventorywebhook", "url": old.url,
                  "method": "put", "headers": {"X-Inventory": "requested"},
                  "resource_opts": {"health_check_interval": "1ms", "health_check_timeout": "1ms",
                                    "start_after_created": False, "start_timeout": "1ms"},
                  "inventory_unknown": {"policy": "requested"}}
        status, _ = b.http("POST", "/api/v5/connectors", config)
        self.assertEqual(status, 201)
        status, _ = b.http("POST", "/api/v1/rules",
                           {"name": "webhookrule", "topic_filter": "inventory/webhook",
                            "actions": [{"type": "forwardconnector", "connector_id": "inventorywebhook"}]})
        self.assertEqual(status, 201)
        pub, _ = b.connect("webhook-publisher")

        def delivered(receiver, value):
            payload = json.dumps({"value": value}, separators=(",", ":")).encode()
            publish(b, pub, "inventory/webhook", payload)
            path, body = receiver.requests.get(timeout=10)
            self.assertEqual(path, "/inventory")
            self.assertEqual(body, payload)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            headers = receiver.request_headers.get(timeout=10)
            self.assertTrue("x-inventory" not in headers, "unexpected request header")
            self.assertEqual(headers["x-mqtt-topic"], "inventory/webhook")

        delivered(old, 41)
        saved = b.base / "saved-data"
        b.data.rename(saved)
        try:
            status, _ = b.http("PUT", "/api/v5/connectors/inventorywebhook", {**config, "url": new.url})
            self.assertEqual(status, 500)
            status, current = b.http("GET", "/api/v5/connectors/inventorywebhook")
            self.assertEqual(status, 200)
            self.assertEqual(current["url"], new.url)
            self.assertEqual(current["resource_opts"], config["resource_opts"])
            self.assertEqual(current["inventory_unknown"], config["inventory_unknown"])
            delivered(new, 42)
            self.assertTrue(old.requests.empty())
        finally:
            self.assertFalse(b.data.exists())
            saved.rename(b.data)
        b.stop()
        b.start()
        pub, _ = b.connect("webhook-publisher-restart")
        delivered(old, 43)
        status, current = b.http("GET", "/api/v5/connectors/inventorywebhook")
        self.assertEqual(status, 200)
        self.assertEqual(current["url"], old.url)
        self.assertEqual(current["resource_opts"], config["resource_opts"])
        self.assertEqual(current["inventory_unknown"], config["inventory_unknown"])

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_file_window_policy_is_accepted_while_window_entry_runs(self):
        b = self.boot(extra='[rules_engine]\nwindow_channel_depth=1\nbackpressure_policy="DropNewest"\n')
        status, _ = b.http("POST", "/api/v5/rules",
                           {"id": "inventorywindow", "sql": 'SELECT sum(v) AS total FROM "inventory/window" GROUP BY COUNTWINDOW(2)',
                            "actions": [{"function": "republish", "args": {
                                "topic": "inventory/window-out", "payload": "${total}",
                                "qos": 0, "retain": False, "user_properties": ""}}]})
        self.assertEqual(status, 201)

        def entry():
            for key, expected in [("window_channel_depth", "1"),
                                   ("backpressure_policy", '"DropNewest"')]:
                status, loaded = b.http("GET", "/api/v1/config/explain?key=rules_engine." + key)
                self.assertEqual(status, 200)
                self.assertEqual(loaded["value"], expected)
            sub, _ = b.connect("window-reader")
            self.assertEqual(b.subscribe(sub, "inventory/window-out"), "00")
            pub, _ = b.connect("window-publisher")
            publish(b, pub, "inventory/window", b'{"v":1}')
            publish(b, pub, "inventory/window", b'{"v":3}')
            head, body = packet(sub)
            self.assertEqual(head, 0x30)
            self.assertEqual(body, word("inventory/window-out") + b"4")

        entry()
        b.stop()
        b.start()
        entry()
        # The boot call-site audit proves the two values are not installed.
        # This entry case records acceptance and window output, not saturation.


    # // AZ-01 inventory: current behaviour; owner MV-11a
    def test_topic_rewrite_drops_session_placeholder_and_loses_rules_at_restart(self):
        b = self.boot()
        rules = [{"action": "publish", "source_topic": "inventory/rewrite/+",
                  "re": "^inventory/rewrite/(.+)$",
                  "dest_topic": "inventory/${clientid}/$1"}]
        status, current = b.http("PUT", "/api/v5/mqtt/topic_rewrite", rules)
        self.assertEqual(status, 200)
        self.assertEqual(current, rules)
        sub, _ = b.connect("rewrite-reader")
        self.assertEqual(b.subscribe(sub, "inventory//value"), "00")
        pub, _ = b.connect("rewrite-publisher")

        def delivery():
            publish(b, pub, "inventory/rewrite/value", b"rewrite-payload")
            self.assertEqual(packet(sub), (0x30, word("inventory//value") + b"rewrite-payload"))

        delivery()
        status, _ = b.http("PUT", "/api/v5/mqtt/topic_rewrite", [{**rules[0], "re": "["}])
        self.assertEqual(status, 400)
        self.assertEqual(b.http("GET", "/api/v5/mqtt/topic_rewrite"), (200, rules))
        delivery()
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", "/api/v5/mqtt/topic_rewrite"), (200, []))
        sub, _ = b.connect("rewrite-reader-restart")
        self.assertEqual(b.subscribe(sub, "inventory/rewrite/value"), "00")
        pub, _ = b.connect("rewrite-publisher-restart")
        publish(b, pub, "inventory/rewrite/value", b"after-restart")
        self.assertEqual(packet(sub), (0x30, word("inventory/rewrite/value") + b"after-restart"))

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_delayed_settings_apply_live_but_reset_at_restart(self):
        b = self.boot()
        status, original = b.http("GET", "/api/v5/mqtt/delayed")
        self.assertEqual(status, 200)
        chosen = {"enable": False, "max_delayed_messages": 1}
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/delayed", chosen), (200, chosen))
        pub, _ = b.connect("delayed-publisher")
        publish(b, pub, "$delayed/3600/inventory/delay", b"not-deferred")
        status, waiting = b.http("GET", "/api/v5/mqtt/delayed/messages")
        self.assertEqual(status, 200)
        self.assertEqual(waiting["meta"]["count"], 0)
        status, _ = b.http("PUT", "/api/v5/mqtt/delayed", {"enable": True, "max_delayed_messages": -1})
        self.assertEqual(status, 400)
        self.assertEqual(b.http("GET", "/api/v5/mqtt/delayed"), (200, chosen))
        chosen["enable"] = True
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/delayed", chosen), (200, chosen))
        publish(b, pub, "$delayed/3600/inventory/delay", b"first")
        publish(b, pub, "$delayed/3600/inventory/delay", b"second")
        status, waiting = b.http("GET", "/api/v5/mqtt/delayed/messages")
        self.assertEqual(status, 200)
        self.assertEqual(waiting["meta"]["count"], 1)
        self.assertEqual(base64.b64decode(waiting["data"][0]["payload"]), b"first")
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", "/api/v5/mqtt/delayed"), (200, original))
        sub, _ = b.connect("delayed-reader-restart")
        self.assertEqual(b.subscribe(sub, "inventory/delay"), "00")
        pub, _ = b.connect("delayed-publisher-restart")
        started = time.monotonic()
        publish(b, pub, "$delayed/1/inventory/delay", b"deferred")
        self.assertEqual(packet(sub), (0x30, word("inventory/delay") + b"deferred"))
        self.assertGreater(time.monotonic() - started, 0.5)

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_retainer_clear_flag_does_not_stop_empty_retained_publish(self):
        b = self.boot()
        status, original = b.http("GET", "/api/v5/mqtt/retainer")
        self.assertEqual(status, 200)
        status, changed = b.http("PUT", "/api/v5/mqtt/retainer", {"stop_publish_clear_msg": True})
        self.assertEqual(status, 200)
        self.assertTrue(changed["stop_publish_clear_msg"])
        sub, _ = b.connect("retain-clear-reader")
        self.assertEqual(b.subscribe(sub, "inventory/clear"), "00")
        pub, _ = b.connect("retain-clear-publisher")
        publish(b, pub, "inventory/clear", b"kept", retain=True)
        self.assertEqual(packet(sub), (0x31, word("inventory/clear") + b"kept"))
        publish(b, pub, "inventory/clear", b"", retain=True)
        self.assertEqual(packet(sub), (0x31, word("inventory/clear")))
        status, messages = b.http("GET", "/api/v5/mqtt/retainer/messages")
        self.assertEqual(status, 200)
        self.assertEqual(messages["meta"]["count"], 0)
        status, _ = b.http("PUT", "/api/v5/mqtt/retainer", {"stop_publish_clear_msg": "yes"})
        self.assertEqual(status, 400)
        self.assertEqual(b.http("GET", "/api/v5/mqtt/retainer"), (200, changed))
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", "/api/v5/mqtt/retainer"), (200, original))

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_license_timezone_is_not_applied_to_stored_history(self):
        b = self.broker
        b.configure()
        b.data.mkdir()
        # A genuine old measurement: 2020-01-01 at 23:30 UTC falls on
        # January 2 with +02:00. Seed its supported legacy file format.
        measured = 1577921400
        settings = {"connection_low_watermark": "1%", "connection_high_watermark": "2%",
                    "high_watermark_timezone": "+02:00"}
        (b.data / "license_state.json").write_text(json.dumps({
            "setting": settings,
            "days": {str(measured // 86400): {"high_watermark": 12345, "observed_at": measured}}}))
        b.start()

        def history():
            self.assertEqual(b.http("GET", "/api/v5/license/setting"), (200, settings))
            status, rows = b.http("GET", "/api/v5/license/session_hwm_history?period=daily")
            self.assertEqual(status, 200)
            selected = [row for row in rows["data"] if row["high_watermark"] == 12345]
            self.assertEqual(len(selected), 1)
            self.assertEqual(selected[0]["period"], "2020-01-01")

        history()
        status, _ = b.http("PUT", "/api/v5/license/setting", {"connection_low_watermark": "9%"})
        self.assertEqual(status, 400)
        history()
        status, current = b.http("PUT", "/api/v5/license/setting", {"high_watermark_timezone": "not-a-timezone"})
        self.assertEqual(status, 200)
        self.assertEqual(current["high_watermark_timezone"], "not-a-timezone")
        settings["high_watermark_timezone"] = "not-a-timezone"
        b.stop()
        b.start()
        history()


    # // AZ-01 inventory: current behaviour; owner MV-11b
    def test_namespace_reference_limits_are_stored_without_enforcement(self):
        b = self.boot(extra='[[tenants.rules]]\nlistener="*"\nattribute="tenant"\nexpression="inventorytenant"\n')
        self.assertEqual(b.http("POST", "/api/v5/mt/ns/inventorytenant")[0], 204)
        # These are the reference API paths; the local-only `limits`
        # block has different readers and must not stand in for them.
        chosen = {"session": {"max_sessions": 1},
                  "limiter": {
                      "client": {key: {"rate": "1B/1s" if "bytes" in key else "1/1s", "burst": "0/s"}
                                 for key in ["bytes", "messages", "delivery_bytes", "delivery_messages", "subscribes"]},
                      "tenant": {key: {"rate": "1B/1s" if key == "bytes" else "1/1s", "burst": "0/s"}
                                 for key in ["bytes", "messages"]}}}
        chosen["inventory_unknown"] = {"policy": "requested"}
        path = "/api/v5/mt/ns/inventorytenant/config"
        self.assertEqual(b.http("PUT", path, chosen), (200, chosen))

        def traffic(suffix):
            sub, ack = b.connect("namespace-reader" + suffix)
            self.assertEqual(ack, "0000")
            pub, ack = b.connect("namespace-publisher" + suffix)
            self.assertEqual(ack, "0000")
            start = time.monotonic()
            for topic in ["inventory/ns", "inventory/ns2", "inventory/ns3"]:
                self.assertEqual(b.subscribe(sub, topic), "00")
            status, rows = b.http("GET", "/api/v5/mt/ns/inventorytenant/client_list")
            self.assertEqual(status, 200)
            self.assertEqual(len(rows), 2)
            for value in [b"one", b"two", b"three"]:
                publish(b, pub, "inventory/ns", value)
                self.assertEqual(packet(sub), (0x30, word("inventory/ns") + value))
            self.assertLess(time.monotonic() - start, 1.5)

        traffic("")
        self.assertEqual(b.http("PUT", path, [chosen])[0], 400)
        self.assertEqual(b.http("GET", path), (200, chosen))
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", path), (200, chosen))
        traffic("-restart")

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_global_zone_failed_save_changes_live_alias_then_reverts(self):
        b = self.boot()
        path = "/api/v5/configs/global_zone"
        chosen = {"mqtt": {"max_inflight": 1, "max_mqueue_len": 3, "max_topic_alias": 2}}
        status, original = b.http("PUT", path, chosen)
        self.assertEqual(status, 200)
        _, props = b.connect_v5("global-zone-before")
        self.assertEqual(props[0x22], 2)
        self.assertEqual(b.http("PUT", path, {"mqtt": {"max_topic_alias": 65536}})[0], 400)
        self.assertEqual(b.http("GET", path), (200, original))
        saved = b.base / "saved-data"
        b.data.rename(saved)
        try:
            self.assertEqual(b.http("PUT", path, {"mqtt": {"max_topic_alias": 3}})[0], 500)
            self.assertEqual(b.http("GET", path)[1]["mqtt"]["max_topic_alias"], 3)
            _, props = b.connect_v5("global-zone-after-failure")
            self.assertEqual(props[0x22], 3)
        finally:
            self.assertFalse(b.data.exists())
            saved.rename(b.data)
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", path), (200, original))
        _, props = b.connect_v5("global-zone-restart")
        self.assertEqual(props[0x22], 2)


    # // AZ-01 inventory: current behaviour; owner MV-11a
    def test_auto_subscribe_options_do_not_replay_retained_state(self):
        b = self.boot()
        pub, _ = b.connect("auto-options-publisher")
        publish(b, pub, "inventory/auto-options", b"old-retained", retain=True)
        chosen = [{"topic": "inventory/auto-options", "qos": 1, "rh": 0, "rap": 0, "nl": 1}]
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/auto_subscribe", chosen), (200, chosen))
        victim, ack = b.connect("auto-options-reader")
        self.assertEqual(ack, "0000")
        victim.settimeout(1.2)
        with self.assertRaises(TimeoutError):
            packet(victim)
        victim.settimeout(10)
        # The retained row really exists: an ordinary subscription gets it.
        manual, _ = b.connect("auto-options-manual")
        self.assertEqual(b.subscribe(manual, "inventory/auto-options"), "00")
        self.assertEqual(packet(manual), (0x31, word("inventory/auto-options") + b"old-retained"))
        # A real QoS 1 publication reaches the automatic QoS 1 subscription.
        pub.sendall(frame(0x32, word("inventory/auto-options") + b"\x00\x01" + b"live"))
        self.assertEqual(packet(pub), (0x40, b"\x00\x01"))
        head, body = packet(victim)
        self.assertEqual(head, 0x32)
        prefix = word("inventory/auto-options")
        self.assertEqual(body[:len(prefix)], prefix)
        self.assertEqual(body[len(prefix) + 2:], b"live")
        victim.sendall(frame(0x40, body[len(prefix):len(prefix) + 2]))
        publish(b, victim, "inventory/auto-options", b"self-suppressed")
        victim.settimeout(1.2)
        with self.assertRaises(TimeoutError):
            packet(victim)
        victim.settimeout(10)
        pub.sendall(frame(0x33, prefix + b"\x00\x02" + b"retained-live"))
        self.assertEqual(packet(pub), (0x40, b"\x00\x02"))
        head, body = packet(victim)
        self.assertEqual(head, 0x32)
        self.assertEqual(body[:len(prefix)], prefix)
        self.assertEqual(body[len(prefix) + 2:], b"retained-live")
        victim.sendall(frame(0x40, body[len(prefix):len(prefix) + 2]))
        chosen[0]["rap"] = 1
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/auto_subscribe", chosen), (200, chosen))
        preserve, ack = b.connect("auto-options-preserve")
        self.assertEqual(ack, "0000")
        pub.sendall(frame(0x33, prefix + b"\x00\x03" + b"retained-preserved"))
        self.assertEqual(packet(pub), (0x40, b"\x00\x03"))
        for target, expected_head in [(victim, 0x32), (preserve, 0x33)]:
            head, body = packet(target)
            self.assertEqual(head, expected_head)
            self.assertEqual(body[:len(prefix)], prefix)
            self.assertEqual(body[len(prefix) + 2:], b"retained-preserved")
            target.sendall(frame(0x40, body[len(prefix):len(prefix) + 2]))
        status, _ = b.http("PUT", "/api/v5/mqtt/auto_subscribe", [{**chosen[0], "rh": 3}])
        self.assertEqual(status, 400)
        self.assertEqual(b.http("GET", "/api/v5/mqtt/auto_subscribe"), (200, chosen))
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", "/api/v5/mqtt/auto_subscribe"), (200, []))


    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_slow_subscription_ack_mode_expiry_and_settings_restart(self):
        b = self.boot()
        path = "/api/v5/slow_subscriptions/settings"
        status, original = b.http("GET", path)
        self.assertEqual(status, 200)
        chosen = {"enable": True, "threshold": "50ms", "top_k_num": 1,
                  "expire_interval": "1h", "stats_type": "internal"}
        self.assertEqual(b.http("PUT", path, chosen), (200, chosen))
        sub, _ = b.connect("slow-options-reader")
        for topic in ["inventory/slow-one", "inventory/slow-two", "inventory/slow-disabled"]:
            self.assertEqual(b.subscribe(sub, topic, qos=1), "01")
        pub, _ = b.connect("slow-options-publisher")

        def delivery(topic, delay, number):
            pub.sendall(frame(0x32, word(topic) + struct.pack("!H", number) + b"slow-payload"))
            self.assertEqual(packet(pub), (0x40, struct.pack("!H", number)))
            head, body = packet(sub)
            self.assertEqual(head, 0x32)
            prefix = word(topic)
            self.assertEqual(body[:len(prefix)], prefix)
            self.assertEqual(body[len(prefix) + 2:], b"slow-payload")
            time.sleep(delay)
            sub.sendall(frame(0x40, body[len(prefix):len(prefix) + 2]))
            sub.sendall(b"\xc0\x00")
            self.assertEqual(packet(sub), (0xd0, b""))

        def rows():
            status, current = b.http("GET", "/api/v5/slow_subscriptions")
            self.assertEqual(status, 200)
            return current["data"]

        delivery("inventory/slow-one", 0.1, 1)
        deadline = time.monotonic() + 5
        while not rows() and time.monotonic() < deadline:
            time.sleep(0.02)
        first = rows()
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["topic"], "inventory/slow-one")
        # The ACK hook uses the complete held-ACK age even for internal mode.
        self.assertGreaterEqual(first[0]["timespan"], 80)
        delivery("inventory/slow-two", 0.2, 2)
        deadline = time.monotonic() + 5
        while rows()[0]["topic"] != "inventory/slow-two" and time.monotonic() < deadline:
            time.sleep(0.02)
        ranked = rows()
        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0]["topic"], "inventory/slow-two")
        chosen["expire_interval"] = "1ms"
        self.assertEqual(b.http("PUT", path, chosen), (200, chosen))
        time.sleep(0.1)
        self.assertEqual(rows(), ranked)
        status, _ = b.http("PUT", path, {"threshold": "0ms", "enable": False})
        self.assertEqual(status, 400)
        self.assertEqual(b.http("GET", path), (200, chosen))
        chosen["enable"] = False
        self.assertEqual(b.http("PUT", path, chosen), (200, chosen))
        self.assertEqual(b.http("DELETE", "/api/v5/slow_subscriptions")[0], 204)
        delivery("inventory/slow-disabled", 0.1, 3)
        time.sleep(0.2)
        self.assertEqual(rows(), [])
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", path), (200, original))


    # // AZ-01 inventory: current behaviour; owner MV-09a
    def test_ldap_file_pool_one_allows_three_simultaneous_backend_binds(self):
        peer = SilentPeer()
        self.addCleanup(peer.close)
        b = self.boot(extra='[ldap]\nserver_url="ldap://127.0.0.1:' + str(peer.port)
                      + '"\nbase_dn="dc=inventory"\nbind_dn="cn=inventory"\nbind_password="'
                      + secrets.token_hex(20) + '"\npool_size=1\nconnect_timeout_ms=100\nread_timeout_ms=100\n')
        ready = threading.Event()
        answers = []
        errors = []

        def connect(index):
            ready.wait(timeout=5)
            try:
                answers.append(b.connect("ldap-pool-" + str(index), authenticated=True)[1])
            except Exception as error:
                errors.append(type(error).__name__)

        workers = [threading.Thread(target=connect, args=(index,), daemon=True)
                   for index in range(3)]
        for worker in workers:
            worker.start()
        ready.set()
        try:
            deadline = time.monotonic() + 2
            while len(peer.clients) < 3 and time.monotonic() < deadline:
                time.sleep(0.02)
            # No backend has answered, yet all three binds have reached it.
            # A pool with capacity one would hold the other two callers.
            self.assertEqual(len(peer.clients), 3)
        finally:
            for worker in workers:
                worker.join(timeout=10)
            self.assertFalse(any(worker.is_alive() for worker in workers))
        self.assertEqual(errors, [])
        self.assertEqual(answers, ["0004"] * 3)


    # // AZ-01 inventory: current behaviour; owner MV-15
    def test_excluded_sso_and_protobuf_configuration_is_stored(self):
        b = self.boot()
        config = {"backend": "oidc", "enable": True,
                  "issuer": "http://127.0.0.1:1/inventory", "clientid": "inventory"}
        path = "/api/v5/sso/oidc"
        status, stored = b.http("PUT", path, config)
        self.assertEqual(status, 200)
        self.assertTrue(stored["enable"])
        self.assertEqual(stored["issuer"], config["issuer"])
        status, refusal = b.http("POST", "/api/v5/sso/login/oidc", {"username": "inventory"}, auth=False)
        self.assertEqual(status, 404)
        self.assertEqual(refusal["code"], "BACKEND_NOT_FOUND")
        self.assertEqual(b.http("PUT", path, {**config, "enable": "yes"})[0], 400)
        self.assertEqual(b.http("GET", path), (200, stored))
        schema = {"name": "inventoryproto", "type": "protobuf", "description": "Inventory schema",
                  "source": 'syntax = "proto3"; message Reading { string value = 1; }'}
        status, proto = b.http("POST", "/api/v5/schema_registry", schema)
        self.assertEqual(status, 201)
        self.assertEqual(proto["type"], "protobuf")
        self.assertEqual(proto["source"], schema["source"])
        self.assertEqual(b.http("POST", "/api/v5/schema_registry", {**schema, "type": "unknown"})[0], 400)
        self.assertEqual(b.http("GET", "/api/v5/schema_registry/inventoryproto"), (200, proto))
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", path), (200, stored))
        self.assertEqual(b.http("GET", "/api/v5/schema_registry/inventoryproto"), (200, proto))

    # // AZ-01 inventory: current behaviour; owner MV-15
    def test_excluded_gateway_and_stream_start_from_file(self):
        b = self.broker
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
            probe.bind(("127.0.0.1", 0))
            coap_port = probe.getsockname()[1]
        journal = b.base / "stream"
        b.configure(extra='[gateway]\ncoap_bind="127.0.0.1:' + str(coap_port)
                    + '"\n[persistence]\nstream_dir="' + str(journal) + '"\n')
        b.start()

        def traffic(suffix):
            sub, _ = b.connect("excluded-reader" + suffix)
            self.assertEqual(b.subscribe(sub, "inventory/excluded"), "00")
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client:
                client.settimeout(5)
                # Confirmable POST /ps/inventory/excluded, no token.
                request = b"\x40\x02\x00\x01\xb2ps\x09inventory\x08excluded\xffcoap-payload"
                client.sendto(request, ("127.0.0.1", coap_port))
                self.assertEqual(client.recv(2048), b"\x60\x44\x00\x01")
            self.assertEqual(packet(sub), (0x31, word("inventory/excluded") + b"coap-payload"))
            pub, _ = b.connect("excluded-publisher" + suffix)
            payload = ("journal-payload" + suffix).encode()
            publish(b, pub, "inventory/excluded", payload)
            self.assertEqual(packet(sub), (0x30, word("inventory/excluded") + payload))
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if any(payload in file.read_bytes() for file in journal.rglob("*") if file.is_file()):
                    return
                time.sleep(0.02)
            self.fail("configured stream did not journal the real publication")

        traffic("")
        b.stop()
        b.start()
        traffic("-restart")

    # MV-11b replaces the inventory: the schema refuses the entry.
    def test_cluster_seeds_start_udp_even_when_enabled_is_false(self):
        b = self.broker
        b.configure(extra='[cluster]\nseed_nodes=["127.0.0.1:1"]\n')
        with self.assertRaises(AssertionError) as ctx:
            b.start()
        self.assertIn("cluster.seed_nodes", str(ctx.exception))
        b.child = None
        b.configure(extra='[cluster]\nenabled=true\n')
        with self.assertRaises(AssertionError) as ctx:
            b.start()
        self.assertIn("cluster.enabled", str(ctx.exception))
        b.child = None


    # // AZ-01 inventory: current behaviour; owner MV-14
    def test_deprecated_edge_settings_warn_without_starting_edge_socket(self):
        b = self.broker
        edge_port = port()
        b.configure()
        path = b.cfg / "indra.toml"
        config = path.read_text().replace('id="az-01-entry"', 'id="az-01-entry"\nbrokerlink_bind="127.0.0.1:' + str(edge_port) + '"')
        config = config.replace('[listeners.tcp]\n', '[listeners.tcp]\nnative=true\n')
        config = config.replace('[listeners.ws]\n', '[listeners.ws]\nnative=true\n')
        config = config.replace('[listeners.tls]\n', '[listeners.tls]\nnative=true\n')
        config = config.replace('[listeners.wss]\n', '[listeners.wss]\nnative=true\n')
        path.write_text(config.replace('level="error"', 'level="warn"'))
        b.start()
        with self.assertRaises(OSError):
            socket.create_connection(("127.0.0.1", edge_port), timeout=1)
        _, ack = b.connect("deprecated-native")
        self.assertEqual(ack, "0000")
        b.stop()
        log = b"\n".join(b.last_output).decode(errors="replace")
        # The warning is the documented effect during the deprecation window.
        self.assertIn("deprecated", log.lower())
        self.assertIn("brokerlink_bind", log)
        self.assertIn("native", log)

    # // AZ-01 inventory: current behaviour; owner MV-10
    def test_legacy_licence_file_is_not_read_at_startup(self):
        b = self.broker
        invalid = b.base / "invalid-licence"
        invalid.write_text("invalid licence document")
        legacy = '[licence]\nlicense_file="' + str(invalid) + '"\nexpiry_warn_days=9\n'
        b.configure(extra=legacy)
        # An explicit path is refused at load: the broker never reads it.
        # The error names the file and the setting.
        try:
            b.start()
        except AssertionError as failure:
            self.assertIn("licence.license_file", str(failure))
        else:
            self.fail("boot with an explicit licence file must fail")
        b.child = None
        warned = '[licence]\nexpiry_warn_days=9\n'
        b.configure(extra=warned)
        b.start()
        _, ack = b.connect("licence-file")
        self.assertEqual(ack, "0000")
        status, loaded = b.http("GET", "/api/v1/config/explain?key=licence.license_file")
        self.assertEqual(status, 200)
        self.assertEqual(loaded["value"], json.dumps("/etc/indramqtt/license.key"))
        status, licence = b.http("GET", "/api/v1/licence/status")
        self.assertEqual(status, 200)
        self.assertEqual(licence["expiry_warn_days"], 9)
        identity = licence["identity"]
        b.stop()
        b.start()
        _, ack = b.connect("licence-file-restart")
        self.assertEqual(ack, "0000")
        self.assertEqual(invalid.read_text(), "invalid licence document")
        self.assertEqual(b.http("GET", "/api/v1/licence/status")[1]["expiry_warn_days"], 9)
        b.stop()

        def command(extra, success, message=None):
            b.configure(extra="[licence]\n" + extra)
            env = {key: value for key, value in os.environ.items() if not key.startswith("INDRA_")}
            env["INDRA_API_KEYS"] = b.key
            result = subprocess.run([str(BIN), "--config-dir", str(b.cfg)], cwd=ROOT,
                                    env=env, capture_output=True, timeout=15)
            self.assertEqual(result.returncode == 0, success, "licence file command status")
            if message:
                self.assertTrue(message.encode() in result.stderr, "missing native command diagnostic")

        request_file = b.base / "licence-request.json"
        command("request_out=" + json.dumps(str(request_file)) + "\n", True)
        request = json.loads(request_file.read_text())
        self.assertEqual(request["installation_identity"], identity)
        saved = request_file.read_bytes()
        command("request_out=" + json.dumps(str(b.base / "missing" / "request.json")) + "\n",
                False, "cannot write licence request")
        self.assertTrue(request_file.read_bytes() == saved)
        command("trusted_keys_path=" + json.dumps(str(invalid)) + "\n",
                False, "cannot load trusted licence keys")
        command("install_file=" + json.dumps(str(invalid)) + "\n",
                False, "licence install refused")
        b.configure(extra=warned)
        b.start()
        status, restored = b.http("GET", "/api/v1/licence/status")
        self.assertEqual(status, 200)
        self.assertEqual(restored["identity"], identity)
        self.assertEqual(restored["expiry_warn_days"], 9)
        _, ack = b.connect("licence-after-refused-command")
        self.assertEqual(ack, "0000")


    def test_certificate_upload_is_an_artifact_and_refused_update_keeps_bytes(self):
        b = self.boot()
        fixture = ROOT / "crates/broker-node/tests/k1_05"
        files = {"key": (fixture / "server-key.pem").read_text(),
                 "chain": (fixture / "server-cert.pem").read_text(),
                 "ca": (fixture / "ca-cert.pem").read_text(),
                 "key_password": secrets.token_hex(20)}
        path = "/api/v5/certs/global/name/inventory"
        self.assertEqual(b.http("POST", path, files)[0], 204)
        status, stored = b.http("GET", path)
        self.assertEqual(status, 200)
        self.assertEqual(set(stored), set(files))

        def check_files():
            self.assertEqual(b.http("GET", path), (200, stored))
            for key, value in files.items():
                target = Path(stored[key]["path"])
                self.assertTrue(target.is_relative_to(b.data / "certs/global/inventory"))
                # Digests keep private material out of assertion failures.
                self.assertEqual(hashlib.sha256(target.read_bytes()).digest(),
                                 hashlib.sha256(value.encode()).digest())
                expected_mode = 0o600 if key in ["key", "key_password"] else 0o644
                self.assertEqual(target.stat().st_mode & 0o777, expected_mode)

        check_files()
        status, _ = b.http("POST", path, {"key": "invalid PEM", "key_password": secrets.token_hex(20)})
        self.assertEqual(status, 400)
        check_files()
        # An account key is a separate uploaded artifact. The reference
        # upload operation promises file storage, not automatic activation.
        account = "/api/v5/certs/global/name/inventoryaccount"
        self.assertEqual(b.http("POST", account, {"acc_key": files["key"]})[0], 204)
        status, account_row = b.http("GET", account)
        self.assertEqual(status, 200)
        self.assertEqual(set(account_row), {"acc_key"})
        account_file = Path(account_row["acc_key"]["path"])
        self.assertEqual(hashlib.sha256(account_file.read_bytes()).digest(),
                         hashlib.sha256(files["key"].encode()).digest())
        self.assertEqual(account_file.stat().st_mode & 0o777, 0o600)
        b.stop()
        b.start()
        check_files()
        self.assertEqual(b.http("GET", account), (200, account_row))


    def test_trace_fields_select_format_time_window_and_stop_real_capture(self):
        b = self.boot()
        now = int(time.time())
        definitions = [
            {"name": "formathex", "type": "topic", "topic": "inventory/trace-format/#",
             "payload_encode": "hex", "formatter": "json"},
            {"name": "formathidden", "type": "clientid", "clientid": "format-publisher",
             "payload_encode": "hidden", "formatter": "json"},
            {"name": "formattext", "type": "ip_address", "ip_address": "127.0.0.1",
             "payload_encode": "text", "formatter": "text"},
            {"name": "formatfuture", "type": "topic", "topic": "inventory/trace-future",
             "payload_encode": "text", "formatter": "json", "start_at": now + 3600,
             "end_at": now + 7200}]
        for item in definitions:
            self.assertEqual(b.http("POST", "/api/v5/trace", item)[0], 200)
        pub, _ = b.connect("format-publisher")
        publish(b, pub, "inventory/trace-format/value", b"format-payload")

        def contents(name):
            status, value = b.http("GET", "/api/v5/trace/" + name + "/log")
            self.assertEqual(status, 200)
            return [row["content"] for row in value["data"]]

        expected = {"clientid": "format-publisher", "topic": "inventory/trace-format/value"}
        hex_lines = contents("formathex")
        self.assertEqual(len(hex_lines), 1)
        self.assertEqual(json.loads(hex_lines[0]), {**expected, "payload": b"format-payload".hex()})
        hidden = contents("formathidden")
        self.assertEqual(len(hidden), 1)
        self.assertEqual(json.loads(hidden[0]), expected)
        self.assertEqual(contents("formattext"), [
            "publish topic=inventory/trace-format/value clientid=format-publisher payload=format-payload"])
        publish(b, pub, "inventory/trace-future", b"future-payload")
        self.assertEqual(contents("formatfuture"), [])
        self.assertEqual(b.http("PUT", "/api/v5/trace/formathex/stop")[0], 200)
        publish(b, pub, "inventory/trace-format/value", b"stopped-payload")
        self.assertEqual(contents("formathex"), hex_lines)
        before = {item["name"]: contents(item["name"]) for item in definitions}
        self.assertEqual(b.http("PUT", "/api/v5/tracing", {"enable": False}), (200, {"enable": False}))
        publish(b, pub, "inventory/trace-format/value", b"disabled-payload")
        self.assertEqual({name: contents(name) for name in before}, before)
        self.assertEqual(b.http("PUT", "/api/v5/tracing", {"enable": "yes"})[0], 400)
        self.assertEqual(b.http("GET", "/api/v5/tracing"), (200, {"enable": False}))
        self.assertEqual(b.http("PUT", "/api/v5/tracing", {"enable": True})[0], 200)
        end = int(time.time()) + 3
        expiring = {"name": "formatexpiring", "type": "topic", "topic": "inventory/trace-expire",
                    "payload_encode": "text", "formatter": "json", "start_at": end - 60, "end_at": end}
        self.assertEqual(b.http("POST", "/api/v5/trace", expiring)[0], 200)
        publish(b, pub, "inventory/trace-expire", b"before-expiry")
        kept = contents("formatexpiring")
        self.assertEqual(len(kept), 1)
        self.assertEqual(json.loads(kept[0])["payload"], "before-expiry")
        while time.time() < end + 0.05:
            time.sleep(0.02)
        publish(b, pub, "inventory/trace-expire", b"after-expiry")
        self.assertEqual(contents("formatexpiring"), kept)
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", "/api/v5/trace"), (200, []))
        self.assertEqual(b.http("GET", "/api/v5/tracing"), (200, {"enable": False}))


    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_global_window_one_sends_two_unacked_packets_and_queue_cap_persists(self):
        b = self.boot()
        path = "/api/v5/configs/global_zone"
        chosen = {"mqtt": {"max_inflight": 1, "max_mqueue_len": 1, "max_topic_alias": 2}}
        status, stored = b.http("PUT", path, chosen)
        self.assertEqual(status, 200)
        self.assertEqual(b.http("PUT", path, {"mqtt": {"max_inflight": 0}})[0], 400)
        self.assertEqual(b.http("GET", path), (200, stored))
        pub, _ = b.connect("bounds-publisher")

        def publish_one(topic, payload, number):
            pub.sendall(frame(0x32, word(topic) + struct.pack("!H", number) + payload))
            self.assertEqual(packet(pub), (0x40, struct.pack("!H", number)))

        def downlink(sock, topic, payload):
            head, body = packet(sock)
            self.assertEqual(head, 0x32)
            prefix = word(topic)
            self.assertEqual(body[:len(prefix)], prefix)
            self.assertEqual(body[len(prefix) + 2:], payload)
            return body[len(prefix):len(prefix) + 2]

        sub, _ = b.connect("bounds-reader")
        self.assertEqual(b.subscribe(sub, "inventory/bounds", qos=1), "01")
        publish_one("inventory/bounds", b"first-live", 1)
        publish_one("inventory/bounds", b"second-live", 2)
        first = downlink(sub, "inventory/bounds", b"first-live")
        second = downlink(sub, "inventory/bounds", b"second-live")
        self.assertNotEqual(first, second)
        status, details = b.http("GET", "/api/v5/clients/bounds-reader")
        self.assertEqual(status, 200)
        self.assertEqual(details["inflight_max"], 1)
        # Both are already on the wire before the first PUBACK; the
        # configured window bounds tracking, not actual outstanding sends.
        sub.sendall(frame(0x40, first))
        sub.sendall(frame(0x40, second))
        offline, ack = b.connect("bounds-offline", clean=False)
        self.assertEqual(ack, "0000")
        self.assertEqual(b.subscribe(offline, "inventory/offline", qos=1), "01")
        offline.sendall(b"\xe0\x00")
        offline.shutdown(socket.SHUT_RDWR)
        offline.close()
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            status, state = b.http("GET", "/api/v5/clients/bounds-offline")
            if status == 200 and not state["connected"]:
                break
            time.sleep(0.02)
        self.assertEqual(status, 200)
        self.assertFalse(state["connected"])
        publish_one("inventory/offline", b"old-offline", 3)
        publish_one("inventory/offline", b"new-offline", 4)

        def queue_state():
            status, queued = b.http("GET", "/api/v5/clients/bounds-offline/mqueue_messages?payload=plain")
            self.assertEqual(status, 200)
            self.assertEqual(queued["meta"]["count"], 1)
            self.assertEqual(queued["data"][0]["payload"], "new-offline")

        queue_state()
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", path), (200, stored))
        queue_state()
        restored, ack = b.connect("bounds-offline", clean=False)
        self.assertEqual(ack, "0100")
        packet_id = downlink(restored, "inventory/offline", b"new-offline")
        restored.sendall(frame(0x40, packet_id))


    def user_quota_entry(self, tenant):
        extra = '' if not tenant else ('[[tenants.rules]]\nlistener="*"\nattribute="tenant"\nexpression="' + tenant + '"\n')
        b = self.boot(extra=extra)
        if tenant:
            self.assertEqual(b.http("POST", "/api/v5/mt/ns/" + tenant)[0], 204)
        path = "/api/v1/auth/users" + ("?tenant=" + tenant if tenant else "")
        quotas = {"max_connections": 3, "max_publish_rate": 1, "max_publish_burst": 1}
        body = {"username": "audituser", "password": b.password, "quotas": quotas}
        status, stored = b.http("POST", path, body)
        self.assertEqual(status, 201)
        self.assertEqual(stored["quotas"], quotas)
        acl_path = "/api/v1/auth/acls" + ("?tenant=" + tenant if tenant else "")
        self.assertEqual(b.http("POST", acl_path, {"client_pattern": "*", "action": "all",
                                                 "topic_pattern": "inventory/#", "allow": True})[0], 201)
        self.assertEqual(b.http("POST", path, {**body, "password": "", "quotas": {**quotas, "max_publish_rate": 2}})[0], 400)
        self.assertEqual(b.http("GET", path), (200, [{"username": "audituser", "quotas": quotas}]))

        def traffic(suffix):
            sub, ack = b.connect("quota-reader" + suffix, True)
            self.assertEqual(ack, "0000")
            first, ack = b.connect("quota-first" + suffix, True)
            self.assertEqual(ack, "0000")
            second, ack = b.connect("quota-second" + suffix, True)
            self.assertEqual(ack, "0000")
            _, ack = b.connect("quota-over-cap" + suffix, True)
            self.assertEqual(ack, "0003")
            self.assertEqual(b.subscribe(sub, "inventory/user-quota"), "00")
            start = time.monotonic()
            for publisher, payload in [(first, b"first-budget"), (second, b"second-budget")]:
                publish(b, publisher, "inventory/user-quota", payload)
                self.assertEqual(packet(sub), (0x30, word("inventory/user-quota") + payload))
            self.assertLess(time.monotonic() - start, 0.8)
            # Each connection has a separate bucket for the same username.
            # The second connection spends another token from the user quota.
            publish(b, first, "inventory/user-quota", b"over-first-budget")
            sub.settimeout(0.2)
            with self.assertRaises(TimeoutError):
                packet(sub)
            sub.settimeout(10)
            time.sleep(1.1)
            publish(b, first, "inventory/user-quota", b"refilled")
            self.assertEqual(packet(sub), (0x30, word("inventory/user-quota") + b"refilled"))

        traffic("")
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", path), (200, [{"username": "audituser", "quotas": quotas}]))
        traffic("-restart")

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_default_user_publish_quota_is_per_client_and_connection_cap_is_applied(self):
        self.user_quota_entry("")

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_tenant_user_publish_quota_is_per_client_and_connection_cap_is_applied(self):
        self.user_quota_entry("inventorytenant")


    # // AZ-01 inventory: current behaviour; owner MV-01
    def test_pg_connector_fields_reach_plaintext_auth_despite_tls_after_refusal_and_restart(self):
        peer = PgAuthenticationPeer()
        self.addCleanup(peer.close)
        b = self.boot()
        credential = secrets.token_hex(20)
        config = {"type": "pgsql", "name": "inventorypg", "server": "127.0.0.1:" + str(peer.port),
                  "database": "inventorydb", "username": "inventoryuser", "password": credential,
                  "application_name": "inventory-app", "pool_size": 3,
                  "disable_prepared_statements": True, "auto_reconnect": False,
                  "ssl": {"enable": True, "verify": "verify_peer",
                          "cacertfile": str(ROOT / "crates/broker-node/tests/k1_05/ca-cert.pem"),
                          "cacerts": False,
                          "certfile": str(ROOT / "crates/broker-node/tests/k1_05/client-a-cert.pem"),
                          "keyfile": str(ROOT / "crates/broker-node/tests/k1_05/client-a-key.pem"),
                          "ciphers": ["TLS_AES_256_GCM_SHA384"], "versions": ["tlsv1.3"],
                          "depth": 1, "hibernate_after": "7s", "log_level": "warning",
                          "middlebox_comp_mode": False, "partial_chain": False,
                          "password": secrets.token_hex(20), "reuse_sessions": False,
                          "secure_renegotiate": True, "server_name_indication": "localhost",
                          "verify_peer_ext_key_usage": "serverAuth",
                          "managed_certs": {"bundle_name": "inventory", "namespace": "global"}}}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.assertEqual(b.http("POST", "/api/v1/rules", {
            "name": "inventorypgrule", "topic_filter": "inventory/pg",
            "actions": [{"type": "forwardconnector", "connector_id": "inventorypg"}]})[0], 201)
        pub, _ = b.connect("pg-publisher")

        def attempt():
            status, current = b.http("GET", "/api/v5/connectors/inventorypg")
            self.assertEqual(status, 200)
            self.assert_fields_kept(current, {key: config[key] for key in [
                "server", "database", "username", "application_name", "pool_size",
                "disable_prepared_statements", "auto_reconnect", "ssl"]})
            publish(b, pub, "inventory/pg", b"database-entry")
            observed = peer.records.get(timeout=10)
            self.assertEqual(observed, {"user": config["username"], "database": config["database"],
                                        "application_name": None,
                                        "password_sha256": hashlib.sha256(credential.encode()).hexdigest()})
            self.assertTrue(peer.errors.empty(), "database test peer protocol error")

        attempt()
        self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorypg", {**config, "server": "127.0.0.1:notaport"})[0], 400)
        self.assertEqual(b.http("GET", "/api/v5/connectors/inventorypg")[1]["server"], config["server"])
        time.sleep(1.1)
        attempt()
        b.stop()
        b.start()
        pub, _ = b.connect("pg-publisher-restart")
        attempt()
        # The documented file form instead fails in the URL constructor.
        # Refusal preserves the stored row and the old live credential.
        password_file = b.base / "database-password"
        password_file.write_text(credential + "\n")
        reference = password_file.as_uri()
        before = b.http("GET", "/api/v5/connectors/inventorypg")
        status, error = b.http("PUT", "/api/v5/connectors/inventorypg", {**config, "password": reference})
        self.assertEqual(status, 400)
        self.assertEqual(error["code"], "BAD_REQUEST")
        self.assertTrue("postgres bad port" in error["message"])
        self.assertTrue(b.http("GET", "/api/v5/connectors/inventorypg") == before)
        time.sleep(1.1)
        attempt()
        b.stop()
        b.start()
        pub, _ = b.connect("pg-file-reference-restart")
        attempt()
        # The plaintext peer obtains the password despite ssl.enable=true.
        # Address/auth propagation is proved; SQL delivery is not qualified.
        # Pool and prepared-statement policy readers remain source-audit proof.


    # // AZ-01 inventory: current behaviour; owner MV-12a
    def test_transformation_fields_run_templates_but_disconnect_only_drops(self):
        b = self.boot()
        config = {"name": "inventoryfields", "topics": ["inventory/transform"],
                  "enable": True, "failure_action": "ignore", "operations": [
                      {"key": "payload", "value": "mapped-${payload}"},
                      {"key": "topic", "value": "inventory/transformed"}]}
        self.assertEqual(b.http("POST", "/api/v5/message_transformations", config)[0], 201)
        sub, _ = b.connect("transform-fields-reader")
        for topic in ["inventory/transform", "inventory/transformed", "inventory/unmatched"]:
            self.assertEqual(b.subscribe(sub, topic), "00")
        pub, _ = b.connect("transform-fields-publisher")
        publish(b, pub, "inventory/transform", b"input")
        self.assertEqual(packet(sub), (0x30, word("inventory/transformed") + b"mapped-input"))
        publish(b, pub, "inventory/unmatched", b"filter-miss")
        self.assertEqual(packet(sub), (0x30, word("inventory/unmatched") + b"filter-miss"))
        config["enable"] = False
        self.assertEqual(b.http("PUT", "/api/v5/message_transformations", [config])[0], 200)
        publish(b, pub, "inventory/transform", b"disabled")
        self.assertEqual(packet(sub), (0x30, word("inventory/transform") + b"disabled"))
        status, _ = b.http("PUT", "/api/v5/message_transformations",
                           [{**config, "operations": [{"key": "invalid", "value": "x"}]}])
        self.assertEqual(status, 400)
        path = "/api/v5/message_transformations/transformation/inventoryfields"
        self.assert_fields_kept(b.http("GET", path)[1], config)
        config["enable"] = True
        config["operations"] = [{"key": "topic", "value": "#"}]
        self.assertEqual(b.http("PUT", "/api/v5/message_transformations", [config])[0], 200)
        publish(b, pub, "inventory/transform", b"ignored-bad-topic")
        self.assertEqual(packet(sub), (0x30, word("inventory/transform") + b"ignored-bad-topic"))
        for failure in ["drop", "disconnect"]:
            config["failure_action"] = failure
            self.assertEqual(b.http("PUT", "/api/v5/message_transformations", [config])[0], 200)
            # Even disconnect leaves the publisher alive to answer PING.
            publish(b, pub, "inventory/transform", b"shed")
            sub.settimeout(1.2)
            with self.assertRaises(TimeoutError):
                packet(sub)
            sub.settimeout(10)
        b.stop()
        b.start()
        self.assert_fields_kept(b.http("GET", path)[1], config)
        sub, _ = b.connect("transform-fields-reader-restart")
        self.assertEqual(b.subscribe(sub, "inventory/transform"), "00")
        pub, _ = b.connect("transform-fields-publisher-restart")
        publish(b, pub, "inventory/transform", b"still-connected")
        sub.settimeout(1.2)
        with self.assertRaises(TimeoutError):
            packet(sub)


    # // AZ-01 inventory: current behaviour; owner MV-12b
    def test_jq_settings_store_without_runtime_selection_and_namespace_flag_has_no_hook(self):
        # // AZ-01 inventory: current behaviour; owner MV-12a
        b = self.boot(extra='[[tenants.rules]]\nlistener="*"\nattribute="tenant"\nexpression="${username}"\n')
        self.assertEqual(b.http("POST", "/api/v5/mt/ns/audituser")[0], 204)
        self.assertEqual(b.http("POST", "/api/v1/auth/users",
                               {"username": "default", "password": b.password})[0], 201)
        self.assertEqual(b.http("POST", "/api/v1/auth/users?tenant=audituser",
                               {"username": "audituser", "password": b.password})[0], 201)
        for query in ["", "?tenant=audituser"]:
            self.assertEqual(b.http("POST", "/api/v1/auth/acls" + query,
                                   {"client_pattern": "*", "action": "all",
                                    "topic_pattern": "inventory/#", "allow": True})[0], 201)
        chosen = {"jq_function_default_timeout": "1ms", "jq_implementation_module": "jq_port",
                  "limit_selects_in_namespace": False}
        status, stored = b.http("PUT", "/api/v5/rule_engine", chosen)
        self.assertEqual(status, 200)
        self.assert_fields_kept(stored, chosen)
        rule = {"id": "inventoryjq", "sql": 'SELECT jq(\'.v\', payload) AS extracted FROM "inventory/jq"',
                "actions": [{"function": "republish", "args": {
                    "topic": "inventory/jq-out", "payload": "${extracted}",
                    "qos": 0, "retain": False, "user_properties": ""}}]}
        self.assertEqual(b.http("POST", "/api/v5/rules", rule)[0], 201)
        sub, ack = b.connect("jq-reader", True, username="default")
        self.assertEqual(ack, "0000")
        self.assertEqual(b.subscribe(sub, "inventory/jq-out"), "00")
        pub, ack = b.connect("jq-publisher", True, username="default")
        self.assertEqual(ack, "0000")
        publish(b, pub, "inventory/jq", b'{"v":29}')
        self.assertEqual(packet(sub), (0x30, word("inventory/jq-out") + b"[29]"))
        chosen.update(jq_function_default_timeout="2ms", jq_implementation_module="jq_nif")
        status, stored = b.http("PUT", "/api/v5/rule_engine", chosen)
        self.assertEqual(status, 200)
        self.assert_fields_kept(stored, chosen)
        publish(b, pub, "inventory/jq", b'{"v":31}')
        self.assertEqual(packet(sub), (0x30, word("inventory/jq-out") + b"[31]"))
        self.assertEqual(b.http("PUT", "/api/v5/rule_engine", {**chosen, "jq_function_default_timeout": "invalid"})[0], 400)
        self.assertEqual(b.http("GET", "/api/v5/rule_engine"), (200, stored))
        status, metrics = b.http("GET", "/api/v5/rules/inventoryjq/metrics")
        self.assertEqual(status, 200)
        matched = metrics["metrics"]["matched"]
        other, ack = b.connect("jq-tenant-publisher", True)
        self.assertEqual(ack, "0000")
        publish(b, other, "inventory/jq", b'{"v":37}')
        self.assertEqual(b.http("GET", "/api/v5/rules/inventoryjq/metrics")[1]["metrics"]["matched"], matched)
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", "/api/v5/rule_engine"), (200, stored))
        sub, _ = b.connect("jq-reader-restart", True, username="default")
        self.assertEqual(b.subscribe(sub, "inventory/jq-out"), "00")
        pub, _ = b.connect("jq-publisher-restart", True, username="default")
        publish(b, pub, "inventory/jq", b'{"v":41}')
        self.assertEqual(packet(sub), (0x30, word("inventory/jq-out") + b"[41]"))
        # Entry shows accepted knobs and actual evaluation. Static reader
        # audit proves no module/deadline hook; this is not a timeout stress test.

    # // AZ-01 inventory: current behaviour; owner MV-03a
    def test_stream_driver_accepts_token_and_admin_url_then_refuses_on_publish(self):
        peer = SilentPeer()
        self.addCleanup(peer.close)
        b = self.boot()
        config = {"type": "pulsar", "name": "inventorydriverstream",
                  "service_url": "pulsar://127.0.0.1:" + str(peer.port),
                  "tenant": "selected_tenant", "namespace": "selected_namespace", "topic": "selected_topic",
                  "token": secrets.token_hex(24), "partition_key_template": "selected_partition",
                  "properties": {"selected_property": "selected_value"}, "batch_size": 20,
                  "batch_bytes": 2097152, "linger_ms": 60000, "max_retries": 0,
                  "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorydriverstreaminitial", "inventory/driver/initial")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            for mode in ("token", "admin"):
                selected = dict(config)
                if mode == "admin":
                    selected.pop("token")
                    selected["service_url"] = "http://127.0.0.1:" + str(peer.port)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydriverstream", selected)[0], 200)
                status, stored = b.http("GET", "/api/v5/connectors/inventorydriverstream")
                self.assertEqual(status, 200)
                self.assert_fields_kept(stored, selected)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydriverstream", {**selected, "tenant": "bad/tenant"})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventorydriverstream") == (200, stored))
                rule_id = "inventorydriverstream" + mode + str(attempt)
                topic = "inventory/driver/" + mode + str(attempt)
                self.assertEqual(b.http("POST", "/api/v5/rules", {
                    "id": rule_id, "sql": 'SELECT * FROM "' + topic + '"',
                    "actions": ["pulsar:inventorydriverstream"]})[0], 201)
                pub, ack = b.connect("driver-stream-" + mode + str(attempt))
                self.assertEqual(ack, "0000")
                publish(b, pub, topic, b'{"v":191}')
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    status, answer = b.http("GET", "/api/v5/rules/" + rule_id + "/metrics")
                    self.assertEqual(status, 200)
                    metrics = answer["metrics"]
                    if metrics["actions.total"]:
                        break
                    time.sleep(0.02)
                self.assertEqual(metrics["matched"], 1)
                self.assertEqual(metrics["actions.total"], 1)
                self.assertEqual(metrics["actions.failed"], 1)
                self.assertEqual(metrics["actions.success"], 0)
        # Source proves these refusals precede client connection. This is not
        # successful wire proof for topic/property/partition configuration.

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_warehouse_sdk_applies_batch_and_flags_but_replaces_auth(self):
        receiver = HttpReceiver(b'{"kind":"bigquery#tableDataInsertAllResponse"}')
        self.addCleanup(receiver.close)
        b = self.boot()
        token = secrets.token_hex(24)
        config = {"type": "bigquery", "name": "inventorywarehouse", "project_id": "audit-project",
                  "dataset_id": "audit_dataset", "table": "selected_table", "template_suffix": "selected_suffix",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "auth": {"type": "access_token", "token": token}, "access_token": token,
                  "ignore_unknown_values": False, "skip_invalid_rows": True,
                  "batch_size": 2, "batch_bytes": 1048576, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_BIGQUERY_COMPILED")
        if expected not in ("0", "1"):
            raise AssertionError("wrapper must name the binary's compiled warehouse SDK feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 'bigquery'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventorywarehouse")[0], 404)
            return
        self.assertEqual(status, 201)
        self.create_forwarding_action_rule(b, config, "inventorywarehouserule", "inventory/warehouse")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorywarehouse")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorywarehouse", {**config, "project_id": ""})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorywarehouse") == (200, stored))
            pub, ack = b.connect("warehouse-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for value in (179, 181):
                publish(b, pub, "inventory/warehouse", json.dumps({"v": value}).encode())
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(urllib.parse.unquote(path), "/bigquery/v2/projects/audit-project/datasets/audit_dataset/tables/selected_table$selected_suffix/insertAll")
            self.assertTrue(headers.get("authorization", "") in ("", "Bearer", "Bearer "),
                            "unexpected authorization material in unauthenticated driver path")
            body = json.loads(raw)
            self.assertTrue(body.get("skipInvalidRows"))
            self.assertFalse(body.get("ignoreUnknownValues"))
            self.assertEqual([row["json"] for row in body["rows"]], [{"v": 179}, {"v": 181}])
            for row in body["rows"]:
                self.assertEqual(uuid.UUID(row["insertId"]).version, 4)
        # Explicit endpoint and static empty auth avoid credential discovery.

    # // AZ-01 inventory: current behaviour; owner MV-03a
    def test_queue_envelope_replaces_tag_keys_group_and_batch_controls(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        listener.settimeout(0.2)
        self.addCleanup(listener.close)
        captured = queue.Queue()
        stopping = threading.Event()
        self.addCleanup(stopping.set)

        def observe():
            while not stopping.is_set():
                try:
                    conn, _ = listener.accept()
                except socket.timeout:
                    continue
                except OSError:
                    return
                with conn:
                    conn.settimeout(5)
                    try:
                        first = conn.recv(1)
                        if not first:
                            continue  # Empty TCP reachability probe, not a frame.
                        head = first + exact(conn, 4)
                        if head[0] != 1:
                            raise AssertionError("unexpected envelope version")
                        length = struct.unpack("!I", head[1:])[0]
                        if length > 65536:
                            raise AssertionError("envelope exceeds observer bound")
                        captured.put(json.loads(exact(conn, length)))
                        ack = b'{"code":0}'
                        conn.sendall(struct.pack("!I", len(ack)) + ack)
                    except (OSError, AssertionError, ValueError) as error:
                        captured.put(type(error).__name__)

        worker = threading.Thread(target=observe, daemon=True)
        worker.start()
        self.addCleanup(lambda: worker.join(timeout=1))
        b = self.boot()
        access, secret = secrets.token_hex(12), secrets.token_hex(24)
        config = {"type": "rocketmq", "name": "inventoryqueue",
                  "endpoint": "127.0.0.1:" + str(listener.getsockname()[1]),
                  "topic": "selected_topic", "access_key": access, "secret_key": secret,
                  "tag_template": "selected-tag", "keys_template": "selected-key",
                  "message_group_template": "selected-group", "batch_size": 20,
                  "buffer_capacity": 1, "linger_ms": 60000, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventoryqueuerule", "inventory/queue")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventoryqueue")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryqueue", {**config, "topic": "bad topic"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryqueue") == (200, stored))
            pub, ack = b.connect("queue-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            payload = b'{"client_id":"payload-client","v":173}'
            publish(b, pub, "inventory/queue", payload)
            envelope = captured.get(timeout=10)
            self.assertTrue(isinstance(envelope, dict), "observer did not capture an envelope")
            self.assertEqual(envelope["topic"], "selected_topic")
            self.assertEqual(len(envelope["messages"]), 1)
            message = envelope["messages"][0]
            self.assertEqual(base64.b64decode(message["body_b64"], validate=True), payload)
            props = message["system_properties"]
            self.assertIsNone(props["tag"])
            self.assertEqual(props["keys"], ["payload-client"])
            self.assertIsNone(props["message_group"])
            body = json.dumps([message["body_b64"]], separators=(",", ":")).encode()
            signing = "\n".join([envelope["client_id"], envelope["datetime"], envelope["topic"], hashlib.md5(body).hexdigest()])
            expected = "MQ " + access + ":" + base64.b64encode(hmac.new(secret.encode(), signing.encode(), hashlib.sha1).digest()).decode()
            self.assertTrue(envelope["authorization"] == expected, "selected signing credentials differ")
        # This observes the implemented envelope, not external protocol compatibility.

    # // AZ-01 inventory: current behaviour; owner MV-03b
    def test_rsa_stream_applies_identity_key_and_bytes_but_accepts_loopback_lookalikes(self):
        receiver = HttpReceiver(b'{"entries":[{"partition":"0","offset":0}]}')
        self.addCleanup(receiver.close)
        b = self.boot()
        fixture = ROOT / "crates/broker-connectors-enterprise/tests/fixtures/gcp_iot_device_rs256_private.pem"
        fingerprint = secrets.token_hex(16)
        config = {"type": "oci_streaming", "name": "inventoryrsastream",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "stream_pool_id": "ocid1.streampool.oc1..audit_pool", "stream_id": "ocid1.stream.oc1..audit_stream",
                  "tenancy_ocid": "ocid1.tenancy.oc1..audit_tenant", "user_ocid": "ocid1.user.oc1..audit_user",
                  "fingerprint": ":".join(fingerprint[index:index + 2] for index in range(0, 32, 2)),
                  "private_key_pem": fixture.read_text(), "partition_key_template": "selected-key",
                  "batch_size": 20, "buffer_capacity": 1, "batch_bytes": 2097152, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventoryrsastreamrule", "inventory/rsastream")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            # These reserved domains are never contacted. Observe API acceptance,
            # then restore the explicit local receiver before publishing.
            for address in ("http://localhost.attacker.invalid", "http://127.0.0.1.attacker.invalid"):
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryrsastream", {
                    **config, "endpoint": address})[0], 200)
                self.assertEqual(b.http("GET", "/api/v5/connectors/inventoryrsastream")[1]["endpoint"], address)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryrsastream", config)[0], 200)
            status, stored = b.http("GET", "/api/v5/connectors/inventoryrsastream")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            for bad in ({"private_key_pem": "invalid key"}, {"stream_pool_id": "invalid"}):
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryrsastream", {**config, **bad})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryrsastream") == (200, stored))
            pub, ack = b.connect("rsa-stream-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            publish(b, pub, "inventory/rsastream", b'{"v":167}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/20180418/streams/ocid1.stream.oc1..audit_stream/messages")
            self.assertEqual(headers["x-content-sha256"], base64.b64encode(hashlib.sha256(raw).digest()).decode())
            self.assertEqual(int(headers["content-length"]), len(raw))
            self.assertEqual(headers["content-type"], "application/json")
            authorization = headers["authorization"]
            self.assertTrue(authorization.startswith("Signature "))
            fields = dict((key, value.strip('"')) for key, value in
                          (part.split("=", 1) for part in authorization[10:].split(",")))
            self.assertEqual(set(fields), {"version", "headers", "keyId", "algorithm", "signature"})
            self.assertEqual(fields["version"], "1")
            self.assertEqual(fields["algorithm"], "rsa-sha256")
            self.assertTrue(fields["keyId"] == "/".join([config["tenancy_ocid"], config["user_ocid"], config["fingerprint"]]),
                            "selected signing identity differs")
            self.assertEqual(fields["headers"].split(), ["(request-target)", "host", "date", "x-content-sha256", "content-type", "content-length"])
            self.assertGreaterEqual(len(base64.b64decode(fields["signature"], validate=True)), 256)
            self.assertEqual(json.loads(raw), {"messages": [{"key": base64.b64encode(b"selected-key").decode(),
                                                          "value": base64.b64encode(b'{"v":167}').decode()}]})
        # This entry checks signature shape and selected identity. Cryptographic
        # RSA verification remains covered by the existing known-answer unit.

    # // AZ-01 inventory: current behaviour; owner MV-02a
    def test_data_api_sdk_replaces_cluster_user_sql_and_session_settings(self):
        statement_id = str(uuid.uuid4())
        receiver = HttpReceiver(json.dumps({"Id": statement_id, "Status": "FINISHED", "HasResultSet": False}).encode())
        self.addCleanup(receiver.close)
        b = self.boot()
        access_key, secret_key = secrets.token_hex(12).upper(), secrets.token_hex(24)
        config = {"type": "redshift", "name": "inventorydataapi", "database": "audit_database", "table": "audit_table",
                  "workgroup_name": "audit_workgroup", "cluster_identifier": "requested_cluster", "db_user": "requested_user",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port), "region": "us-west-2",
                  "access_key_id": access_key, "secret_access_key": secret_key, "session_token": secrets.token_hex(24),
                  "sql_template": "INSERT INTO $table (payload) VALUES ($5)", "batch_size": 20, "batch_bytes": 2097152,
                  "linger_ms": 60000, "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorydataapirule", "inventory/dataapi")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
                self.assert_fields_kept(b.http("GET", "/api/v5/connectors/inventorydataapi")[1], config)
            pub, ack = b.connect("data-api-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for workgroup in ("audit_workgroup", None):
                if workgroup:
                    config["workgroup_name"] = workgroup
                else:
                    config.pop("workgroup_name", None)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydataapi", config)[0], 200)
                status, stored = b.http("GET", "/api/v5/connectors/inventorydataapi")
                self.assertEqual(status, 200)
                self.assert_fields_kept(stored, config)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydataapi", {
                    **config, "database": ""})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventorydataapi") == (200, stored))
                lower = int(time.time() * 1000) - 1000
                publish(b, pub, "inventory/dataapi", b'{"v":157}')
                for operation in ("BatchExecuteStatement", "DescribeStatement"):
                    path, raw = receiver.requests.get(timeout=10)
                    headers = receiver.request_headers.get(timeout=10)
                    self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
                    self.assertEqual(path, "/")
                    self.assertEqual(headers["x-amz-target"], "RedshiftData." + operation)
                    self.assertTrue("x-amz-security-token" not in headers, "unexpected request header")
                    self.assert_sigv4_request("POST", path, headers, raw, access_key, secret_key, "us-west-2", "redshift-data")
                    body = json.loads(raw)
                    if operation == "DescribeStatement":
                        self.assertTrue(body == {"Id": statement_id}, "unexpected status-poll request")
                        continue
                    token = body.pop("ClientToken", None)
                    self.assertTrue(token is None or (isinstance(token, str) and 0 < len(token) <= 64),
                                    "invalid protocol idempotency metadata")
                    self.assertEqual(set(body), {"Database", "WorkgroupName", "Sqls"})
                    self.assertEqual(body["Database"], "audit_database")
                    if workgroup:
                        self.assertEqual(body["WorkgroupName"], workgroup)
                    else:
                        self.assertTrue(body["WorkgroupName"] and body["WorkgroupName"] != config["cluster_identifier"])
                    self.assertEqual(len(body["Sqls"]), 1)
                    sql = body["Sqls"][0]
                    prefix = "INSERT INTO audit_table (time_ms, topic, client_id, qos, payload) VALUES ("
                    suffix = ", 'inventory/dataapi', '', 0, '{\"v\":157}')"
                    self.assertTrue(sql.startswith(prefix))
                    self.assertTrue(sql.endswith(suffix))
                    timestamp = sql[len(prefix):-len(suffix)]
                    self.assertTrue(timestamp.isdigit())
                    self.assertTrue(lower <= int(timestamp) <= int(time.time() * 1000) + 1000)

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_table_store_replaces_primary_attribute_and_batch_mappings(self):
        receiver = HttpReceiver(b'{"tables":[{"rows":[{"is_ok":true}]}]}')
        self.addCleanup(receiver.close)
        b = self.boot()
        access_key, secret_key = secrets.token_hex(12), secrets.token_hex(24)
        config = {"type": "tablestore", "name": "inventorytablestore", "instance_name": "audit_instance",
                  "table_name": "audit_table", "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "access_key_id": access_key, "access_key_secret": secret_key,
                  "primary_keys": [{"name": "requested_pk", "source": "${payload.requested}", "data_type": "integer"}],
                  "attribute_columns": [{"name": "requested_attr", "source": "${payload.requested}", "data_type": "integer"}],
                  "batch_size": 20, "buffer_capacity": 1, "linger_ms": 60000, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorytablestorerule", "inventory/tablestore")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorytablestore")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorytablestore", {
                **config, "endpoint": "ftp://127.0.0.1/invalid"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorytablestore") == (200, stored))
            pub, ack = b.connect("table-store-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = int(time.time() * 1000) - 1000
            publish(b, pub, "inventory/tablestore", b'{"client_id":"audit_device","temperature":42.5,"requested":163}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/api/BatchWriteRow")
            self.assertEqual(headers["x-ots-instancename"], "audit_instance")
            self.assertEqual(headers["x-ots-apiversion"], "2015-12-31")
            md5 = base64.b64encode(hashlib.md5(raw).digest()).decode()
            self.assertEqual(headers["x-ots-contentmd5"], md5)
            canonical = (path + "\nPOST\n" + md5 + "\nx-ots-apiversion:" + headers["x-ots-apiversion"]
                         + "\nx-ots-date:" + headers["x-ots-date"] + "\nx-ots-instancename:audit_instance\n")
            signature = base64.b64encode(hmac.new(secret_key.encode(), canonical.encode(), hashlib.sha1).digest()).decode()
            self.assertTrue(hmac.compare_digest(headers["authorization"], "OTS " + access_key + ":" + signature),
                            "selected credentials did not sign table-store request")
            body = json.loads(raw)
            self.assertEqual(set(body), {"tables"})
            self.assertEqual(len(body["tables"]), 1)
            table = body["tables"][0]
            self.assertEqual(set(table), {"table_name", "rows"})
            self.assertEqual(table["table_name"], "audit_table")
            self.assertEqual(len(table["rows"]), 1)
            row = table["rows"][0]
            self.assertEqual(len(row["attributes"]), 1)
            timestamp = row["attributes"][0].pop("timestamp_ms")
            self.assertTrue(lower <= timestamp <= int(time.time() * 1000) + 1000)
            self.assertEqual(row, {"operation": "put", "primary_keys": [{"name": "device_id", "type": "string", "value": "audit_device"}],
                "attributes": [{"name": "temperature", "type": "double", "value": 42.5}]})

    # // AZ-01 inventory: current behaviour; owner MV-04
    def test_removed_table_kind_still_writes_and_replaces_codec_partition_and_deadline(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        access_key, secret_key = secrets.token_hex(12).upper(), secrets.token_hex(24)
        config = {"type": "s3_tables", "name": "inventorytableobject",
                  "table_bucket_arn": "arn:aws:s3tables:us-west-2:000000000000:bucket/audit-bucket",
                  "namespace": "audit_namespace", "table_name": "audit_table", "region": "us-west-2",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "access_key_id": access_key, "secret_access_key": secret_key, "session_token": secrets.token_hex(24),
                  "target_format": "parquetplaceholder", "partition_spec": [{"source_name": "device_id", "transform": "identity"}],
                  "batch_size": 20, "buffer_capacity": 1, "linger_ms": 60000, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorytableobjectrule", "inventory/tableobject")
        payload = b'{"device_id":"requested-device","v":149}'
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorytableobject")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorytableobject", {
                **config, "table_bucket_arn": "invalid"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorytableobject") == (200, stored))
            pub, ack = b.connect("table-object-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = time.time() - 1
            publish(b, pub, "inventory/tableobject", payload)
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "PUT")
            parts = urllib.parse.urlsplit(path).path.split("/")
            self.assertEqual(len(parts), 3)
            self.assertEqual(parts[:2], ["", "data"])
            self.assertTrue(parts[2].endswith(".data.gz"))
            self.assertEqual(uuid.UUID(parts[2][:-8]).version, 4)
            self.assertEqual(headers["content-type"], "application/x-ndjson")
            self.assertEqual(headers["content-encoding"], "gzip")
            self.assertEqual(headers["x-amz-content-sha256"], hashlib.sha256(raw).hexdigest())
            self.assertTrue("x-amz-security-token" not in headers, "unexpected request header")
            self.assert_sigv4_request("PUT", path, headers, raw, access_key, secret_key, "us-west-2", "s3")
            decoded = gzip.decompress(raw)
            self.assertTrue(decoded.endswith(b"\n"))
            self.assertEqual(len(decoded.splitlines()), 1)
            document = json.loads(decoded)
            stamp = datetime.datetime.fromisoformat(document.pop("timestamp").replace("Z", "+00:00")).timestamp()
            self.assertTrue(lower <= stamp <= time.time() + 1)
            self.assertEqual(document, {"topic": "inventory/tableobject", "qos": 0, "payload": json.loads(payload)})
        self.assert_forward_waits_past_configured_deadline(b, config, "inventorytabledeadline", payload,
                                                        {"table_bucket_arn": "invalid"})

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_time_series_http_replaces_measure_dimension_time_and_does_not_apply_deadline(self):
        receiver = HttpReceiver(b'{"RecordsIngested":{"Total":1}}')
        self.addCleanup(receiver.close)
        b = self.boot()
        access_key, secret_key = secrets.token_hex(12).upper(), secrets.token_hex(24)
        config = {"type": "timestream", "name": "inventorytimeseries", "database_name": "audit_database",
                  "table_name": "audit_table", "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "region": "us-west-2", "access_key_id": access_key, "secret_access_key": secret_key,
                  "session_token": secrets.token_hex(24), "time_unit": "nanoseconds", "measure_name_template": "requested_measure",
                  "dimensions": {"requested_dimension": "${payload.site}"}, "multi_measure_mappings": {"requested": "BIGINT"},
                  "batch_size": 20, "batch_bytes": 2097152, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorytimeseriesrule", "inventory/timeseries")
        payload = b'{"client_id":"audit_device","site":"requested_site","temperature":42.5,"requested":151}'
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorytimeseries")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorytimeseries", {
                **config, "database_name": ""})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorytimeseries") == (200, stored))
            pub, ack = b.connect("time-series-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = int(time.time() * 1000) - 1000
            publish(b, pub, "inventory/timeseries", payload)
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/")
            self.assertEqual(headers["x-amz-target"], "Timestream_20181101.WriteRecords")
            self.assertTrue("x-amz-security-token" not in headers, "unexpected request header")
            self.assert_sigv4_request("POST", path, headers, raw, access_key, secret_key, "us-west-2", "timestream")
            body = json.loads(raw)
            self.assertEqual(set(body), {"DatabaseName", "TableName", "Records"})
            self.assertEqual(body["DatabaseName"], "audit_database")
            self.assertEqual(body["TableName"], "audit_table")
            self.assertEqual(len(body["Records"]), 1)
            record = body["Records"][0]
            timestamp = record.pop("Time")
            self.assertTrue(timestamp.isdigit())
            self.assertTrue(lower <= int(timestamp) <= int(time.time() * 1000) + 1000)
            self.assertEqual(record, {"Dimensions": [{"Name": "device", "Value": "audit_device"},
                {"Name": "topic", "Value": "inventory/timeseries"}], "MeasureName": "inventory/timeseries",
                "MeasureValueType": "MULTI", "MeasureValues": [{"Name": "temperature", "Value": "42.5", "Type": "DOUBLE"}],
                "TimeUnit": "MILLISECONDS"})
        self.assert_forward_waits_past_configured_deadline(b, config, "inventorytimedeadline", payload,
                                                        {"database_name": ""})

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_document_sdk_drops_payload_mapping_sort_key_ttl_and_typed_partition(self):
        receiver = HttpReceiver(b'{"UnprocessedItems":{}}')
        self.addCleanup(receiver.close)
        b = self.boot()
        access_key, secret_key = secrets.token_hex(12).upper(), secrets.token_hex(24)
        config = {"type": "dynamodb", "name": "inventorydocument", "table_name": "audit_table",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port), "region": "us-west-2",
                  "access_key_id": access_key, "secret_access_key": secret_key, "session_token": secrets.token_hex(24),
                  "partition_key": "selected_pk", "sort_key": {"name": "selected_sk", "template": "selected", "key_type": "S"},
                  "ttl_attribute": "expires", "ttl_secs": 60, "attributes_mapping": {"requested": "${payload.v}"},
                  "batch_size": 20, "batch_bytes": 2097152, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_DYNAMODB_COMPILED")
        if expected not in ("0", "1"):
            raise AssertionError("wrapper must name the binary's compiled document SDK feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 'dynamodb'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventorydocument")[0], 404)
            return
        self.assertEqual(status, 201)
        self.create_forwarding_action_rule(b, config, "inventorydocumentrule", "inventory/document")
        partitions = ["selected_pk", {"name": "requested_pk", "template": "${payload.v}", "key_type": "N"}]
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
                self.assert_fields_kept(b.http("GET", "/api/v5/connectors/inventorydocument")[1], config)
            pub, ack = b.connect("document-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for partition in partitions:
                config["partition_key"] = partition
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydocument", config)[0], 200)
                status, stored = b.http("GET", "/api/v5/connectors/inventorydocument")
                self.assertEqual(status, 200)
                self.assert_fields_kept(stored, config)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydocument", {
                    **config, "table_name": ""})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventorydocument") == (200, stored))
                lower = int(time.time() * 1000) - 1000
                publish(b, pub, "inventory/document", b'{"v":137}')
                path, raw = receiver.requests.get(timeout=10)
                headers = receiver.request_headers.get(timeout=10)
                self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
                self.assertEqual(path, "/")
                self.assertEqual(headers["x-amz-target"], "DynamoDB_20120810.BatchWriteItem")
                self.assertTrue("x-amz-security-token" not in headers, "unexpected request header")
                self.assert_sigv4_request("POST", path, headers, raw, access_key, secret_key, "us-west-2", "dynamodb")
                body = json.loads(raw)
                self.assertEqual(set(body), {"RequestItems"})
                self.assertEqual(set(body["RequestItems"]), {"audit_table"})
                writes = body["RequestItems"]["audit_table"]
                self.assertEqual(len(writes), 1)
                self.assertEqual(set(writes[0]), {"PutRequest"})
                self.assertEqual(set(writes[0]["PutRequest"]), {"Item"})
                item = writes[0]["PutRequest"]["Item"]
                field = partition if isinstance(partition, str) else "id"
                self.assertEqual(set(item), {field})
                self.assertEqual(set(item[field]), {"S"})
                self.assertTrue(item[field]["S"].isdigit())
                self.assertTrue(lower <= int(item[field]["S"]) <= int(time.time() * 1000) + 1000)

    # // AZ-01 inventory: current behaviour; owner MV-03b
    def test_stream_sdk_applies_signed_data_but_replaces_partition_hash_and_session(self):
        receiver = HttpReceiver(b'{"FailedRecordCount":0,"Records":[{"SequenceNumber":"1","ShardId":"audit-shard"}]}')
        self.addCleanup(receiver.close)
        b = self.boot()
        access_key, secret_key = secrets.token_hex(12).upper(), secrets.token_hex(24)
        config = {"type": "kinesis", "name": "inventorystream", "stream_name": "audit_stream",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port), "region": "us-west-2",
                  "access_key_id": access_key, "secret_access_key": secret_key, "session_token": secrets.token_hex(24),
                  "partition_key_template": "requested_partition", "explicit_hash_key": "123",
                  "batch_size": 20, "batch_bytes": 2097152, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_KINESIS_COMPILED")
        if expected not in ("0", "1"):
            raise AssertionError("wrapper must name the binary's compiled stream SDK feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 'kinesis'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventorystream")[0], 404)
            return
        self.assertEqual(status, 201)
        self.create_forwarding_action_rule(b, config, "inventorystreamrule", "inventory/stream")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorystream")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorystream", {
                **config, "stream_name": ""})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorystream") == (200, stored))
            pub, ack = b.connect("stream-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            publish(b, pub, "inventory/stream", b'{"v":139}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/")
            self.assertEqual(headers["x-amz-target"], "Kinesis_20131202.PutRecords")
            self.assertTrue("x-amz-security-token" not in headers, "unexpected request header")
            self.assert_sigv4_request("POST", path, headers, raw, access_key, secret_key, "us-west-2", "kinesis")
            body = json.loads(raw)
            self.assertEqual(body, {"StreamName": "audit_stream", "Records": [{
                "PartitionKey": "inventory/stream", "Data": base64.b64encode(b'{"v":139}').decode()}]})

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_blob_auth_variants_reach_wire_but_compression_and_batch_policy_are_replaced(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        key_bytes = secrets.token_bytes(32)
        key = base64.b64encode(key_bytes).decode()
        bearer = secrets.token_hex(24)
        sas = "sp=w&sig=" + secrets.token_hex(24)
        variants = [
            ("shared", {"auth": {"type": "sharedkey", "account_key": key}}),
            ("shared", {"account_key": key}), ("shared", {"accountKey": key}),
            ("shared", {"auth": {"account_key": key}}), ("shared", {"auth": {"accountKey": key}}),
            ("sas", {"auth": {"type": "sastoken", "sas_token": sas}}),
            ("sas", {"sas_token": sas}), ("sas", {"sas": sas}),
            ("sas", {"auth": {"sas_token": sas}}), ("sas", {"auth": {"sas": sas}}),
            ("bearer", {"auth": {"type": "bearertoken", "token": bearer}}),
            ("bearer", {"token": bearer}), ("bearer", {"bearer_token": bearer}),
            ("bearer", {"auth": {"token": bearer}}), ("bearer", {"auth": {"bearer_token": bearer}})]
        base = {"type": "azure_blob", "name": "inventoryblob", "account_name": "auditaccount",
                "container_name": "audit-container", "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                "blob_path_template": "selected.ndjson", "compression": "gzip", "max_records_per_blob": 20,
                "max_bytes_per_blob": 2097152, "flush_interval_secs": 60, "buffer_capacity": 1, "timeout_ms": 1200}
        config = {**base, **variants[0][1]}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_AZURE_BLOB_COMPILED")
        if expected not in ("0", "1"):
            raise AssertionError("wrapper must name the binary's compiled blob-store feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 'azure_blob'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventoryblob")[0], 404)
            return
        self.assertEqual(status, 201)
        self.create_forwarding_action_rule(b, config, "inventoryblobrule", "inventory/blob")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
                self.assert_fields_kept(b.http("GET", "/api/v5/connectors/inventoryblob")[1], config)
            pub, ack = b.connect("blob-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for mode, auth in variants:
                config = {**base, **auth}
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryblob", config)[0], 200)
                status, stored = b.http("GET", "/api/v5/connectors/inventoryblob")
                self.assertEqual(status, 200)
                self.assert_fields_kept(stored, config)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryblob", {
                    **config, "blob_path_template": "/bad"})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryblob") == (200, stored))
                lower = time.time() - 1
                publish(b, pub, "inventory/blob", b'{"v":131}')
                setup_path, setup_body = receiver.requests.get(timeout=10)
                setup_headers = receiver.request_headers.get(timeout=10)
                self.assertEqual(receiver.request_methods.get(timeout=10), "PUT")
                self.assertEqual(urllib.parse.urlsplit(setup_path).path, "/audit-container")
                self.assertEqual(setup_body, b"")
                setup_query = urllib.parse.parse_qs(urllib.parse.urlsplit(setup_path).query)
                self.assertEqual(setup_query["restype"], ["container"])
                path, raw = receiver.requests.get(timeout=10)
                headers = receiver.request_headers.get(timeout=10)
                self.assertEqual(receiver.request_methods.get(timeout=10), "PUT")
                self.assertEqual(urllib.parse.urlsplit(path).path, "/audit-container/selected.ndjson")
                self.assertEqual(headers["x-ms-blob-type"], "BlockBlob")
                self.assertEqual(headers["content-type"], "application/x-ndjson")
                self.assertTrue("content-encoding" not in headers, "unexpected request header")
                if mode == "sas":
                    self.assertTrue(urllib.parse.urlsplit(path).query == sas, "selected SAS differs")
                    self.assertTrue(setup_query.get("sig") == [sas.split("sig=", 1)[1]], "setup SAS differs")
                    self.assertTrue("authorization" not in headers, "unexpected request header")
                    self.assertTrue("authorization" not in setup_headers, "unexpected request header")
                elif mode == "bearer":
                    self.assertTrue(headers["authorization"] == "Bearer " + bearer, "selected bearer differs")
                    self.assertTrue(setup_headers["authorization"] == "Bearer " + bearer, "setup bearer differs")
                else:
                    canonical = ("PUT\n\n\n" + str(len(raw)) + "\n\n" + headers["content-type"]
                        + "\n\n\n\n\n\n\nx-ms-blob-type:BlockBlob\nx-ms-date:" + headers["x-ms-date"]
                        + "\nx-ms-version:" + headers["x-ms-version"] + "\n/auditaccount/audit-container/selected.ndjson")
                    signature = base64.b64encode(hmac.new(key_bytes, canonical.encode(), hashlib.sha256).digest()).decode()
                    self.assertTrue(headers["authorization"] == "SharedKey auditaccount:" + signature,
                                    "selected account key did not sign the blob")
                self.assertTrue(raw.endswith(b"\n"))
                self.assertEqual(len(raw.splitlines()), 1)
                document = json.loads(raw)
                stamp = datetime.datetime.fromisoformat(document.pop("timestamp").replace("Z", "+00:00")).timestamp()
                self.assertTrue(lower <= stamp <= time.time() + 1)
                self.assertEqual(document, {"topic": "inventory/blob", "qos": 0, "payload": {"v": 131}})

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_object_store_sdk_applies_signed_target_but_drops_compression_and_session_token(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        access_key, secret_key = secrets.token_hex(12).upper(), secrets.token_hex(24)
        config = {"type": "s3", "name": "inventoryobject",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "bucket": "audit-bucket", "region": "us-west-2", "access_key_id": access_key,
                  "secret_access_key": secret_key, "session_token": secrets.token_hex(24),
                  "key_template": "selected.ndjson", "compression": "gzip", "batch_size": 20,
                  "batch_bytes": 10485760, "batch_timeout_ms": 60000, "timeout_ms": 1200}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_S3_COMPILED")
        if expected not in ("0", "1"):
            raise AssertionError("wrapper must name the binary's compiled object-store feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 's3'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventoryobject")[0], 404)
            return
        self.assertEqual(status, 201)
        self.create_forwarding_action_rule(b, config, "inventoryobjectrule", "inventory/object")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventoryobject")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryobject", {
                **config, "bucket": "invalid bucket"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryobject") == (200, stored))
            pub, ack = b.connect("object-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = time.time() - 1
            publish(b, pub, "inventory/object", b'{"v":127}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "PUT")
            self.assertEqual(urllib.parse.urlsplit(path).path, "/audit-bucket/selected.ndjson")
            self.assertEqual(headers["content-type"], "application/x-ndjson")
            self.assertTrue("x-amz-security-token" not in headers, "unexpected request header")
            self.assert_sigv4_request("PUT", path, headers, raw, access_key, secret_key, "us-west-2", "s3")
            encodings = [value.strip() for value in headers.get("content-encoding", "").split(",") if value.strip()]
            self.assertTrue(encodings in ([], ["aws-chunked"]), "unexpected object encoding")
            if encodings:
                decoded, position = bytearray(), 0
                while True:
                    end = raw.index(b"\r\n", position)
                    length = int(raw[position:end].split(b";", 1)[0], 16)
                    position = end + 2
                    if not length:
                        break
                    self.assertLessEqual(len(decoded) + length, 1048576)
                    decoded.extend(raw[position:position + length])
                    position += length
                    self.assertEqual(raw[position:position + 2], b"\r\n")
                    position += 2
                raw = bytes(decoded)
            self.assertTrue(raw.endswith(b"\n"))
            self.assertEqual(len(raw.splitlines()), 1)
            document = json.loads(raw)
            stamp = datetime.datetime.fromisoformat(document.pop("timestamp").replace("Z", "+00:00")).timestamp()
            self.assertTrue(lower <= stamp <= time.time() + 1)
            self.assertEqual(document, {"topic": "inventory/object", "qos": 0, "payload": {"v": 127}})

    # // AZ-01 inventory: current behaviour; owner MV-02c
    def test_search_connector_drops_document_id_nested_auth_and_batch_controls(self):
        receiver = HttpReceiver(b'{"errors":false,"items":[{"index":{"status":201}}]}')
        self.addCleanup(receiver.close)
        b = self.boot()
        password = secrets.token_hex(24)
        config = {"type": "elasticsearch", "name": "inventorysearch",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "index_template": "audit_index", "username": "audit_user", "password": password,
                  "auth": {"type": "apikey", "key": secrets.token_hex(24)},
                  "doc_id_template": "requested_id", "batch_size": 20, "batch_timeout_ms": 60000,
                  "max_retries": 0, "request_timeout_ms": 1200}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_ELASTICSEARCH_COMPILED")
        if expected not in ("0", "1"):
            raise AssertionError("wrapper must name the binary's compiled search-store feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 'elasticsearch'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventorysearch")[0], 404)
            return
        self.assertEqual(status, 201)
        self.create_forwarding_action_rule(b, config, "inventorysearchrule", "inventory/search")
        for attempt in range(2):
            if attempt:
                config.pop("username")
                config.pop("password")
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorysearch", config)[0], 200)
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorysearch")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorysearch", {
                **config, "index_template": ""})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorysearch") == (200, stored))
            pub, ack = b.connect("search-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = time.time() - 1
            publish(b, pub, "inventory/search", b'{"v":113}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/_bulk")
            if attempt:
                self.assertTrue("authorization" not in headers, "unexpected request header")
            else:
                credential = "Basic " + base64.b64encode(("audit_user:" + password).encode()).decode()
                self.assertTrue(headers["authorization"] == credential, "selected credentials differ")
            self.assertTrue(raw.endswith(b"\n"))
            lines = raw.splitlines()
            self.assertEqual(len(lines), 2)
            self.assertEqual(json.loads(lines[0]), {"index": {"_index": "audit_index"}})
            document = json.loads(lines[1])
            stamp = datetime.datetime.fromisoformat(document.pop("timestamp").replace("Z", "+00:00")).timestamp()
            self.assertTrue(lower <= stamp <= time.time() + 1)
            self.assertEqual(document, {"topic": "inventory/search", "qos": 0, "payload": {"v": 113}})

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_rest_sql_connector_replaces_selected_tag_metric_and_batch_templates(self):
        receiver = HttpReceiver(b'{"code":0,"rows":1}')
        self.addCleanup(receiver.close)
        b = self.boot()
        password = secrets.token_hex(24)
        config = {"type": "tdengine", "name": "inventoryrestsql",
                  "server": "http://127.0.0.1:" + str(receiver.server.server_port) + "/rest/sql",
                  "database": "audit_database", "stable_name": "audit_stable", "subtable_template": "audit_subtable",
                  "username": "audit_user", "password": password,
                  "tags_template": {"requested_tag": "requested"},
                  "metrics_template": {"requested_metric": "${payload.requested}"},
                  "batch_size": 20, "batch_bytes": 2097152, "linger_ms": 60000, "buffer_capacity": 1,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventoryrestsqlrule", "inventory/restsql")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventoryrestsql")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            for bad in ({"stable_name": "invalid stable"}, {"server": "http://127.0.0.1:notaport/rest/sql"}):
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryrestsql", {**config, **bad})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryrestsql") == (200, stored))
            pub, ack = b.connect("rest-sql-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = int(time.time() * 1000) - 1000
            publish(b, pub, "inventory/restsql", b'{"temperature":109,"requested":99}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/rest/sql/audit_database")
            expected_auth = "Basic " + base64.b64encode(("audit_user:" + password).encode()).decode()
            self.assertTrue(headers["authorization"] == expected_auth, "selected credentials differ")
            statement = raw.decode()
            prefix = "INSERT INTO audit_subtable USING audit_stable TAGS ('room1') VALUES ("
            suffix = ", 109);"
            self.assertTrue(statement.startswith(prefix))
            self.assertTrue(statement.endswith(suffix))
            timestamp = statement[len(prefix):-len(suffix)]
            self.assertTrue(timestamp.isdigit())
            self.assertTrue(lower <= int(timestamp) <= int(time.time() * 1000) + 1000)

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_stream_load_downgrades_https_and_replaces_load_headers(self):
        receiver = HttpReceiver(b'{"Status":"Success","NumberLoadedRows":1}')
        self.addCleanup(receiver.close)
        b = self.boot()
        password = secrets.token_hex(24)
        config = {"type": "doris", "name": "inventoryload",
                  "server": "https://127.0.0.1:" + str(receiver.server.server_port),
                  "http_port": receiver.server.server_port, "database": "audit_database", "table": "audit_rows",
                  "username": "audit_user", "password": password, "format": "csv",
                  "jsonpaths": '["$.v"]', "strip_outer_array": False, "max_filter_ratio": 0.5,
                  "batch_size": 20, "batch_bytes": 1048576, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventoryloadrule", "inventory/load")
        for attempt in range(2):
            if attempt:
                config["http_port"] = receiver.server.server_port + 65536
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryload", config)[0], 200)
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventoryload")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryload", {
                **config, "database": "invalid database"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryload") == (200, stored))
            pub, ack = b.connect("load-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = int(time.time() * 1000) - 1000
            publish(b, pub, "inventory/load", b'{"v":107}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "PUT")
            self.assertEqual(path, "/api/audit_database/audit_rows/_stream_load")
            expected_auth = "Basic " + base64.b64encode(("audit_user:" + password).encode()).decode()
            self.assertTrue(headers["authorization"] == expected_auth, "selected credentials differ")
            self.assertEqual(headers["format"], "json")
            self.assertEqual(headers["strip_outer_array"], "true")
            self.assertEqual(headers["max_filter_ratio"], "0")
            self.assertTrue("jsonpaths" not in headers, "unexpected request header")
            self.assertTrue(headers["label"].startswith("indra-"))
            rows = json.loads(raw)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(set(row), {"v", "topic", "qos", "timestamp"})
            self.assertEqual(row["v"], 107)
            self.assertEqual(row["topic"], "inventory/load")
            self.assertEqual(row["qos"], 0)
            self.assertTrue(lower <= row["timestamp"] <= int(time.time() * 1000) + 1000)

    # // AZ-01 inventory: current behaviour; owner MV-02a
    def test_sql_url_keeps_encoded_password_drops_application_option_and_batch_bound(self):
        fixture = ROOT / "crates/broker-node/tests/k1_05"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(fixture / "server-cert.pem", fixture / "server-key.pem")
        peer = PgAuthenticationPeer(context)
        self.addCleanup(peer.close)
        b = self.broker
        b.configure()
        b.extra_env["SSL_CERT_FILE"] = str(fixture / "ca-cert.pem")
        b.start()
        encoded_password = secrets.token_hex(24) + "%2B"
        connection = ("postgresql://audit_user:" + encoded_password + "@127.0.0.1:" + str(peer.port)
                      + "/audit_database?application_name=audit_application")
        config = {"type": "cockroachdb", "name": "inventorysqlurl", "connection_string": connection,
                  "table": "audit_rows", "upsert_conflict_columns": ["v"], "batch_size": 20,
                  "max_retry_attempts": 1, "buffer_capacity": 1, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorysqlurlrule", "inventory/sqlurl")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorysqlurl")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorysqlurl", {
                **config, "table": ""})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorysqlurl") == (200, stored))
            pub, ack = b.connect("sql-url-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            publish(b, pub, "inventory/sqlurl", b'{"v":103}')
            record = peer.records.get(timeout=10)
            self.assert_fields_kept(record, {"user": "audit_user", "database": "audit_database",
                "application_name": None, "password_sha256": hashlib.sha256(encoded_password.encode()).hexdigest()})
            self.assertGreaterEqual(peer.tls_sessions, attempt + 1)
            self.assertTrue(peer.errors.empty(), "TLS database observer protocol error")
        # Authentication is refused by the observer; SQL and retry/backlog
        # saturation are not qualified by this entry case.

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_sql_http_connector_replaces_format_precision_batch_and_nested_auth(self):
        receiver = HttpReceiver(b'{"code":0}')
        self.addCleanup(receiver.close)
        b = self.boot()
        password = secrets.token_hex(24)
        config = {"type": "greptimedb", "name": "inventorysqlhttp",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "database": "audit_database", "table": "audit_rows",
                  "username": "audit_user", "password": password,
                  "auth": {"username": "nested_user", "password": secrets.token_hex(24)},
                  "format": "influx_line_protocol", "timestamp_precision": "second",
                  "batch_size": 20, "buffer_capacity": 1, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorysqlhttprule", "inventory/sqlhttp")
        for attempt in range(2):
            if attempt:
                config.pop("username")
                config.pop("password")
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorysqlhttp", config)[0], 200)
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorysqlhttp")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorysqlhttp", {
                **config, "table": ""})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorysqlhttp") == (200, stored))
            pub, ack = b.connect("sql-http-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = int(time.time() * 1000) - 1000
            publish(b, pub, "inventory/sqlhttp", b'{"v":99}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/v1/sql?db=audit_database")
            if attempt:
                self.assertTrue("authorization" not in headers, "unexpected request header")
            else:
                expected = "Basic " + base64.b64encode(("audit_user:" + password).encode()).decode()
                self.assertTrue(headers["authorization"] == expected, "selected credentials differ")
            self.assertTrue(headers["content-type"].startswith("application/x-www-form-urlencoded"))
            fields = urllib.parse.parse_qs(raw.decode(), strict_parsing=True)
            self.assertEqual(set(fields), {"sql"})
            self.assertEqual(len(fields["sql"]), 1)
            statement = fields["sql"][0]
            prefix, suffix = "INSERT INTO audit_rows (ts, v) VALUES (", ", 99)"
            self.assertTrue(statement.startswith(prefix))
            self.assertTrue(statement.endswith(suffix))
            timestamp = statement[len(prefix):-len(suffix)]
            self.assertTrue(timestamp.isdigit())
            self.assertTrue(lower <= int(timestamp) <= int(time.time() * 1000) + 1000)

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_tablet_connector_replaces_alignment_measurements_types_and_batching(self):
        receiver = HttpReceiver(b'{"code":200}')
        self.addCleanup(receiver.close)
        b = self.boot()
        password = secrets.token_hex(24)
        config = {"type": "iotdb", "name": "inventorytablet",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port) + "/rest/v2",
                  "device_path_template": "root.audit.unit", "username": "audit_user", "password": password,
                  "is_aligned": True, "measurements": ["requested"], "data_types": ["INT64"],
                  "batch_size": 20, "batch_bytes": 1048576, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorytabletrule", "inventory/tablet")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorytablet")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorytablet", {
                **config, "device_path_template": "invalid.root"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorytablet") == (200, stored))
            pub, ack = b.connect("tablet-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = int(time.time() * 1000) - 1000
            for _ in range(20):
                publish(b, pub, "inventory/tablet", b'{"temperature":101.5,"requested":9}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/rest/v2/insertTablet")
            expected_auth = "Basic " + base64.b64encode(("audit_user:" + password).encode()).decode()
            self.assertTrue(headers["authorization"] == expected_auth, "selected credentials differ")
            body = json.loads(raw)
            times = body.pop("timestamps")
            self.assertEqual(len(times), 20)
            for stamp in times:
                self.assertTrue(lower <= stamp <= int(time.time() * 1000) + 1000)
            self.assertEqual(body, {"device": "root.audit.unit", "is_aligned": True,
                "measurements": ["requested"], "data_types": ["INT64"], "values": [[9] * 20]})

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_layer_connector_replaces_selected_columns_time_and_batching(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        token = secrets.token_hex(24)
        password = secrets.token_hex(24)
        config = {"type": "datalayers", "name": "inventorylayer",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "database": "audit_database", "table": "audit_rows", "auth_token": token,
                  "timestamp_field": "requested_time", "tag_columns": ["requested_tag"],
                  "field_columns": ["v"], "batch_size": 20, "buffer_capacity": 1, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorylayerrule", "inventory/layer")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            pub, ack = b.connect("layer-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for mode in ("bearer", "basic"):
                if mode == "bearer":
                    config.pop("username", None)
                    config.pop("password", None)
                    config["auth_token"] = token
                    expected_auth = "Bearer " + token
                else:
                    config.pop("auth_token", None)
                    config.update(username="audit_user", password=password)
                    expected_auth = "Basic " + base64.b64encode(("audit_user:" + password).encode()).decode()
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorylayer", config)[0], 200)
                status, stored = b.http("GET", "/api/v5/connectors/inventorylayer")
                self.assertEqual(status, 200)
                self.assert_fields_kept(stored, config)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorylayer", {
                    **config, "database": ""})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventorylayer") == (200, stored))
                payload = {"timestamp": 1700000000, "requested_time": 1800000000,
                           "sensor": "selected_sensor", "requested_tag": "ignored", "temperature": 42, "v": 97}
                publish(b, pub, "inventory/layer", json.dumps(payload).encode())
                path, raw = receiver.requests.get(timeout=10)
                headers = receiver.request_headers.get(timeout=10)
                self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
                self.assertEqual(path, "/write?db=audit_database&precision=us")
                self.assertTrue(headers["authorization"] == expected_auth, "selected credentials differ")
                self.assertEqual(raw, b'audit_rows,sensor=selected_sensor temperature=42i 1700000000000000')

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_metric_connector_forces_http_plain_body_and_single_row_batch(self):
        receiver = HttpReceiver(b'{"success":1,"failed":0,"errors":[]}')
        self.addCleanup(receiver.close)
        b = self.boot()
        config = {"type": "opentsdb", "name": "inventorymetric",
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "metric_template": "audit.metric", "value_field": "v",
                  "tag_mappings": {"site": "selected"}, "summary": True,
                  "protocol": "telnet", "compression": "gzip", "batch_size": 20,
                  "buffer_capacity": 1, "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorymetricrule", "inventory/metric")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            pub, ack = b.connect("metric-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for summary in (True, False):
                config["summary"] = summary
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorymetric", config)[0], 200)
                status, stored = b.http("GET", "/api/v5/connectors/inventorymetric")
                self.assertEqual(status, 200)
                self.assert_fields_kept(stored, config)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorymetric", {
                    **config, "metric_template": ""})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventorymetric") == (200, stored))
                lower = int(time.time()) - 1
                publish(b, pub, "inventory/metric", b'{"v":97}')
                path, raw = receiver.requests.get(timeout=10)
                headers = receiver.request_headers.get(timeout=10)
                self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
                self.assertEqual(path, "/api/put?summary" if summary else "/api/put")
                self.assertTrue("content-encoding" not in headers, "unexpected request header")
                points = json.loads(raw)
                self.assertEqual(len(points), 1)
                point = points[0]
                self.assertEqual(set(point), {"metric", "timestamp", "value", "tags"})
                self.assertEqual(point["metric"], "audit.metric")
                self.assertEqual(point["value"], 97)
                self.assertEqual(point["tags"], {"site": "selected"})
                self.assertTrue(lower <= point["timestamp"] <= int(time.time()) + 1)

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_streaming_store_drops_selected_channel_role_mapping_and_target(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        key = (ROOT / "crates/broker-connectors-enterprise/tests/fixtures/gcp_iot_device_rs256_private.pem").read_text()
        config = {"type": "snowflake", "name": "inventorystreaming",
                  "account": "audit_account", "user": "audit_user", "database": "audit_database",
                  "schema": "audit_schema", "table": "audit_table", "private_key_pem": key,
                  "endpoint": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "role": "audit_role", "channel": "audit_channel",
                  "column_mappings": {"requested_column": "${payload.v}"},
                  "batch_size": 20, "batch_bytes": 1048576, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9,
                  "timeout_ms": 1200}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorystreamingrule", "inventory/streaming")
        for attempt in range(2):
            if attempt:
                config.update(database="other_database", schema="other_schema", table="other_table")
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorystreaming", config)[0], 200)
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorystreaming")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorystreaming", {
                **config, "private_key_pem": "invalid key"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorystreaming") == (200, stored))
            pub, ack = b.connect("streaming-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            lower = int(time.time() * 1000) - 1000
            publish(b, pub, "inventory/streaming", b'{"v":93}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/v1/data/streaming/channels/INDRA_CHANNEL/rows")
            self.assertTrue("x-snowflake-role" not in headers, "unexpected request header")
            self.assertTrue(headers["authorization"].startswith("Bearer "))
            parts = headers["authorization"][7:].split(".")
            self.assertEqual(len(parts), 3)
            def decode_part(part):
                return json.loads(base64.urlsafe_b64decode(part + "=" * (-len(part) % 4)))
            self.assertEqual(decode_part(parts[0])["alg"], "RS256")
            claims = decode_part(parts[1])
            self.assertEqual(claims["iss"], "AUDIT_ACCOUNT.AUDIT_USER")
            self.assertEqual(claims["sub"], claims["iss"])
            self.assertEqual(claims["exp"] - claims["iat"], 3600)
            rows = json.loads(raw)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(set(row), {"V", "_MQTT_TOPIC", "_MQTT_TIMESTAMP"})
            self.assertEqual(row["V"], 93)
            self.assertEqual(row["_MQTT_TOPIC"], "inventory/streaming")
            self.assertTrue(lower <= row["_MQTT_TIMESTAMP"] <= int(time.time() * 1000) + 1000)

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_disk_connector_writes_outside_data_and_ignores_format_rotation(self):
        b = self.boot()
        outside = b.base / "outside-data"
        outside.mkdir()
        config = {"type": "disk_log", "name": "inventorydisk", "directory": str(outside),
                  "filename_prefix": "selected", "filename_extension": "entry",
                  "format": "raw", "max_file_size_bytes": 1, "max_file_age_secs": 1,
                  "compression": "gzip", "max_backup_files": 1, "max_retention_days": 1,
                  "sync_mode": "os_default", "timeout_ms": 1}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorydiskrule", "inventory/disk")
        active = outside / "selected.entry"
        self.assertFalse(active.resolve().is_relative_to(b.data.resolve()))
        expected = []
        for attempt in range(2):
            if attempt:
                link = b.data / "outside-link"
                link.symlink_to(outside, target_is_directory=True)
                config["directory"] = str(link)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydisk", config)[0], 200)
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorydisk")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydisk", {
                **config, "filename_prefix": "bad/name"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorydisk") == (200, stored))
            pub, ack = b.connect("disk-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for offset in range(2):
                value = attempt * 2 + offset
                lower = int(time.time() * 1000) - 1000
                publish(b, pub, "inventory/disk", json.dumps({"v": value}).encode())
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    lines = active.read_bytes().splitlines()
                    if len(lines) == len(expected) + 1:
                        break
                    time.sleep(0.02)
                self.assertEqual(len(lines), len(expected) + 1)
                row = json.loads(lines[-1])
                self.assertEqual(set(row), {"timestamp", "topic", "qos", "payload"})
                self.assertEqual(row["topic"], "inventory/disk")
                self.assertEqual(row["qos"], 0)
                self.assertEqual(row["payload"], {"v": value})
                self.assertTrue(lower <= row["timestamp"] <= int(time.time() * 1000) + 1000)
                expected.append(row)
                self.assertEqual([json.loads(line) for line in lines], expected)
                self.assertEqual(sorted(x.name for x in outside.iterdir()), ["selected.entry"])

    # // AZ-01 inventory: current behaviour; owner MV-02d
    def test_statement_connector_drops_warehouse_mapping_and_batch_controls(self):
        receiver = HttpReceiver(b'{"status":{"state":"SUCCEEDED"}}')
        self.addCleanup(receiver.close)
        b = self.boot()
        token = secrets.token_hex(24)
        config = {"type": "databricks", "name": "inventorystatement",
                  "host": "http://127.0.0.1:" + str(receiver.server.server_port),
                  "token": token, "catalog": "audit_catalog", "schema": "audit_schema",
                  "table": "audit_rows", "timeout_ms": 1200,
                  "http_path": "/sql/1.0/warehouses/auditwarehouse",
                  "partition_key_template": "audit_partition",
                  "column_mappings": {"requested_column": "${payload.v}"},
                  "batch_size": 20, "batch_bytes": 1048576, "linger_ms": 60000,
                  "max_retries": 0, "initial_backoff_ms": 7, "max_backoff_ms": 9}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorystatementrule", "inventory/statement")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            status, stored = b.http("GET", "/api/v5/connectors/inventorystatement")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, config)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorystatement", {
                **config, "catalog": "invalid catalog"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorystatement") == (200, stored))
            pub, ack = b.connect("statement-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            publish(b, pub, "inventory/statement", b'{"v":91}')
            path, raw = receiver.requests.get(timeout=10)
            headers = receiver.request_headers.get(timeout=10)
            self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
            self.assertEqual(path, "/api/2.0/sql/statements")
            self.assertTrue(headers["authorization"] == "Bearer " + token,
                            "selected credential did not reach the local observer")
            body = json.loads(raw)
            self.assertEqual(body, {"catalog": "audit_catalog", "schema": "audit_schema",
                "statement": "INSERT INTO audit_catalog.audit_schema.audit_rows (v) VALUES (?)",
                "parameters": [{"name": "1", "value": "91", "type": "BIGINT"}]})

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_alloydb_inline_credentials_reach_tls_but_file_password_is_literal(self):
        fixture = ROOT / "crates/broker-node/tests/k1_05"
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(fixture / "server-cert.pem", fixture / "server-key.pem")
        peer = PgAuthenticationPeer(context)
        self.addCleanup(peer.close)
        b = self.broker
        b.configure()
        # Trust is scoped to this child. The observer still requires a
        # verified TLS handshake; it never disables verification.
        b.extra_env["SSL_CERT_FILE"] = str(fixture / "ca-cert.pem")
        b.start()
        credential = secrets.token_hex(24)
        password_file = b.base / "alloydb-password"
        password_file.write_text(credential + "\n")
        config = {"type": "alloydb", "name": "inventoryalloy",
                  "server": "127.0.0.1:" + str(peer.port), "database": "inventorydb",
                  "table": "inventorytable", "username": "inventoryuser", "password": credential,
                  "timeout_ms": 1000}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventoryalloyrule", "inventory/alloy")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
                config["password"] = password_file.as_uri()
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryalloy", config)[0], 200)
            # The constructor currently accepts a malformed port and an
            # omitted port using its fixed fallback. Do not publish while
            # either unrequested target is selected.
            for address in ["127.0.0.1:notaport", "127.0.0.1"]:
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryalloy", {
                    **config, "server": address})[0], 200)
                self.assertEqual(b.http("GET", "/api/v5/connectors/inventoryalloy")[1]["server"], address)
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryalloy", config)[0], 200)
            status, kept = b.http("GET", "/api/v5/connectors/inventoryalloy")
            self.assertEqual(status, 200)
            self.assert_fields_kept(kept, {key: value for key, value in config.items() if key != "password"})
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryalloy", {
                **config, "table": ""})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryalloy") == (200, kept))
            pub, ack = b.connect("alloy-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            publish(b, pub, "inventory/alloy", b'{"v":89}')
            record = peer.records.get(timeout=10)
            self.assert_fields_kept(record, {"user": "inventoryuser", "database": "inventorydb",
                                           "password_sha256": hashlib.sha256(config["password"].encode()).hexdigest()})
            self.assertGreaterEqual(peer.tls_sessions, attempt + 1)
            self.assertTrue(peer.errors.empty(), "TLS database observer protocol error")
        # The peer refuses authentication after recording a digest. It
        # never executes a statement or qualifies the database service.

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_influx_connector_keeps_canonical_parameters_but_sends_default_v2_request(self):
        receiver = HttpReceiver()
        self.addCleanup(receiver.close)
        b = self.boot()
        variants = [
            {"influxdb_type": "influxdb_api_v2", "bucket": "requested-bucket", "org": "requested-org",
             "token": secrets.token_hex(24), "ping_with_auth": True},
            {"influxdb_type": "influxdb_api_v1", "database": "requested-db",
             "username": "requested-user", "password": secrets.token_hex(24), "ping_with_auth": True},
            {"influxdb_type": "influxdb_api_v3", "database": "requested-db3",
             "token": secrets.token_hex(24), "ping_with_auth": True}]
        config = {"type": "influxdb", "name": "inventoryinflux",
                  "server": "127.0.0.1:" + str(receiver.server.server_port),
                  "pool_size": 3, "max_inactive": "1ms", "parameters": variants[0]}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventoryinfluxrule", "inventory/influx")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            pub, ack = b.connect("influx-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            for parameters in variants:
                config["parameters"] = parameters
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryinflux", config)[0], 200)
                status, stored = b.http("GET", "/api/v5/connectors/inventoryinflux")
                self.assertEqual(status, 200)
                self.assert_fields_kept(stored, config)
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventoryinflux", {
                    **config, "bucket": ""})[0], 400)
                self.assertTrue(b.http("GET", "/api/v5/connectors/inventoryinflux") == (200, stored))
                payload = json.dumps({"entry": "influx-entry-" + str(attempt) + "-" + parameters["influxdb_type"]},
                                     separators=(",", ":")).encode()
                lower = int(time.time() * 1000) - 1000
                publish(b, pub, "inventory/influx", payload)
                path, body = receiver.requests.get(timeout=10)
                headers = receiver.request_headers.get(timeout=10)
                self.assertEqual(receiver.request_methods.get(timeout=10), "POST")
                parsed = urllib.parse.urlsplit(path)
                self.assertEqual(parsed.path, "/api/v2/write")
                query = urllib.parse.parse_qs(parsed.query)
                self.assertEqual(query["precision"], ["ms"])
                if "bucket" in parameters:
                    self.assertNotEqual(query["bucket"], [parameters["bucket"]])
                    self.assertNotEqual(query["org"], [parameters["org"]])
                else:
                    self.assertNotIn("db", query)
                self.assertTrue(headers["authorization"].startswith("Token "))
                if "token" in parameters:
                    self.assertTrue(headers["authorization"] != "Token " + parameters["token"],
                                    "requested token unexpectedly reached the header")
                escaped = payload.replace(b"\\", b"\\\\").replace(b'"', b'\\"')
                prefix = b'mqtt_events,topic=inventory/influx qos=0i,payload="' + escaped + b'" '
                self.assertTrue(body.startswith(prefix))
                self.assertTrue(body.endswith(b"\n"))
                stamp = body[len(prefix):-1]
                self.assertTrue(stamp.isdigit())
                self.assertGreaterEqual(int(stamp), lower)
                self.assertLessEqual(int(stamp), int(time.time() * 1000) + 1000)
        # HTTP receipt proves the broker's selected request fields. The
        # loopback peer does not qualify any database protocol version.

    # // AZ-01 inventory: current behaviour; owner MV-02b
    def test_clickhouse_request_timeout_is_stored_but_not_applied_to_driver(self):
        peer = SilentPeer()
        self.addCleanup(peer.close)
        b = self.boot()
        credential = secrets.token_hex(24)
        config = {"type": "clickhouse", "name": "inventorydeadline",
                  "url": "http://127.0.0.1:" + str(peer.port), "database": "inventorydb",
                  "table": "inventorytable", "username": "inventoryuser", "password": credential,
                  "request_timeout_ms": 1}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_CLICKHOUSE_COMPILED")
        if expected not in ["0", "1"]:
            raise AssertionError("wrapper must name the binary's compiled HTTP database feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 'clickhouse'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventorydeadline")[0], 404)
            return
        self.assertEqual(status, 201)
        rule_id = self.create_forwarding_action_rule(b, config, "inventorydeadlinerule", "inventory/deadline")
        metrics_path = "/api/v5/rules/" + rule_id + "/metrics"
        password_file = b.base / "http-database-password"
        password_file.write_text(credential + "\n")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
                config["password"] = password_file.as_uri()
                self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydeadline", config)[0], 200)
            status, stored = b.http("GET", "/api/v5/connectors/inventorydeadline")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, {key: value for key, value in config.items() if key != "password"})
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorydeadline", {
                **config, "table": "invalid table;"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorydeadline") == (200, stored))
            before = len(peer.clients)
            pub, ack = b.connect("deadline-publisher-" + str(attempt))
            self.assertEqual(ack, "0000")
            pub.sendall(frame(0x30, word("inventory/deadline") + b'{"v":83}'))
            deadline = time.monotonic() + 5
            request = b""
            buffers = {}
            while time.monotonic() < deadline and not request:
                for connection in list(peer.clients)[before:]:
                    connection.settimeout(0.05)
                    try:
                        raw = connection.recv(8192)
                    except TimeoutError:
                        continue
                    buffers[connection] = buffers.get(connection, b"") + raw
                    head = buffers[connection]
                    self.assertLessEqual(len(head), 65536, "loopback HTTP header exceeds observer bound")
                    if b"\r\n\r\n" in head and head.startswith((b"GET ", b"POST ")):
                        request = head
                        break
                if not request:
                    time.sleep(0.02)
            self.assertTrue(request, "no driver HTTP request reached the loopback peer")
            self.assertTrue(b"inventorydb" in request)
            self.assertTrue(config["password"].encode() in request,
                            "configured password text did not reach driver headers")
            if attempt:
                self.assertFalse(credential.encode() in request,
                                 "file-reference content unexpectedly reached the header")
            time.sleep(0.5)
            status, metrics = b.http("GET", metrics_path)
            self.assertEqual(status, 200)
            self.assertEqual(metrics["metrics"]["matched"], 1)
            self.assertEqual(metrics["metrics"]["actions.total"], 0)
            self.assertEqual(metrics["metrics"]["actions.failed"], 0)
            self.assertEqual(metrics["metrics"]["actions.success"], 0)
            # A real HTTP request is still waiting after 500 times the
            # selected one-millisecond timeout. The driver never gets it.
        # The silent observer does not acknowledge inserts or qualify a
        # database. Raw request headers are never printed or retained.

    # // AZ-01 inventory: current behaviour; owner MV-03a
    def test_mqtt_connector_keeps_tls_and_protocol_controls_but_uses_plain_v4(self):
        remote_base = Path(self.scratch.name) / "remote"
        remote_base.mkdir()
        remote = Broker(remote_base)
        self.addCleanup(remote.stop)
        remote.configure(anonymous=False)
        remote.start()
        self.assertEqual(remote.http("POST", "/api/v1/auth/users", {
            "username": "audituser", "password": remote.password})[0], 201)
        self.assertEqual(remote.http("POST", "/api/v1/auth/acls", {
            "client_pattern": "*", "action": "all", "topic_pattern": "inventory/#", "allow": True})[0], 201)
        receiver, ack = remote.connect("mqtt-connector-observer", True)
        self.assertEqual(ack, "0000")
        self.assertEqual(remote.subscribe(receiver, "inventory/bridge"), "00")
        b = self.boot()
        config = {"type": "mqtt", "name": "inventorymqtt", "server": "127.0.0.1:" + str(remote.mqtt),
                  "client_id": "inventory-bridge", "username": "audituser", "password": remote.password,
                  "proto_ver": "v5", "keepalive": "1s", "clean_start": False,
                  "clientid_prefix": "requested-prefix", "pool_size": 2, "max_inflight": 1,
                  "connect_timeout": "1s", "retry_interval": "1s", "reconnect_interval": "1s",
                  "bridge_mode": True, "mode": "cluster_shareload",
                  "static_clientids": [{"node": "az-01-entry", "ids": ["requested-static-0", {
                      "clientid": "requested-static-1", "username": "audituser", "password": remote.password}]}],
                  "tcp_opts": {"active_n": 1, "buffer": "4KB", "delay_send": True,
                               "keepalive": True, "nodelay": False, "recbuf": "8KB", "sndbuf": "8KB"},
                  "ssl": {"enable": True, "verify": "verify_peer",
                          "cacertfile": str(ROOT / "crates/broker-node/tests/k1_05/ca-cert.pem")}}
        self.assertEqual(b.http("POST", "/api/v5/connectors", config)[0], 201)
        self.create_forwarding_action_rule(b, config, "inventorymqttrule", "inventory/bridge")
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            pub, ack = b.connect("bridge-source-" + str(attempt))
            self.assertEqual(ack, "0000")
            payload = json.dumps({"entry": "bridge-field-" + str(attempt)}, separators=(",", ":")).encode()
            publish(b, pub, "inventory/bridge", payload)
            self.assertEqual(packet(receiver), (0x30, word("inventory/bridge") + payload))
            status, actual = remote.http("GET", "/api/v5/clients/inventory-bridge")
            self.assertEqual(status, 200)
            self.assertEqual(actual["username"], "audituser")
            self.assertEqual(actual["proto_ver"], 4)
            self.assertEqual(actual["keepalive"], 60)
            self.assertTrue(actual["clean_start"])
            status, stored = b.http("GET", "/api/v5/connectors/inventorymqtt")
            self.assertEqual(status, 200)
            self.assert_fields_kept(stored, {key: value for key, value in config.items() if key != "password"})
            before = b.http("GET", "/api/v5/connectors/inventorymqtt")
            self.assertEqual(b.http("PUT", "/api/v5/connectors/inventorymqtt", {
                **config, "server": "127.0.0.1:notaport"})[0], 400)
            self.assertTrue(b.http("GET", "/api/v5/connectors/inventorymqtt") == before)
            payload = json.dumps({"entry": "bridge-after-refusal-" + str(attempt)}, separators=(",", ":")).encode()
            publish(b, pub, "inventory/bridge", payload)
            self.assertEqual(packet(receiver), (0x30, word("inventory/bridge") + payload))
        # The remote listener is the real broker's plain TCP listener and
        # validates the generated password. No TLS handshake is involved.

    # // AZ-01 inventory: current behaviour; owner MV-11c
    def test_retainer_enable_is_ignored_while_live_caps_reset_at_restart(self):
        b = self.boot()
        original = b.http("GET", "/api/v5/mqtt/retainer")
        self.assertEqual(original[0], 200)
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/retainer", {"enable": False})[0], 200)
        pub, _ = b.connect("retainer-cap-publisher")
        publish(b, pub, "inventory/cap-disabled", b"off", True)
        self.assertEqual(b.http("GET", "/api/v5/mqtt/retainer/messages")[1]["meta"]["count"], 1)
        publish(b, pub, "inventory/cap-disabled", b"", True)
        self.assertEqual(b.http("GET", "/api/v5/mqtt/retainer/messages")[1]["meta"]["count"], 0)
        chosen = {"enable": True, "max_payload_size": "8B", "backend": {"max_retained_messages": 1}}
        status, kept = b.http("PUT", "/api/v5/mqtt/retainer", chosen)
        self.assertEqual(status, 200)
        publish(b, pub, "inventory/cap-first", b"one", True)
        publish(b, pub, "inventory/cap-big", b"too-large-for-cap", True)
        publish(b, pub, "inventory/cap-second", b"two", True)
        status, listed = b.http("GET", "/api/v5/mqtt/retainer/messages")
        self.assertEqual(status, 200)
        self.assertEqual(listed["meta"]["count"], 1)
        self.assertEqual([row["topic"] for row in listed["data"]], ["inventory/cap-first"])
        sub, _ = b.connect("retainer-cap-reader")
        self.assertEqual(b.subscribe(sub, "inventory/cap-first"), "00")
        self.assertEqual(packet(sub), (0x31, word("inventory/cap-first") + b"one"))
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/retainer", {"max_payload_size": "invalid"})[0], 400)
        self.assertEqual(b.http("GET", "/api/v5/mqtt/retainer"), (200, kept))
        self.assertEqual(b.http("PUT", "/api/v5/mqtt/retainer", {"enable": False})[0], 200)
        b.stop()
        b.start()
        self.assertEqual(b.http("GET", "/api/v5/mqtt/retainer"), original)
        # Enable is ignored on native ingress. Live caps work but reset.

    # // AZ-01 inventory: current behaviour; owner MV-03a
    def test_amqp_timeout_diagnostic_contains_the_configured_password(self):
        b = self.broker
        b.configure()
        config_file = b.cfg / "indra.toml"
        config_file.write_text(config_file.read_text().replace('level="error"', 'level="warn"'))
        b.start()
        peer = SilentPeer()
        self.addCleanup(peer.close)
        credential = secrets.token_hex(24)
        config = {"type": "rabbitmq", "name": "inventoryamqp",
                  "server": "127.0.0.1:" + str(peer.port),
                  "username": "inventoryuser", "password": credential}
        status, result = b.http("POST", "/api/v5/connectors", config)
        expected = os.environ.get("AZ01_RABBITMQ_COMPILED")
        if expected not in ["0", "1"]:
            raise AssertionError("wrapper must name the binary's compiled AMQP feature")
        if expected == "0":
            self.assertEqual(status, 400)
            self.assertEqual(result["code"], "BAD_REQUEST")
            self.assertTrue("needs cargo feature 'rabbitmq'" in result["message"])
            self.assertEqual(b.http("GET", "/api/v5/connectors/inventoryamqp")[0], 404)
            return
        self.assertEqual(status, 201)
        self.create_forwarding_action_rule(b, config, "inventoryamqprule", "inventory/amqp")
        pub, _ = b.connect("amqp-timeout-publisher")
        pub.settimeout(20)
        publish(b, pub, "inventory/amqp", b'{"entry":"diagnostic"}')
        until = time.monotonic() + 15
        while len(peer.clients) < 2 and time.monotonic() < until:
            time.sleep(0.05)
        self.assertGreaterEqual(len(peer.clients), 2)
        # The first connection and its one retry both need a timeout.
        # A completed publisher PING and an extra bounded wait let the
        # rule worker report its final failure before logs are collected.
        time.sleep(5.5)
        b.stop()
        output = b"".join(b.last_output)
        self.assertTrue(b"rabbitmq driver connect timeout" in output)
        self.assertTrue(credential.encode() in output,
                        "timeout diagnostic did not contain the generated credential")
        # Only presence is asserted; never print or retain the raw log.

    # // AZ-01 inventory: current behaviour; owner MV-12b
    def test_republish_direct_flag_does_not_select_normal_rule_processing(self):
        b = self.boot()
        downstream = {"id": "inventorydownstream", "sql": 'SELECT * FROM "inventory/dispatch-output"',
                      "actions": [{"function": "republish", "args": {
                          "topic": "inventory/dispatch-final", "payload": "${payload}",
                          "qos": 0, "retain": False, "direct_dispatch": True}}]}
        self.assertEqual(b.http("POST", "/api/v5/rules", downstream)[0], 201)
        upstream = {"id": "inventoryupstream", "sql": 'SELECT * FROM "inventory/dispatch-input"',
                    "actions": [{"function": "republish", "args": {
                        "topic": "inventory/dispatch-output", "payload": "${payload}",
                        "qos": 0, "retain": False, "direct_dispatch": False}}]}
        self.assertEqual(b.http("POST", "/api/v5/rules", upstream)[0], 201)
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            sub, _ = b.connect("dispatch-reader-" + str(attempt))
            self.assertEqual(b.subscribe(sub, "inventory/dispatch-output"), "00")
            pub, _ = b.connect("dispatch-publisher-" + str(attempt))
            for direct in [False, True]:
                upstream["actions"][0]["args"]["direct_dispatch"] = direct
                self.assertEqual(b.http("PUT", "/api/v5/rules/inventoryupstream", upstream)[0], 200)
                raw = b'{"v":67}'
                publish(b, pub, "inventory/dispatch-input", raw)
                self.assertEqual(packet(sub), (0x30, word("inventory/dispatch-output") + raw))
                # Observe beyond the completed upstream delivery. The call-site
                # audit proves both modes take the same direct fan-out path.
                until = time.monotonic() + 1.2
                while time.monotonic() < until:
                    status, metrics = b.http("GET", "/api/v5/rules/inventorydownstream/metrics")
                    self.assertEqual(status, 200)
                    self.assertEqual(metrics["metrics"]["matched"], 0)
                    time.sleep(0.05)
                actual = b.http("GET", "/api/v5/rules/inventoryupstream")[1]
                self.assertEqual(actual["actions"][0]["args"]["direct_dispatch"], direct)
            refused = {**upstream, "actions": [{"function": "republish", "args": {
                **upstream["actions"][0]["args"], "direct_dispatch": 7}}]}
            self.assertEqual(b.http("PUT", "/api/v5/rules/inventoryupstream", refused)[0], 400)
            self.assertEqual(b.http("GET", "/api/v5/rules/inventoryupstream")[1], actual)

    def test_file_tls_policy_reaches_handshake_and_certificate_identity(self):
        b = self.broker
        b.configure()
        fixture = ROOT / "crates/broker-node/tests/k1_05"
        tls_port = port()
        config_path = b.cfg / "indra.toml"
        base = config_path.read_text()

        def configure(require, legacy):
            policy = f'''[listeners.tls]
 enabled=true
 bind="127.0.0.1:{tls_port}"
 cert_file="{fixture / 'server-cert.pem'}"
 key_file="{fixture / 'server-key.pem'}"
 verify="verify_peer"
 cacertfile="{fixture / 'ca-cert.pem'}"
 fail_if_no_peer_cert={str(require).lower()}
 verify_client_cert={str(legacy).lower()}
 peer_cert_as_username="cn"
 peer_cert_as_clientid="cn"
'''
            config_path.write_text(base.replace("[listeners.tls]\nenabled=false\n", policy))

        def connect(client):
            context = ssl.create_default_context(cafile=str(fixture / "ca-cert.pem"))
            if client:
                context.load_cert_chain(fixture / (client + "-cert.pem"), fixture / (client + "-key.pem"))
            raw = socket.create_connection(("127.0.0.1", tls_port), timeout=5)
            b.sockets.append(raw)
            try:
                sock = context.wrap_socket(raw, server_hostname="127.0.0.1")
                b.sockets.append(sock)
                sock.sendall(frame(16, b"\x00\x04MQTT\x04\x02\x00\x3c" + word("inventory-tls-ignored")))
                return sock, packet(sock)
            except OSError:
                return None, None

        for require, legacy in [(True, False), (False, True)]:
            configure(require, legacy)
            b.start()
            for client in [None, "wrong-client", "expired-client"]:
                _, reply = connect(client)
                self.assertNotEqual(reply, (0x20, b"\x00\x00"), "untrusted or missing certificate connected")
            sock, reply = connect("client-a")
            self.assertEqual(reply, (0x20, b"\x00\x00"))
            status, session = b.http("GET", "/api/v5/clients/k1-05-a")
            self.assertEqual(status, 200)
            self.assertEqual(session["clientid"], "k1-05-a")
            self.assertEqual(session["username"], "k1-05-a")
            self.assertEqual(b.subscribe(sock, "inventory/tls"), "00")
            sock.sendall(frame(0x30, word("inventory/tls") + b"verified-file-policy"))
            self.assertEqual(packet(sock), (0x30, word("inventory/tls") + b"verified-file-policy"))
            sock.sendall(b"\xc0\x00")
            self.assertEqual(packet(sock), (0xd0, b""))
            b.stop()
        # Both policies come from the file in separate broker processes.
        # This covers certificate TLS, not PSK or a database qualification.

    def test_api_key_fields_control_real_requests_and_survive_restart(self):
        b = self.broker
        b.configure()
        seed_admin(b)
        b.start()
        token = login(b, "inventoryadmin", b.admin_password)
        path = "/api/v5/api_key/inventorykey"
        status, minted = b.http("POST", "/api/v5/api_key", {
            "name": "inventorykey", "desc": "inventory label", "enable": True,
            "expired_at": "infinity", "role": "administrator", "scopes": ["monitoring"]}, auth=token)
        self.assertEqual(status, 200)
        secret = minted["api_secret"]
        key_id = minted["api_key"]
        credentials = "Basic " + base64.b64encode((key_id + ":" + secret).encode()).decode()

        def access(path, expected, method="GET", body=None):
            self.assertEqual(b.http(method, path, body, auth=False, authorization=credentials)[0], expected)

        def patch(body):
            self.assertEqual(b.http("PUT", path, body, auth=token)[0], 200)

        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
                token = login(b, "inventoryadmin", b.admin_password)
            access("/api/v5/metrics", 200)
            access("/api/v5/clients", 403)
            wrong_id = "Basic " + base64.b64encode(("wrong-identifier:" + secret).encode()).decode()
            self.assertEqual(b.http("GET", "/api/v5/metrics", auth=False, authorization=wrong_id)[0], 401)
            wrong_secret = "Basic " + base64.b64encode((key_id + ":" + secrets.token_hex(24)).encode()).decode()
            self.assertEqual(b.http("GET", "/api/v5/metrics", auth=False, authorization=wrong_secret)[0], 401)
            patch({"scopes": []})
            access("/api/v5/metrics", 403)
            patch({"scopes": "unset", "role": "viewer"})
            access("/api/v5/clients", 200)
            access("/api/v5/publish", 403, "POST", {"topic": "inventory/key", "payload": "blocked"})
            patch({"role": "administrator", "scopes": ["monitoring"], "enable": False})
            access("/api/v5/metrics", 401)
            patch({"enable": True, "expired_at": 1})
            access("/api/v5/metrics", 401)
            patch({"expired_at": "infinity"})
            access("/api/v5/metrics", 200)
            before = b.http("GET", path, auth=token)
            self.assertEqual(b.http("PUT", path, {"enable": False, "scopes": ["unknown-scope"]}, auth=token)[0], 400)
            self.assertEqual(b.http("GET", path, auth=token), before)
            access("/api/v5/metrics", 200)
            state = tomllib.loads((b.data / "state.toml").read_text())["api_keys"]["keys"][0]
            self.assertTrue(state["key_id"] == key_id)
            self.assertTrue(state["key_hash"] == hashlib.sha256(secret.encode()).hexdigest())
            self.assertEqual(state["scopes"], ["monitoring"])
            self.assertTrue(state["scopes_set"])
            self.assertEqual(state["name"], "inventorykey")
            self.assertEqual(state["description"], "inventory label")
            self.assertFalse(secret in (b.data / "state.toml").read_text())

    def test_admin_password_role_and_mfa_state_control_login_after_restart(self):
        b = self.broker
        b.configure()
        seed_admin(b)
        state_path = b.data / "state.toml"
        state_path.write_text(state_path.read_text().replace("must_change_password=false", "must_change_password=true"))
        b.start()
        token = login(b, "inventoryadmin", b.admin_password)
        self.assertEqual(b.http("GET", "/api/v5/metrics", auth=token)[0], 403)
        replacement = "A1!" + secrets.token_hex(24)
        self.assertEqual(b.http("PUT", "/api/v5/users/inventoryadmin/change_pwd", {
            "old_pwd": b.admin_password, "new_pwd": replacement}, auth=token)[0], 204)
        self.assertEqual(b.http("POST", "/api/v5/login", {
            "username": "inventoryadmin", "password": b.admin_password}, auth=False)[0], 401)
        b.admin_password = replacement
        token = login(b, "inventoryadmin", b.admin_password)
        self.assertEqual(b.http("GET", "/api/v5/metrics", auth=token)[0], 200)
        viewer_password = "A1!" + secrets.token_hex(24)
        self.assertEqual(b.http("POST", "/api/v5/users", {
            "username": "inventoryviewer", "password": viewer_password, "role": "viewer"}, auth=token)[0], 200)
        viewer = login(b, "inventoryviewer", viewer_password)
        self.assertEqual(b.http("GET", "/api/v5/metrics", auth=viewer)[0], 200)
        self.assertEqual(b.http("POST", "/api/v5/users", {
            "username": "blockeduser", "password": viewer_password}, auth=viewer)[0], 403)
        status, setup = b.http("POST", "/api/v5/sso/mfa/setup", {"username": "inventoryadmin"}, auth=token)
        self.assertEqual(status, 200)
        encoded = setup["secret"]
        secret = base64.b32decode(encoded + "=" * (-len(encoded) % 8))
        counter = int(time.time()) // 30 - 1
        digest = hmac.digest(secret, struct.pack("!Q", counter), "sha1")
        offset = digest[-1] & 15
        code = str((int.from_bytes(digest[offset:offset + 4], "big") & 0x7fffffff) % 1000000).zfill(6)
        self.assertEqual(b.http("POST", "/api/v5/sso/mfa/verify", {
            "username": "inventoryadmin", "code": code}, auth=token)[0], 200)
        saved = state_path.read_bytes()
        self.assertEqual(b.http("POST", "/api/v5/sso/mfa/verify", {
            "username": "inventoryadmin", "code": "invalid"}, auth=token)[0], 400)
        self.assertTrue(state_path.read_bytes() == saved)
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            request = {"username": "inventoryadmin", "password": b.admin_password}
            status, body = b.http("POST", "/api/v5/login", request, auth=False)
            self.assertEqual((status, body["code"]), (401, "BAD_MFA_TOKEN"))
            status, body = b.http("POST", "/api/v5/login", {**request, "mfa_token": code}, auth=False)
            self.assertEqual((status, body["code"]), (401, "BAD_MFA_TOKEN"))
            viewer = login(b, "inventoryviewer", viewer_password)
            self.assertEqual(b.http("POST", "/api/v5/users", {
                "username": "blockeduser", "password": viewer_password}, auth=viewer)[0], 403)
            entries = tomllib.loads(state_path.read_text())["admin_users"]["users"]
            admin = next(row for row in entries if row["username"] == "inventoryadmin")
            self.assertFalse(admin["must_change_password"])
            self.assertTrue(admin["mfa_enabled"])
            self.assertTrue(base64.b64decode(admin["mfa_secret"]) == secret)
            self.assertEqual(admin["mfa_last_step"], counter)
        self.assertEqual(b.http("DELETE", "/api/v5/users/inventoryadmin/mfa", auth=viewer)[0], 403)

    def test_json_and_avro_registry_documents_are_persisted_artifacts(self):
        b = self.boot()
        definitions = [
            {"name": "inventoryjson", "type": "json", "description": "JSON registry label",
             "schema": {"type": "object", "properties": {"v": {"type": "integer"}}}},
            {"name": "inventoryavro", "type": "avro", "description": "Avro registry label",
             "schema": {"type": "record", "name": "Inventory", "fields": [{"name": "v", "type": "int"}]}}]
        for config in definitions:
            self.assertEqual(b.http("POST", "/api/v5/schema_registry", config)[0], 201)
        for attempt in range(2):
            if attempt:
                b.stop()
                b.start()
            for config in definitions:
                path = "/api/v5/schema_registry/" + config["name"]
                status, actual = b.http("GET", path)
                self.assertEqual(status, 200)
                self.assert_fields_kept(actual, config)
                self.assertEqual(json.loads(actual["source"]), config["schema"])
                self.assertEqual(b.http("POST", "/api/v5/schema_registry", {
                    **config, "type": "unknown"})[0], 400)
                self.assertEqual(b.http("GET", path), (200, actual))
        # Registry operations use these documents. They do not promise an
        # automatic codec on arbitrary MQTT messages.

    # // AZ-01 inventory: current behaviour; owner MV-13a
    def test_mcp_dispatch_uses_the_same_settings_store(self):
        b = self.boot()
        call = {"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": "put_api_v5_authorization_settings",
                           "arguments": {"body": {"no_match": "allow", "deny_action": "ignore"}}}}
        status, result = b.http("POST", "/mcp?toolsets=all", call)
        self.assertEqual(status, 200)
        self.assertIn("result", result)
        self.assertFalse(result["result"].get("isError", False))
        status, current = b.http("GET", "/api/v5/authorization/settings")
        self.assertEqual(status, 200)
        self.assertEqual(current["no_match"], "allow")


if __name__ == "__main__":
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(InventoryEntry)
    if suite.countTestCases() < 86:
        raise SystemExit("inventory suite has no complete case set")
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
