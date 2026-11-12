"""Helpers for the configuration examples.

The verifier sets the environment. This module reads it. It gives the
example scripts a small HTTP client and a raw MQTT client. Standard
library only.
"""

import json
import os
import socket
import struct
import time
import urllib.error
import urllib.request
from pathlib import Path

API_PORT = int(os.environ["API_PORT"])
MQTT_PORT = int(os.environ["MQTT_PORT"])
API_KEY = os.environ["API_KEY"]
WORK_DIR = Path(os.environ["WORK_DIR"])
CONFIG_DIR = Path(os.environ["CONFIG_DIR"])
DATA_DIR = Path(os.environ["DATA_DIR"])


def http(method, path, body=None):
    """Run one management API call. Return (status, body)."""
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        f"http://127.0.0.1:{API_PORT}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            raw = response.read().decode()
            try:
                return response.status, json.loads(raw)
            except ValueError:
                return response.status, raw
    except urllib.error.HTTPError as error:
        raw = error.read().decode()
        try:
            return error.code, json.loads(raw)
        except ValueError:
            return error.code, raw


def word(value):
    """Encode one MQTT length-prefixed string."""
    raw = value.encode() if isinstance(value, str) else value
    return struct.pack("!H", len(raw)) + raw


def frame(kind, body):
    """Encode one MQTT packet."""
    remaining = len(body)
    head = bytearray([kind])
    while True:
        digit = remaining % 128
        remaining //= 128
        head.append(digit | (128 if remaining else 0))
        if not remaining:
            return bytes(head) + body


def exact(sock, length):
    """Read exactly this many bytes from a socket."""
    raw = b""
    while len(raw) < length:
        chunk = sock.recv(length - len(raw))
        if not chunk:
            raise AssertionError("connection closed")
        raw += chunk
    return raw


def packet(sock):
    """Read one MQTT packet. Return (head, body)."""
    head = exact(sock, 1)[0]
    remaining = 0
    scale = 1
    while True:
        digit = exact(sock, 1)[0]
        remaining += (digit & 127) * scale
        if not digit & 128:
            return head, exact(sock, remaining)
        scale *= 128
        if scale > 128**3:
            raise AssertionError("invalid remaining length")


def parse_properties(raw):
    """Parse a v5 property block. Return a dict of number values."""
    pos = 0
    length = 0
    scale = 1
    while True:
        digit = raw[pos]
        pos += 1
        length += (digit & 127) * scale
        if not digit & 128:
            break
        scale *= 128
        if scale > 128**3:
            raise AssertionError("invalid property length")
    if pos + length != len(raw):
        raise AssertionError("invalid CONNACK properties")
    props = {}
    widths = {
        0x11: 4,
        0x13: 2,
        0x21: 2,
        0x22: 2,
        0x24: 1,
        0x25: 1,
        0x27: 4,
        0x28: 1,
        0x29: 1,
        0x2A: 1,
    }
    while pos < len(raw):
        key = raw[pos]
        pos += 1
        width = widths.get(key)
        if width is None:
            raise AssertionError(f"unexpected CONNACK property {key:#x}")
        if pos + width > len(raw):
            raise AssertionError("invalid CONNACK property")
        props[key] = int.from_bytes(raw[pos : pos + width], "big")
        pos += width
    return props


def connect(client, keepalive=60, clean=True, v5=False, expiry=0):
    """Connect one client. Return (socket, ack, properties)."""
    sock = socket.create_connection(("127.0.0.1", MQTT_PORT), timeout=10)
    sock.settimeout(10)
    flags = 2 if clean else 0
    if v5:
        props = b""
        if expiry:
            props = bytes([0x11]) + struct.pack("!I", expiry)
        body = (
            word("MQTT")
            + b"\x05"
            + bytes([flags])
            + struct.pack("!H", keepalive)
            + bytes([len(props)])
            + props
            + word(client)
        )
    else:
        body = (
            word("MQTT")
            + b"\x04"
            + bytes([flags])
            + struct.pack("!H", keepalive)
            + word(client)
        )
    sock.sendall(frame(16, body))
    head, ack = packet(sock)
    if head != 32 or len(ack) < 2:
        raise AssertionError(f"invalid CONNACK {head:#x} {ack.hex()}")
    if v5:
        if ack[:2] != b"\x00\x00":
            raise AssertionError(f"v5 connection refused {ack.hex()}")
        return sock, ack, parse_properties(ack[2:])
    if ack != b"\x00\x00":
        raise AssertionError(f"v4 connection refused {ack.hex()}")
    return sock, ack, {}


def subscribe(sock, topic, qos=0, v5=False):
    """Subscribe one client to one topic."""
    body = b"\x00\x01" + (b"\x00" if v5 else b"") + word(topic) + bytes([qos])
    sock.sendall(frame(130, body))
    head, body = packet(sock)
    if head != 144 or body[:2] != b"\x00\x01":
        raise AssertionError(f"invalid SUBACK {head:#x} {body.hex()}")


def publish(sock, topic, payload, qos=0, retain=False, v5=False):
    """Publish one message. Wait for the PUBACK of QoS 1."""
    kind = 0x30 | (qos << 1) | (1 if retain else 0)
    body = word(topic)
    if qos:
        body += b"\x00\x01"
    if v5:
        body += b"\x00"
    body += payload
    sock.sendall(frame(kind, body))
    if qos == 1:
        head, ack = packet(sock)
        if head != 0x40:
            raise AssertionError(f"invalid PUBACK {head:#x} {ack.hex()}")


def disconnect(sock):
    """Send DISCONNECT and close the socket."""
    try:
        sock.sendall(b"\xe0\x00")
    except OSError:
        pass
    sock.close()


def wait_for(predicate, timeout=5.0, interval=0.05):
    """Run a predicate until it returns true. Return its value."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(interval)
    return predicate()


def log_text():
    """Read the broker log of this run."""
    return (WORK_DIR / "broker.log").read_text(encoding="utf-8", errors="replace")
