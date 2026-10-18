#!/usr/bin/env python3
"""Client-library checklist for the Eclipse Paho Python client.

The program speaks to a running broker through client sockets only. It
prints one line for each check:

    RESULT <check> <pass|fail|not-supported> <detail>

Collect these lines and write the results table in results.md. This
program imports the real `paho.mqtt` package. Point PYTHONPATH at the
directory that holds it; see run.sh.

It covers plain TCP, TLS, WebSocket and secure WebSocket where the
library allows them. It covers MQTT 3.1.1 and MQTT 5.0.
"""

import json
import os
import queue
import signal
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

try:
    import paho.mqtt.client as mqtt
except ImportError as error:  # the runner reports this
    sys.stderr.write("paho-mqtt is not importable: %s\n" % error)
    raise SystemExit(3)

HOST = os.environ.get("MATRIX_HOST", "127.0.0.1")
TCP_PORT = int(os.environ.get("MATRIX_TCP_PORT", "1883"))
TLS_PORT = int(os.environ.get("MATRIX_TLS_PORT", "8883"))
WS_PORT = int(os.environ.get("MATRIX_WS_PORT", "8083"))
WSS_PORT = int(os.environ.get("MATRIX_WSS_PORT", "8084"))
WS_PATH = os.environ.get("MATRIX_WS_PATH", "/mqtt")
CA_FILE = os.environ.get("MATRIX_CA", "")
API_BASE = os.environ.get("MATRIX_API_BASE", "")
API_KEY = os.environ.get("MATRIX_API_KEY", "")
USER = os.environ.get("MATRIX_USER", "")
PASSWORD = os.environ.get("MATRIX_PASSWORD", "")
PREFIX = os.environ.get("MATRIX_PREFIX", "mx")
RESTART_CMD = os.environ.get("MATRIX_RESTART_CMD", "")

CHECKS = [
    "v311-connect", "v5-connect", "qos0", "qos1", "qos2", "retained",
    "will", "session-resume", "shared", "payload-1mib", "auth",
    "keepalive", "restart",
]

PORT = {"tcp": TCP_PORT, "tls": TLS_PORT, "ws": WS_PORT, "wss": WSS_PORT}
TRANSPORT = "tcp"


def emit(name, status, detail):
    print("RESULT %s %s %s" % (name, status, detail), flush=True)


def tag(suffix):
    return "%s-%s-%s" % (PREFIX, suffix, os.urandom(4).hex())


def reason_code_of(reason):
    """Return the numeric CONNACK code of a Paho reason code."""
    value = getattr(reason, "value", reason)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


class Conn:
    """One Paho client with an event queue, so checks stay readable."""

    def __init__(self, client_id, protocol=mqtt.MQTTv311, clean=True, will=None):
        transport = "websockets" if TRANSPORT in ("ws", "wss") else "tcp"
        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id,
            protocol=protocol,
            clean_session=(clean if protocol == mqtt.MQTTv311 else None),
            transport=transport,
        )
        if USER:
            self.client.username_pw_set(USER, PASSWORD)
        if will is not None:
            topic, payload, qos, retain = will
            self.client.will_set(topic, payload, qos, retain)
        if TRANSPORT in ("tls", "wss"):
            self.client.tls_set(ca_certs=CA_FILE or None)
        if TRANSPORT in ("ws", "wss"):
            self.client.ws_set_options(path=WS_PATH)
        self.messages = queue.Queue()
        self.connected = threading.Event()
        self.disconnected = threading.Event()
        self.code = None
        self.session_present = False
        self.connect_count = 0
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.client.on_disconnect = self._on_disconnect

    def _on_connect(self, client, userdata, flags, reason, properties=None):
        self.code = reason_code_of(reason)
        self.session_present = bool(getattr(flags, "session_present", False))
        self.connect_count += 1
        self.connected.set()

    def _on_message(self, client, userdata, message):
        self.messages.put((message.topic, message.payload, message.retain))

    def _on_disconnect(self, client, userdata, flags=None, reason=None,
                       properties=None):
        self.disconnected.set()

    def start(self, keepalive=15, timeout=8):
        self.client.connect_async(HOST, PORT[TRANSPORT], keepalive)
        self.client.loop_start()
        return self.connected.wait(timeout)

    def wait_message(self, timeout=8):
        try:
            return self.messages.get(timeout=timeout)
        except queue.Empty:
            return None

    def stop(self, graceful=True):
        try:
            if graceful:
                self.client.disconnect()
        except Exception:
            pass
        try:
            self.client.loop_stop()
        except Exception:
            pass


def connect_ok(conn, timeout=8):
    if not conn.start(timeout=timeout):
        return False, "no CONNACK within %ss" % timeout
    return conn.code == 0, "CONNACK %s" % conn.code


def check_connect(version):
    protocol = mqtt.MQTTv5 if version == 5 else mqtt.MQTTv311
    conn = Conn(tag("v%d" % version), protocol=protocol)
    try:
        ok, detail = connect_ok(conn)
        if not ok:
            raise RuntimeError(detail)
        return detail
    finally:
        conn.stop()


def check_qos(qos):
    topic = tag("qos%d" % qos)
    payload = ("q%d-%s" % (qos, os.urandom(6).hex())).encode()
    sub = Conn(tag("sub"))
    pub = Conn(tag("pub"))
    try:
        ok, detail = connect_ok(sub)
        if not ok:
            raise RuntimeError("subscriber: %s" % detail)
        sub.client.subscribe(topic, qos)
        time.sleep(0.4)
        ok, detail = connect_ok(pub)
        if not ok:
            raise RuntimeError("publisher: %s" % detail)
        pub.client.publish(topic, payload, qos)
        item = sub.wait_message(timeout=10)
        if item is None:
            raise RuntimeError("no delivery at QoS %d" % qos)
        if item[1] != payload:
            raise RuntimeError("payload mismatch")
        return "delivered at QoS %d" % qos
    finally:
        pub.stop()
        sub.stop()


def check_retained():
    topic = tag("retain")
    payload = ("retained-%s" % os.urandom(6).hex()).encode()
    pub = Conn(tag("rpub"))
    try:
        ok, detail = connect_ok(pub)
        if not ok:
            raise RuntimeError("publisher: %s" % detail)
        pub.client.publish(topic, payload, 1, retain=True)
        time.sleep(0.8)
    finally:
        pub.stop()
    late = Conn(tag("rsub"))
    try:
        ok, detail = connect_ok(late)
        if not ok:
            raise RuntimeError("late subscriber: %s" % detail)
        late.client.subscribe(topic, 1)
        item = late.wait_message(timeout=10)
        if item is None:
            raise RuntimeError("late subscriber got no retained message")
        if item[1] != payload:
            raise RuntimeError("retained payload mismatch")
        if not item[2]:
            raise RuntimeError("retained message arrived without the retain flag")
        return "late subscriber got the retained message"
    finally:
        late.stop()


def will_victim():
    global TRANSPORT
    TRANSPORT = os.environ.get("MATRIX_WILL_TRANSPORT", "tcp")
    topic = os.environ["MATRIX_WILL_TOPIC"]
    payload = os.environ["MATRIX_WILL_PAYLOAD"].encode()
    conn = Conn("mx-will-victim", will=(topic, payload, 1, False))
    ok, _ = connect_ok(conn)
    if not ok:
        print("FAILED", flush=True)
        return
    print("READY", flush=True)
    signal.pause()


def check_will():
    topic = tag("will")
    payload = ("will-%s" % os.urandom(6).hex()).encode()
    obs = Conn(tag("wobs"))
    try:
        ok, detail = connect_ok(obs)
        if not ok:
            raise RuntimeError("observer: %s" % detail)
        obs.client.subscribe(topic, 1)
        time.sleep(0.4)
        env = dict(os.environ)
        env["MATRIX_WILL_TOPIC"] = topic
        env["MATRIX_WILL_PAYLOAD"] = payload.decode()
        env["MATRIX_WILL_TRANSPORT"] = TRANSPORT
        victim = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--will-victim"],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True,
            env=env)
        ready = victim.stdout.readline().strip() if victim.stdout else ""
        if ready != "READY":
            victim.kill()
            raise RuntimeError("will victim did not connect (got %r)" % ready)
        time.sleep(0.4)
        os.kill(victim.pid, signal.SIGKILL)
        victim.wait(timeout=5)
        item = obs.wait_message(timeout=10)
        if item is None:
            raise RuntimeError("no will after the victim was killed")
        if item[1] != payload:
            raise RuntimeError("will payload mismatch")
        return "will message published on an abrupt close"
    finally:
        obs.stop()


def check_session_resume():
    cid = tag("sess")
    topic = tag("sessq")
    payloads = [("queued-%d-%s" % (i, os.urandom(3).hex())).encode()
                for i in range(3)]
    first = Conn(cid, clean=False)
    try:
        ok, detail = connect_ok(first)
        if not ok:
            raise RuntimeError("first connect: %s" % detail)
        first.client.subscribe(topic, 1)
        time.sleep(0.5)
        first.stop()
        time.sleep(0.4)
    finally:
        first.stop()
    pub = Conn(tag("spub"))
    try:
        ok, detail = connect_ok(pub)
        if not ok:
            raise RuntimeError("publisher: %s" % detail)
        for payload in payloads:
            pub.client.publish(topic, payload, 1)
        time.sleep(0.8)
    finally:
        pub.stop()
    resumed = Conn(cid, clean=False)
    try:
        ok, detail = connect_ok(resumed)
        if not ok:
            raise RuntimeError("resumed connect: %s" % detail)
        if not resumed.session_present:
            raise RuntimeError("session_present was false on resume")
        got = set()
        deadline = time.time() + 10
        while time.time() < deadline and len(got) < len(payloads):
            item = resumed.wait_message(timeout=1)
            if item is not None:
                got.add(item[1])
        if got != set(payloads):
            raise RuntimeError("queued messages missing: got %d of %d"
                               % (len(got), len(payloads)))
        return "session resumed and replayed %d queued messages" % len(got)
    finally:
        resumed.stop()


def check_shared():
    group = tag("grp")
    inner = tag("shared")
    filter_ = "$share/%s/%s" % (group, inner)
    count = 10
    first = Conn(tag("sh1"))
    second = Conn(tag("sh2"))
    try:
        for conn in (first, second):
            ok, detail = connect_ok(conn)
            if not ok:
                raise RuntimeError("shared subscriber: %s" % detail)
            conn.client.subscribe(filter_, 1)
        time.sleep(0.6)
        pub = Conn(tag("shpub"))
        try:
            ok, detail = connect_ok(pub)
            if not ok:
                raise RuntimeError("publisher: %s" % detail)
            for index in range(count):
                pub.client.publish(inner, ("s%d" % index).encode(), 1)
            seen = []
            deadline = time.time() + 10
            while time.time() < deadline and len(seen) < count:
                for conn in (first, second):
                    try:
                        seen.append(conn.messages.get_nowait()[1])
                    except queue.Empty:
                        pass
                time.sleep(0.1)
        finally:
            pub.stop()
        if len(seen) != count:
            raise RuntimeError("shared group got %d of %d messages"
                               % (len(seen), count))
        if len(set(seen)) != count:
            raise RuntimeError("shared group delivered duplicates")
        return "shared group delivered each of %d messages once" % count
    finally:
        first.stop()
        second.stop()


def check_payload_1mib():
    topic = tag("big")
    payload = os.urandom(1024 * 1024)
    sub = Conn(tag("bigsub"))
    pub = Conn(tag("bigpub"))
    try:
        ok, detail = connect_ok(sub)
        if not ok:
            raise RuntimeError("subscriber: %s" % detail)
        sub.client.subscribe(topic, 1)
        time.sleep(0.5)
        ok, detail = connect_ok(pub)
        if not ok:
            raise RuntimeError("publisher: %s" % detail)
        pub.client.publish(topic, payload, 1)
        item = sub.wait_message(timeout=20)
        if item is None:
            raise RuntimeError("1 MiB payload was not delivered")
        if item[1] != payload:
            raise RuntimeError("1 MiB payload length %d" % len(item[1]))
        return "1 MiB payload delivered"
    finally:
        pub.stop()
        sub.stop()


def check_auth():
    if not API_BASE or not API_KEY:
        raise RuntimeError("no API entry for user setup")
    user = tag("user")
    good = "Good-pw-%s!" % os.urandom(4).hex()
    request = urllib.request.Request(
        API_BASE + "/api/v1/auth/users",
        data=json.dumps({"username": user, "password": good}).encode(),
        headers={"Authorization": "Bearer " + API_KEY,
                 "Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            if response.status not in (200, 201):
                raise RuntimeError("create user: HTTP %s" % response.status)
    except urllib.error.HTTPError as error:
        raise RuntimeError("create user: HTTP %s" % error.code)
    accepted = Conn(tag("au-ok"))
    try:
        accepted.client.username_pw_set(user, good)
        ok, detail = connect_ok(accepted)
        if not ok:
            raise RuntimeError("good credentials refused: %s" % detail)
    finally:
        accepted.stop()
    refused = Conn(tag("au-bad"))
    try:
        refused.client.username_pw_set(user, "wrong-pw")
        ok, detail = connect_ok(refused)
        if ok:
            raise RuntimeError("wrong password accepted")
        return "good accepted, bad refused (%s)" % detail
    finally:
        refused.stop()


def raw_connect(username, password, keepalive):
    """Send one MQTT 3.1.1 CONNECT on a raw socket. Return the CONNACK code."""
    sock = socket.create_connection((HOST, TCP_PORT), timeout=5)
    field = lambda value: struct.pack(">H", len(value)) + value
    body = b"\x00\x04MQTT\x04\xc2" + struct.pack(">H", keepalive)
    body += field(tag("ka").encode()) + field(username.encode())
    body += field(password.encode())
    sock.sendall(bytes([0x10, len(body)]) + body)
    sock.settimeout(5)
    ack = sock.recv(4)
    if len(ack) < 4 or ack[0] != 0x20:
        sock.close()
        return None
    return ack[3], sock


def check_keepalive():
    # A library keeps the connection alive for us, so this check uses one
    # raw socket to observe the broker's own keep-alive timeout.
    keepalive = 2
    result = raw_connect(USER, PASSWORD, keepalive)
    if result is None:
        raise RuntimeError("broker refused the keepalive probe CONNECT")
    code, sock = result
    if code != 0:
        sock.close()
        raise RuntimeError("keepalive probe CONNACK %s" % code)
    sock.settimeout(keepalive * 3 + 4)
    start = time.time()
    try:
        data = sock.recv(16)
    except socket.timeout:
        sock.close()
        raise RuntimeError("broker kept an idle connection open")
    sock.close()
    if data != b"":
        raise RuntimeError("broker sent data on the idle connection")
    return "broker closed the idle connection after %.1fs" % (time.time() - start)


def check_restart():
    if not RESTART_CMD:
        return None
    first = Conn(tag("rs"))
    try:
        ok, detail = connect_ok(first)
        if not ok:
            raise RuntimeError("first connect: %s" % detail)
    finally:
        first.stop()
    if subprocess.call(RESTART_CMD, shell=True) != 0:
        raise RuntimeError("restart command failed")
    deadline = time.time() + 30
    while time.time() < deadline:
        again = Conn(tag("rs2"))
        try:
            if again.start(timeout=3) and again.code == 0:
                return "reconnect after restart works"
        finally:
            again.stop()
        time.sleep(1)
    raise RuntimeError("no reconnect within 30s of the restart")


FUNCS = {
    "v311-connect": lambda: check_connect(4),
    "v5-connect": lambda: check_connect(5),
    "qos0": lambda: check_qos(0),
    "qos1": lambda: check_qos(1),
    "qos2": lambda: check_qos(2),
    "retained": check_retained,
    "will": check_will,
    "session-resume": check_session_resume,
    "shared": check_shared,
    "payload-1mib": check_payload_1mib,
    "auth": check_auth,
    "keepalive": check_keepalive,
    "restart": check_restart,
}


def main():
    global TRANSPORT
    if len(sys.argv) > 1 and sys.argv[1] == "--will-victim":
        will_victim()
        return
    TRANSPORT = sys.argv[1] if len(sys.argv) > 1 else "tcp"
    wanted = sys.argv[2:] if len(sys.argv) > 2 else CHECKS
    for name in CHECKS:
        if name not in wanted:
            continue
        try:
            detail = FUNCS[name]()
        except Exception as error:  # one line per check, keep going
            emit(name, "fail", "%s: %s" % (type(error).__name__, error))
            continue
        if detail is None:
            emit(name, "not-supported", "no restart command in this run")
        else:
            emit(name, "pass", detail)


if __name__ == "__main__":
    main()
