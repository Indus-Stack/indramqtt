#!/usr/bin/env bash
# Boot test for a built broker.
#
# This script starts the kernel and the edge as an operator does: from an
# indra.toml file and INDRA_* variables. It then examines the behaviour
# through the sockets. It does not use internal functions.
#
# Usage:
#   tools/boot_smoke.sh <dir with indramqtt and indra> <edge ebin dir>
#
# Build the inputs first:
#   cargo build -p broker-node --bins
#   (cd beam && mkdir -p ebin && erlc -o ebin src/*.erl \
#      && cp src/indra_edge.app.src ebin/indra_edge.app)
#
# The script exits with 0 if all the checks are satisfactory. It prints
# one line for each check.
set -u

BIN_DIR=${1:?give the directory that contains indramqtt and indra}
EBIN=${2:?give the ebin directory of the edge}
# The script starts the edge from a different directory. Thus it needs
# absolute paths.
BIN_DIR=$(cd "$BIN_DIR" 2>/dev/null && pwd) || { echo "FAIL: $1 is not a directory"; exit 2; }
EBIN=$(cd "$EBIN" 2>/dev/null && pwd) || { echo "FAIL: $2 is not a directory"; exit 2; }
KERNEL=$BIN_DIR/indramqtt
CTL=$BIN_DIR/indra

for tool in "$KERNEL" "$CTL"; do
    [ -x "$tool" ] || { echo "FAIL: $tool is not an executable file"; exit 2; }
done
[ -f "$EBIN/indra_edge_sup.beam" ] || { echo "FAIL: $EBIN does not contain the edge"; exit 2; }
command -v erl >/dev/null || { echo "FAIL: erl is not in PATH"; exit 2; }
command -v python3 >/dev/null || { echo "FAIL: python3 is not in PATH"; exit 2; }

ROOT=$(mktemp -d)
KERNEL_PID=
EDGE_PID=
failures=0

cleanup() {
    [ -n "$EDGE_PID" ] && kill "$EDGE_PID" 2>/dev/null
    [ -n "$KERNEL_PID" ] && kill "$KERNEL_PID" 2>/dev/null
    wait 2>/dev/null
    rm -rf "$ROOT"
}
trap cleanup EXIT

pass() { echo "ok   $1"; }
fail() { echo "FAIL $1"; failures=$((failures + 1)); }
check() { if [ "$2" = 0 ]; then pass "$1"; else fail "$1"; fi; }

# Get free ports. The checks use these ports and not the defaults. Thus
# a check is satisfactory only if the broker reads the port from the file.
read -r API_PORT LINK_PORT MQTT_PORT WS_PORT <<EOF
$(python3 - <<'PY'
import socket
socks = [socket.socket() for _ in range(4)]
for s in socks:
    s.bind(("127.0.0.1", 0))
print(" ".join(str(s.getsockname()[1]) for s in socks))
for s in socks:
    s.close()
PY
)
EOF

API_KEY_GOOD=boot-smoke-key-0001
# With SMOKE_NATIVE=1 the kernel owns the plaintext MQTT listener
# (listeners.tcp.native = true). Without it the edge owns the listener.
# The MQTT checks are the same for the two modes.
if [ "${SMOKE_NATIVE:-0}" = 1 ]; then NATIVE=true; else NATIVE=false; fi
mkdir -p "$ROOT/cfg" "$ROOT/data"
cat > "$ROOT/cfg/indra.toml" <<EOF
[node]
id = "boot-smoke"
data_dir = "$ROOT/data"
brokerlink_bind = "127.0.0.1:$LINK_PORT"

[listeners.tcp]
bind = "127.0.0.1:$MQTT_PORT"
native = $NATIVE

[listeners.ws]
bind = "127.0.0.1:$WS_PORT"
path = "/smoke"

[listeners.api]
bind = "127.0.0.1:$API_PORT"

[logging]
level = "warn"

# A client with a username goes into the tenant of that username.
[[tenants.rules]]
expression = "t-\${username}"

# Listener-scoped rules. The default rule is first, so edge
# clients keep the default scope. Scoped rules stay to prove
# scoping at connect does not break delivery.
[[tenants.rules]]
listener = "tls"
expression = "t-tls-\${username}"

[[tenants.rules]]
listener = "tcp"
expression = "t-tcp-\${username}"
EOF

# --- 1. The kernel starts from the file and the credential variables ---
INDRA_API_KEYS=$API_KEY_GOOD INDRA_API_KEY=$API_KEY_GOOD \
    "$KERNEL" --config-dir "$ROOT/cfg" > "$ROOT/kernel.log" 2>&1 &
KERNEL_PID=$!

healthz=
for _ in $(seq 1 50); do
    healthz=$(python3 - "$API_PORT" <<'PY' 2>/dev/null
import sys, urllib.request
print(urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/healthz", timeout=1).read().decode())
PY
)
    [ "$healthz" = OK ] && break
    kill -0 "$KERNEL_PID" 2>/dev/null || break
    sleep 0.2
done
kill -0 "$KERNEL_PID" 2>/dev/null
check "the kernel starts when INDRA_API_KEYS and INDRA_API_KEY are set" $?
[ "$healthz" = OK ]
check "listeners.api.bind from indra.toml is the bind in operation" $?
if grep -q ' INFO' "$ROOT/kernel.log"; then
    fail "logging.level from indra.toml is the level in operation"
else
    pass "logging.level from indra.toml is the level in operation"
fi

# --- 2. The operator CLI uses an API key ---
"$CTL" ctl --endpoint "http://127.0.0.1:$API_PORT" --api-key "$API_KEY_GOOD" status \
    > "$ROOT/ctl.log" 2>&1
check "indra ctl gets access with a key from INDRA_API_KEYS" $?
if "$CTL" ctl --endpoint "http://127.0.0.1:$API_PORT" --api-key wrong-key-wrong-key status \
    > /dev/null 2>&1; then
    fail "indra ctl gets no access with an incorrect key"
else
    pass "indra ctl gets no access with an incorrect key"
fi
"$CTL" ctl --endpoint "http://127.0.0.1:$API_PORT" --api-key "$API_KEY_GOOD" --json \
    config explain --key node.id 2>/dev/null | grep -q 'boot-smoke'
check "explain shows the value from indra.toml" $?

# --- 3. Each setting in the schema is permitted in indra.toml ---
python3 - "$ROOT" > "$ROOT/all.count" <<'PY'
import json, os, sys
root = sys.argv[1]
schema = json.load(open("schemas/config-schema.json"))
lines = []
def walk(prefix, node):
    scalars, tables = [], []
    for name, spec in sorted(node.get("properties", {}).items()):
        if spec.get("type") == "object":
            tables.append((name, spec))
        elif "default" in spec:
            scalars.append((name, spec["default"]))
    if prefix and scalars:
        lines.append(f"[{prefix}]")
        for name, default in scalars:
            lines.append(f"{name} = {json.dumps(default)}")
        lines.append("")
    for name, spec in tables:
        walk(f"{prefix}.{name}" if prefix else name, spec)
walk("", schema)
os.makedirs(f"{root}/all", exist_ok=True)
open(f"{root}/all/indra.toml", "w").write("\n".join(lines))
print(sum(1 for line in lines if " = " in line))
PY
"$KERNEL" --config-dir "$ROOT/all" --print-edge-args > "$ROOT/all.log" 2>&1
all_status=$?
[ "$all_status" = 0 ] || head -3 "$ROOT/all.log"
check "indra.toml accepts each setting in schemas/config-schema.json" "$all_status"

# --- 4. The edge gets its listeners from the resolved configuration ---
mapfile -t EDGE_ARGS < <("$KERNEL" --config-dir "$ROOT/cfg" --print-edge-args)
[ "${#EDGE_ARGS[@]}" -gt 0 ]
check "the kernel prints the edge arguments" $?
(cd "$ROOT" && exec erl -noshell -noinput -pa "$EBIN" "${EDGE_ARGS[@]}" \
    -eval '{ok, _} = application:ensure_all_started(indra_edge), timer:sleep(infinity).' \
    > "$ROOT/edge.log" 2>&1) &
EDGE_PID=$!

python3 - "$MQTT_PORT" "$WS_PORT" <<'PY'
import base64, os, socket, struct, sys, time
mqtt_port, ws_port = int(sys.argv[1]), int(sys.argv[2])

def wait_port(port):
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=2)
        except OSError:
            time.sleep(0.1)
    raise SystemExit(f"FAIL the edge does not listen on port {port} from indra.toml")

def packet(kind, body):
    return bytes([kind, len(body)]) + body

def field(data):
    return struct.pack(">H", len(data)) + data

def mqtt_connect(sock, client_id, username=None):
    if username is None:
        body = b"\x00\x04MQTT\x04\x02\x00\x3c" + field(client_id)
    else:
        # Connect flags: username, password and clean session.
        body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(b"pw")
    sock.sendall(packet(0x10, body))
    return sock.recv(4) == b"\x20\x02\x00\x00"

def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""

failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1

sub = wait_port(mqtt_port)
report("listeners.tcp.bind from indra.toml is the MQTT bind in operation", mqtt_connect(sub, b"smoke-sub"))
topic = b"smoke/check"
sub.sendall(packet(0x82, b"\x00\x01" + struct.pack(">H", len(topic)) + topic + b"\x00"))
suback = sub.recv(5)
pub = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
mqtt_connect(pub, b"smoke-pub")
pub.sendall(packet(0x30, struct.pack(">H", len(topic)) + topic + b"smoke-ok"))
sub.settimeout(5)
try:
    delivered = sub.recv(100)
except OSError:
    delivered = b""
report("a publish goes to a subscriber through the edge and the kernel",
       suback[:1] == b"\x90" and delivered.endswith(b"smoke-ok"))

# MQTT 5 delivery. Each MQTT 5 PUBLISH has a property length, also when
# there are no properties. The check compares the full packet.
def mqtt5_client(client_id):
    sock = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
    sock.sendall(packet(0x10, b"\x00\x04MQTT\x05\x02\x00\x3c\x00" + field(client_id)))
    ack = receive(sock, 5)
    return sock, len(ack) >= 4 and ack[0] == 0x20 and ack[3] == 0

def subscribe5(sock, topic):
    sock.sendall(packet(0x82, b"\x00\x02\x00" + field(topic) + b"\x00"))
    ack = receive(sock, 5)
    return len(ack) >= 6 and ack[0] == 0x90 and ack[4] == 0 and ack[5] == 0

v5_topic = b"smoke/v5"
v5_token = os.urandom(8).hex().encode()
v5_sub, v5_ok = mqtt5_client(b"smoke-v5-sub")
v5_ready = v5_ok and subscribe5(v5_sub, v5_topic)
# The publisher sets the retain flag, thus the broker keeps the message.
pub.sendall(packet(0x31, field(v5_topic) + v5_token))
report("an MQTT 5 subscriber gets a publish with an empty property section",
       v5_ready and receive(v5_sub, 5) == packet(0x30, field(v5_topic) + b"\x00" + v5_token))
v5_late, v5_late_ok = mqtt5_client(b"smoke-v5-late")
v5_late.sendall(packet(0x82, b"\x00\x02\x00" + field(v5_topic) + b"\x00"))
v5_replay = b""
for _ in range(2):
    v5_replay += receive(v5_late, 5)
    if v5_token in v5_replay:
        break
report("a new MQTT 5 subscription gets the retained message with the retain flag",
       v5_late_ok and v5_replay.endswith(packet(0x31, field(v5_topic) + b"\x00" + v5_token)))

# Tenants. The payload is random, thus the broker cannot know it.
def tenant_client(client_id, username):
    sock = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
    return sock, mqtt_connect(sock, client_id, username)

def subscribe(sock, topic):
    sock.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    return sock.recv(5)[:1] == b"\x90"

# Fan-out. One publish to many subscribers must reach each of them.
# The count is larger than one write batch of the kernel for each link,
# thus a kernel that writes one batch and then waits fails this check.
import select
fan_topic = b"smoke/fan"
fan_token = os.urandom(6).hex().encode()
fan_socks = []
for index in range(400):
    fan_sock = socket.create_connection(("127.0.0.1", mqtt_port), timeout=5)
    if mqtt_connect(fan_sock, b"smoke-fan-%d" % index) and subscribe(fan_sock, fan_topic):
        fan_socks.append(fan_sock)
FAN_PUBLISHES = 4
for _ in range(FAN_PUBLISHES):
    pub.sendall(packet(0x30, field(fan_topic) + fan_token))
fan_seen = {s: b"" for s in fan_socks}
fan_deadline = time.time() + 10
def fan_complete():
    return all(data.count(fan_token) >= FAN_PUBLISHES for data in fan_seen.values())
while time.time() < fan_deadline and not fan_complete():
    readable, _, _ = select.select(fan_socks, [], [], 0.5)
    for s in readable:
        try:
            fan_seen[s] += s.recv(4096)
        except OSError:
            pass
fan_got = sum(data.count(fan_token) for data in fan_seen.values())
report("each of 400 subscribers gets each of 4 publishes (%d of %d deliveries)" % (fan_got, 400 * FAN_PUBLISHES),
       len(fan_socks) == 400 and fan_complete())
for s in fan_socks:
    s.close()

secret = base64.b16encode(os.urandom(8))
a_sub, a_ok = tenant_client(b"smoke-a-sub", b"alpha")
b_sub, b_ok = tenant_client(b"smoke-b-sub", b"beta")
a_pub, _ = tenant_client(b"smoke-a-pub", b"alpha")
topic = b"smoke/tenant"
ready = a_ok and b_ok and subscribe(a_sub, topic) and subscribe(b_sub, topic)
a_pub.sendall(packet(0x30, field(topic) + secret))
in_tenant = receive(a_sub, 5)
other_tenant = receive(b_sub, 2)
report("a subscriber gets the publish of a client in the same tenant",
       ready and in_tenant.endswith(secret))
report("a subscriber does not get the publish of a client in a different tenant",
       ready and secret not in other_tenant)

# Same client id in two tenants. Both connects succeed.
# Both sockets stay open. A takeover between tenants fails.
a_dup, a_dup_ok = tenant_client(b"smoke-dup", b"alpha")
b_dup, b_dup_ok = tenant_client(b"smoke-dup", b"beta")
b_pub, _ = tenant_client(b"smoke-b-pub", b"beta")
dup_topic = b"smoke/dup"
dup_ready = (a_dup_ok and b_dup_ok
             and subscribe(a_dup, dup_topic) and subscribe(b_dup, dup_topic))
a_pub.sendall(packet(0x30, field(dup_topic) + secret))
a_dup_got = receive(a_dup, 5)
b_dup_got = receive(b_dup, 2)
report("two clients with the same client id stay connected in two tenants",
       dup_ready and a_dup_got.endswith(secret) and secret not in b_dup_got)
b_pub.sendall(packet(0x30, field(dup_topic) + secret))
b_dup_got2 = receive(b_dup, 5)
a_dup_got2 = receive(a_dup, 2)
report("a takeover between tenants does not happen",
       dup_ready and b_dup_got2.endswith(secret) and secret not in a_dup_got2)

def upgrade(path):
    sock = wait_port(ws_port)
    key = base64.b64encode(os.urandom(16)).decode()
    sock.sendall((f"GET {path} HTTP/1.1\r\nHost: smoke\r\nUpgrade: websocket\r\n"
                  f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
                  "Sec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n").encode())
    return sock.recv(200).split(b"\r\n")[0]

report("listeners.ws.path from indra.toml is the WebSocket path in operation",
       b"101" in upgrade("/smoke") and b"404" in upgrade("/mqtt"))
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

# --- 4b. Tenant filters on the client list keep tenants apart ---
# Run this check before the first user exists. The broker stays open.
# The tenant rule maps each username to its own tenant. The same
# client id connects in both tenants. A publish uses a random value the
# broker cannot know. Each tenant filter of the client list shows its
# own client only. The unfiltered list shows both namesakes with two
# tenant ids.
python3 - "$MQTT_PORT" "$API_PORT" "$API_KEY_GOOD" <<'PY'
import json, os, socket, struct, sys, urllib.request
mqtt_port, api_port, api_key = sys.argv[1], sys.argv[2], sys.argv[3]
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    import time
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", int(port)), timeout=5)
        except OSError:
            time.sleep(0.1)
    raise SystemExit("FAIL the edge does not listen")
def mqtt_connect(sock, client_id, username):
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(b"pw")
    sock.sendall(packet(0x10, body))
    return sock.recv(4) == b"\x20\x02\x00\x00"
def subscribe(sock, topic):
    sock.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    return sock.recv(5)[:1] == b"\x90"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
def api_get(path):
    req = urllib.request.Request(
        f"http://127.0.0.1:{api_port}{path}",
        headers={"Authorization": f"Bearer {api_key}"})
    with urllib.request.urlopen(req) as res:
        return res.status, json.loads(res.read().decode())
def rows_of(tenant):
    status, body = api_get(f"/api/v5/clients?tenant={tenant}")
    assert status == 200, f"client list of {tenant}: {status}"
    return body["data"]
topic = b"smoke/filter/check"
secret = os.urandom(8).hex().encode()
a_sock = wait_port(mqtt_port)
a_ok = mqtt_connect(a_sock, b"smoke-filter-dup", b"alpha")
b_sock = wait_port(mqtt_port)
b_ok = mqtt_connect(b_sock, b"smoke-filter-dup", b"beta")
a_pub = wait_port(mqtt_port)
a_pub_ok = mqtt_connect(a_pub, b"smoke-filter-pub", b"alpha")
ready = a_ok and b_ok and a_pub_ok and subscribe(a_sock, topic) and subscribe(b_sock, topic)
if ready:
    a_pub.sendall(packet(0x30, field(topic) + secret))
    report("a subscriber gets the publish of its own tenant before the read check",
           receive(a_sock, 5).endswith(secret))
    report("a subscriber does not get the publish of another tenant before the read check",
           secret not in receive(b_sock, 2))
else:
    report("a subscriber gets the publish of its own tenant before the read check", False)
    report("a subscriber does not get the publish of another tenant before the read check", False)
rows_a = rows_of("t-alpha")
rows_b = rows_of("t-beta")
report("the tenant filter of the first tenant shows its client only",
       all(row.get("mountpoint") == "t-alpha" for row in rows_a)
       and any(row.get("clientid") == "smoke-filter-dup" for row in rows_a))
report("the tenant filter of the second tenant shows its client only",
       all(row.get("mountpoint") == "t-beta" for row in rows_b)
       and any(row.get("clientid") == "smoke-filter-dup" for row in rows_b))
status, body = api_get("/api/v5/clients")
mounts = sorted(row.get("mountpoint") for row in body["data"]
                if row.get("clientid") == "smoke-filter-dup")
report("the unfiltered list shows the same client id with two tenant ids",
       status == 200 and mounts == ["t-alpha", "t-beta"])
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

# --- 10. Explicit bind sections keep tenants apart ---
# Run this check before the first user exists. The broker stays open.
# Unknown users connect. The check proves isolation on the new path.
# Each connect uses the new bind path. The edge sets the section
# flags. Each publish uses a random value. Delivery stays in-tenant.
# The default tenant still delivers as before.
python3 - "$MQTT_PORT" <<'PY'
import os, socket, struct, sys
mqtt_port = sys.argv[1]
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    import time
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", int(port)), timeout=2)
        except OSError:
            time.sleep(0.1)
    raise SystemExit(f"FAIL the edge does not listen on port {port}")
def mqtt_connect(sock, client_id, username=None):
    if username is None:
        body = b"\x00\x04MQTT\x04\x02\x00\x3c" + field(client_id)
    else:
        body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(b"pw")
    sock.sendall(packet(0x10, body))
    return sock.recv(4) == b"\x20\x02\x00\x00"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
def wait_port2(port):
    import time
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", int(port)), timeout=5)
        except OSError:
            time.sleep(0.1)
    raise SystemExit("FAIL the edge does not listen")
def connect(client_id, username=None):
    sock = wait_port2(mqtt_port)
    return sock, mqtt_connect(sock, client_id, username)
def subscribe(sock, topic):
    sock.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    return sock.recv(5)[:1] == b"\x90"
secret = os.urandom(8).hex().encode()
topic = b"smoke/bind/check"
a_sub, a_ok = connect(b"bind-a-sub", b"bind-a")
b_sub, b_ok = connect(b"bind-b-sub", b"bind-b")
a_pub, a_pub_ok = connect(b"bind-a-pub", b"bind-a")
ready = a_ok and b_ok and a_pub_ok and subscribe(a_sub, topic) and subscribe(b_sub, topic)
if ready:
    a_pub.sendall(packet(0x30, field(topic) + secret))
    report("a subscriber gets the publish of its own tenant through the new bind path",
           receive(a_sub, 5).endswith(secret))
    report("a subscriber does not get the publish of another tenant through the new bind path",
           secret not in receive(b_sub, 2))
else:
    report("a subscriber gets the publish of its own tenant through the new bind path", False)
    report("a subscriber does not get the publish of another tenant through the new bind path", False)
anon_secret = os.urandom(8).hex().encode()
anon_sub, anon_ok = connect(b"bind-anon-sub")
anon_pub, anon_pub_ok = connect(b"bind-anon-pub")
anon_ready = anon_ok and anon_pub_ok
if anon_ready:
    anon_ready = subscribe(anon_sub, topic)
if anon_ready:
    anon_pub.sendall(packet(0x30, field(topic) + anon_secret))
    report("the default tenant still delivers without credentials",
           receive(anon_sub, 5).endswith(anon_secret))
else:
    report("the default tenant still delivers without credentials", False)
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

# --- 5. Users and ACLs of each tenant stay after a restart ---
# The tenant rule maps each username to its own tenant. The password is
# random, thus the broker cannot know it. The broker restarts on the
# same data directory. The same password must still open access, and a
# wrong password must still fail. A publish must still reach the
# subscriber in the same tenant, which proves the ACLs stayed too.
KEEP_USER="smoke-keep"
KEEP_TENANT="t-smoke-keep"
KEEP_PASS=$(python3 -c 'import os; print(os.urandom(12).hex())')
KEEP_TOPIC="smoke/keep/check"
python3 - "$API_PORT" "$API_KEY_GOOD" "$KEEP_TENANT" "$KEEP_USER" "$KEEP_PASS" <<'PY'
import json, sys, urllib.request
api, key, tenant, user, password = sys.argv[1:6]
def call(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{api}{path}", method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    with urllib.request.urlopen(req) as res:
        return res.status, res.read().decode()
status, _ = call("POST", f"/api/v1/auth/users?tenant={tenant}",
                 {"username": user, "password": password})
assert status == 201, f"create per-tenant user: {status}"
for action in ("publish", "subscribe"):
    status, _ = call("POST", f"/api/v1/auth/acls?tenant={tenant}",
                     {"client_pattern": "*", "action": action,
                      "topic_pattern": "smoke/keep/#", "allow": True})
    assert status == 201, f"create per-tenant {action} rule: {status}"
print("per-tenant user and ACLs are stored")
PY
check "a per-tenant user with a random password is stored through the API" $?

# Restart the kernel and the edge on the same data directory.
kill "$KERNEL_PID" "$EDGE_PID" 2>/dev/null
wait 2>/dev/null
sleep 0.5
INDRA_API_KEYS=$API_KEY_GOOD INDRA_API_KEY=$API_KEY_GOOD \
    "$KERNEL" --config-dir "$ROOT/cfg" > "$ROOT/kernel2.log" 2>&1 &
KERNEL_PID=$!
healthz=
for _ in $(seq 1 50); do
    healthz=$(python3 - "$API_PORT" <<'PY' 2>/dev/null
import sys, urllib.request
print(urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/healthz", timeout=1).read().decode())
PY
)
    [ "$healthz" = OK ] && break
    kill -0 "$KERNEL_PID" 2>/dev/null || break
    sleep 0.2
done
[ "$healthz" = OK ]
check "the kernel restarts on the same data directory" $?
(cd "$ROOT" && exec erl -noshell -noinput -pa "$EBIN" "${EDGE_ARGS[@]}" \
    -eval '{ok, _} = application:ensure_all_started(indra_edge), timer:sleep(infinity).' \
    > "$ROOT/edge2.log" 2>&1) &
EDGE_PID=$!

python3 - "$MQTT_PORT" "$KEEP_USER" "$KEEP_PASS" "$KEEP_TOPIC" <<'PY'
import socket, struct, sys, time
mqtt_port, username, password, topic = sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode(), sys.argv[4].encode()
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=2)
        except OSError:
            time.sleep(0.1)
    raise SystemExit(f"FAIL the edge does not listen on port {port} after the restart")
def connect(client_id, username, password):
    sock = wait_port(mqtt_port)
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(password)
    sock.sendall(packet(0x10, body))
    return sock, sock.recv(4) == b"\x20\x02\x00\x00"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
sub, ok = connect(b"keep-sub", username, password)
report("the kept password still opens access after the restart", ok)
wrong, wrong_ok = connect(b"keep-wrong", username, b"wrong-password")
report("a wrong password still fails after the restart", not wrong_ok)
ready = ok
if ok:
    sub.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    ready = sub.recv(5)[:1] == b"\x90"
    pub, pub_ok = connect(b"keep-pub", username, password)
    ready = ready and pub_ok
    if ready:
        pub.sendall(packet(0x30, field(topic) + b"keep-ok"))
        report("a publish still reaches the subscriber in the same tenant after the restart",
               receive(sub, 5).endswith(b"keep-ok"))
    else:
        report("a publish still reaches the subscriber in the same tenant after the restart", False)
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

# --- 6. A tenant from the management API carries a real client ---
# The tenant rule maps each username to its own tenant. Each namespace
# is created through POST /mt/ns/{ns}. Each user is created through the
# API with a random password the broker cannot know. A publish in one
# tenant reaches the subscriber in the same tenant only.
MT_USER_A="mtapi"
MT_NS_A="t-mtapi"
MT_USER_B="mtother"
MT_NS_B="t-mtother"
MT_PASS_A=$(python3 -c 'import os; print(os.urandom(12).hex())')
MT_PASS_B=$(python3 -c 'import os; print(os.urandom(12).hex())')
MT_TOPIC="smoke/mt/check"
python3 - "$API_PORT" "$API_KEY_GOOD" "$MT_NS_A" "$MT_USER_A" "$MT_PASS_A" "$MT_NS_B" "$MT_USER_B" "$MT_PASS_B" <<'PY'
import json, sys, urllib.request
api, key, ns_a, user_a, pass_a, ns_b, user_b, pass_b = sys.argv[1:9]
def call(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{api}{path}", method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, res.read().decode()
    except urllib.request.HTTPError as err:
        return err.code, err.read().decode()
for ns, user, password in ((ns_a, user_a, pass_a), (ns_b, user_b, pass_b)):
    status, _ = call("POST", f"/api/v5/mt/ns/{ns}", {})
    assert status in (204, 409), f"create managed namespace {ns}: {status}"
    status, _ = call("POST", f"/api/v1/auth/users?tenant={ns}",
                     {"username": user, "password": password})
    assert status in (201, 409), f"create tenant user {user}: {status}"
    for action in ("publish", "subscribe"):
        status, _ = call("POST", f"/api/v1/auth/acls?tenant={ns}",
                         {"client_pattern": "*", "action": action,
                          "topic_pattern": "smoke/mt/#", "allow": True})
        assert status == 201, f"create tenant {action} rule: {status}"
    status, text = call("GET", f"/api/v5/mt/ns/{ns}/config")
    assert status == 200, f"show managed namespace {ns}: {status} {text}"
print("managed namespaces and tenant users are stored through the API")
PY
check "two tenants and two users with random passwords are stored through the API" $?

python3 - "$MQTT_PORT" "$MT_USER_A" "$MT_PASS_A" "$MT_USER_B" "$MT_PASS_B" "$MT_TOPIC" <<'PY'
import socket, struct, sys, time
mqtt_port, username_a, password_a, username_b, password_b, topic = (
    sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode(),
    sys.argv[4].encode(), sys.argv[5].encode(), sys.argv[6].encode())
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=2)
        except OSError:
            time.sleep(0.1)
    raise SystemExit(f"FAIL the edge does not listen on port {port}")
def connect(client_id, username, password):
    sock = wait_port(mqtt_port)
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(password)
    sock.sendall(packet(0x10, body))
    return sock, sock.recv(4) == b"\x20\x02\x00\x00"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
import os
secret = os.urandom(8).hex().encode()
sub, ok = connect(b"mtapi-sub", username_a, password_a)
other, other_ok = connect(b"mtapi-other", username_b, password_b)
ready = ok and other_ok
if ok:
    sub.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    ready = ready and sub.recv(5)[:1] == b"\x90"
if other_ok:
    other.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    ready = ready and other.recv(5)[:1] == b"\x90"
pub, pub_ok = connect(b"mtapi-pub", username_a, password_a)
ready = ready and pub_ok
if ready:
    pub.sendall(packet(0x30, field(topic) + secret))
    report("a publish in the API tenant reaches its own subscriber",
           receive(sub, 5).endswith(secret))
    report("a publish in the API tenant does not leak to another tenant",
           secret not in receive(other, 2))
else:
    report("a publish in the API tenant reaches its own subscriber", False)
    report("a publish in the API tenant does not leak to another tenant", False)
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

# --- 7. Tenant commands of the operator CLI ---
# Two tenants come from `indra ctl tenants create`. Each tenant gets
# one user with a random password the broker cannot know, plus publish
# and subscribe rules from `indra ctl acls create`. Real clients
# connect in each tenant. A publish in one tenant reaches its own
# subscriber only.
CLI_NS_A="t-cli-a"
CLI_NS_B="t-cli-b"
CLI_USER_A="cli-a"
CLI_USER_B="cli-b"
CLI_PASS_A=$(python3 -c 'import os; print(os.urandom(12).hex())')
CLI_PASS_B=$(python3 -c 'import os; print(os.urandom(12).hex())')
CLI_TOPIC="smoke/cli/check"
CTL_API="http://127.0.0.1:$API_PORT"
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" tenants create "$CLI_NS_A" > /dev/null
check "indra ctl tenants create makes the first tenant" $?
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" tenants create "$CLI_NS_B" > /dev/null
check "indra ctl tenants create makes the second tenant" $?
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" tenants list | grep -q "$CLI_NS_A"
check "indra ctl tenants list shows the first tenant" $?
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" tenants show "$CLI_NS_B" | grep -q "namespace: $CLI_NS_B"
check "indra ctl tenants show reads the second tenant" $?
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" users create --username "$CLI_USER_A" --tenant "$CLI_NS_A" --password "$CLI_PASS_A" > /dev/null
check "indra ctl users create makes a user in the first tenant" $?
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" users create --username "$CLI_USER_B" --tenant "$CLI_NS_B" --password "$CLI_PASS_B" > /dev/null
check "indra ctl users create makes a user in the second tenant" $?
for action in publish subscribe; do
    "$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" acls create --tenant "$CLI_NS_A" --client-pattern '*' --action "$action" --topic-pattern 'smoke/cli/#' --allow > /dev/null
    check "indra ctl acls create adds the $action rule in the first tenant" $?
    "$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" acls create --tenant "$CLI_NS_B" --client-pattern '*' --action "$action" --topic-pattern 'smoke/cli/#' --allow > /dev/null
    check "indra ctl acls create adds the $action rule in the second tenant" $?
done
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" acls list --tenant "$CLI_NS_A" | grep -q publish
check "indra ctl acls list shows the rules of the first tenant" $?

python3 - "$MQTT_PORT" "$CLI_USER_A" "$CLI_PASS_A" "$CLI_USER_B" "$CLI_PASS_B" "$CLI_TOPIC" <<'PY'
import os, socket, struct, sys
mqtt_port, username_a, password_a, username_b, password_b, topic = (
    sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode(),
    sys.argv[4].encode(), sys.argv[5].encode(), sys.argv[6].encode())
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=2)
        except OSError:
            import time
            time.sleep(0.1)
    raise SystemExit(f"FAIL the edge does not listen on port {port}")
def connect(client_id, username, password):
    sock = wait_port(mqtt_port)
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(password)
    sock.sendall(packet(0x10, body))
    return sock, sock.recv(4) == b"\x20\x02\x00\x00"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
secret = os.urandom(8).hex().encode()
sub, ok = connect(b"cli-a-sub", username_a, password_a)
other, other_ok = connect(b"cli-b-sub", username_b, password_b)
ready = ok and other_ok
if ok:
    sub.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    ready = ready and sub.recv(5)[:1] == b"\x90"
if other_ok:
    other.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    ready = ready and other.recv(5)[:1] == b"\x90"
pub, pub_ok = connect(b"cli-a-pub", username_a, password_a)
ready = ready and pub_ok
if ready:
    pub.sendall(packet(0x30, field(topic) + secret))
    report("a publish through CLI users reaches its own subscriber",
           receive(sub, 5).endswith(secret))
    report("a publish through CLI users does not leak to another tenant",
           secret not in receive(other, 2))
else:
    report("a publish through CLI users reaches its own subscriber", False)
    report("a publish through CLI users does not leak to another tenant", False)
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" tenants delete "$CLI_NS_B" --yes > /dev/null
check "indra ctl tenants delete removes the second tenant" $?
if "$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" tenants list | grep -q "$CLI_NS_B"; then
    fail "the deleted tenant stays out of indra ctl tenants list"
else
    pass "the deleted tenant stays out of indra ctl tenants list"
fi

# --- 8. A rule of one tenant fires only for that tenant ---
# Two tenants come from the API. One rule of the first tenant
# republishes. A random value the broker cannot know is published in
# each tenant. The republished message arrives in the first tenant
# only. A publish in the second tenant fires no rule.
RULE_NS_A="t-rule-a"
RULE_NS_B="t-rule-b"
RULE_USER_A="rule-a"
RULE_USER_B="rule-b"
RULE_PASS_A=$(python3 -c 'import os; print(os.urandom(12).hex())')
RULE_PASS_B=$(python3 -c 'import os; print(os.urandom(12).hex())')
RULE_IN="smoke/rule/in"
RULE_OUT="smoke/rule/out"
python3 - "$API_PORT" "$API_KEY_GOOD" "$RULE_NS_A" "$RULE_USER_A" "$RULE_PASS_A" "$RULE_NS_B" "$RULE_USER_B" "$RULE_PASS_B" "$RULE_IN" "$RULE_OUT" <<'PY'
import json, sys, urllib.request
api, key, ns_a, user_a, pass_a, ns_b, user_b, pass_b, rule_in, rule_out = sys.argv[1:11]
def call(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{api}{path}", method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, res.read().decode()
    except urllib.request.HTTPError as err:
        return err.code, err.read().decode()
for ns, user, password in ((ns_a, user_a, pass_a), (ns_b, user_b, pass_b)):
    status, _ = call("POST", f"/api/v5/mt/ns/{ns}", {})
    assert status in (204, 409), f"create managed namespace {ns}: {status}"
    status, _ = call("POST", f"/api/v1/auth/users?tenant={ns}",
                     {"username": user, "password": password})
    assert status in (201, 409), f"create tenant user {user}: {status}"
    for action in ("publish", "subscribe"):
        status, _ = call("POST", f"/api/v1/auth/acls?tenant={ns}",
                         {"client_pattern": "*", "action": action,
                          "topic_pattern": "smoke/rule/#", "allow": True})
        assert status == 201, f"create tenant {action} rule: {status}"
status, text = call("POST", "/api/v1/rules",
                    {"name": "smoke-tenant-rule", "topic_filter": rule_in,
                     "tenant": ns_a,
                     "actions": [{"type": "republish", "topic": rule_out, "qos": 0}]})
assert status == 201, f"create tenant rule: {status} {text}"
assert json.loads(text)["tenant"] == ns_a, f"rule keeps its tenant: {text}"
print("tenant rule is stored through the API")
PY
check "a rule of the first tenant with a republish action is stored through the API" $?

python3 - "$MQTT_PORT" "$RULE_USER_A" "$RULE_PASS_A" "$RULE_USER_B" "$RULE_PASS_B" "$RULE_IN" "$RULE_OUT" <<'PY'
import os, socket, struct, sys
mqtt_port, username_a, password_a, username_b, password_b, rule_in, rule_out = (
    sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode(),
    sys.argv[4].encode(), sys.argv[5].encode(), sys.argv[6].encode(), sys.argv[7].encode())
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=2)
        except OSError:
            import time
            time.sleep(0.1)
    raise SystemExit(f"FAIL the edge does not listen on port {port}")
def connect(client_id, username, password):
    sock = wait_port(mqtt_port)
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(password)
    sock.sendall(packet(0x10, body))
    return sock, sock.recv(4) == b"\x20\x02\x00\x00"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
secret_a = os.urandom(8).hex().encode()
secret_b = os.urandom(8).hex().encode()
sub_a, ok_a = connect(b"rule-a-sub", username_a, password_a)
sub_b, ok_b = connect(b"rule-b-sub", username_b, password_b)
ready = ok_a and ok_b
if ok_a:
    sub_a.sendall(packet(0x82, b"\x00\x01" + field(rule_out) + b"\x00"))
    ready = ready and sub_a.recv(5)[:1] == b"\x90"
if ok_b:
    sub_b.sendall(packet(0x82, b"\x00\x01" + field(rule_out) + b"\x00"))
    ready = ready and sub_b.recv(5)[:1] == b"\x90"
pub_a, pub_a_ok = connect(b"rule-a-pub", username_a, password_a)
pub_b, pub_b_ok = connect(b"rule-b-pub", username_b, password_b)
ready = ready and pub_a_ok and pub_b_ok
if ready:
    pub_a.sendall(packet(0x30, field(rule_in) + secret_a))
    report("the rule output arrives in the tenant of the rule",
           receive(sub_a, 5).endswith(secret_a))
    report("the rule output does not leak to the other tenant",
           secret_a not in receive(sub_b, 2))
    pub_b.sendall(packet(0x30, field(rule_in) + secret_b))
    report("a publish in the tenant without the rule fires no output there",
           secret_b not in receive(sub_b, 2))
    report("a publish in the tenant without the rule sends nothing to the rule tenant",
           secret_b not in receive(sub_a, 2))
else:
    report("the rule output arrives in the tenant of the rule", False)
    report("the rule output does not leak to the other tenant", False)
    report("a publish in the tenant without the rule fires no output there", False)
    report("a publish in the tenant without the rule sends nothing to the rule tenant", False)
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

# --- 9. A full tenant keeps service for other tenants ---
# Two tenants come from the API. The first tenant sets a low
# connection limit. The limit fills with real clients. A client in
# the second tenant still connects and publishes a random value.
LIM_NS_A="t-lim-a"
LIM_NS_B="t-lim-b"
LIM_USER_A="lim-a"
LIM_USER_B="lim-b"
LIM_PASS_A=$(python3 -c 'import os; print(os.urandom(12).hex())')
LIM_PASS_B=$(python3 -c 'import os; print(os.urandom(12).hex())')
LIM_TOPIC="smoke/lim/check"
python3 - "$API_PORT" "$API_KEY_GOOD" "$LIM_NS_A" "$LIM_USER_A" "$LIM_PASS_A" "$LIM_NS_B" "$LIM_USER_B" "$LIM_PASS_B" <<'PY'
import json, sys, urllib.request
api, key, ns_a, user_a, pass_a, ns_b, user_b, pass_b = sys.argv[1:9]
def call(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{api}{path}", method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, res.read().decode()
    except urllib.request.HTTPError as err:
        return err.code, err.read().decode()
for ns, user, password in ((ns_a, user_a, pass_a), (ns_b, user_b, pass_b)):
    status, _ = call("POST", f"/api/v5/mt/ns/{ns}", {})
    assert status in (204, 409), f"create managed namespace {ns}: {status}"
    status, _ = call("POST", f"/api/v1/auth/users?tenant={ns}",
                     {"username": user, "password": password})
    assert status in (201, 409), f"create tenant user {user}: {status}"
    for action in ("publish", "subscribe"):
        status, _ = call("POST", f"/api/v1/auth/acls?tenant={ns}",
                         {"client_pattern": "*", "action": action,
                          "topic_pattern": "smoke/lim/#", "allow": True})
        assert status == 201, f"create tenant {action} rule: {status}"
status, _ = call("PUT", f"/api/v5/mt/ns/{ns_a}/config",
                 {"limits": {"max_connections": 1}})
assert status == 200, f"set per-tenant connection limit: {status}"
print("per-tenant connection limit is stored through the API")
PY
check "a low connection limit for one tenant is stored through the API" $?

python3 - "$MQTT_PORT" "$LIM_USER_A" "$LIM_PASS_A" "$LIM_USER_B" "$LIM_PASS_B" "$LIM_TOPIC" <<'PY'
import os, socket, struct, sys
mqtt_port, username_a, password_a, username_b, password_b, topic = (
    sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode(),
    sys.argv[4].encode(), sys.argv[5].encode(), sys.argv[6].encode())
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", port), timeout=2)
        except OSError:
            import time
            time.sleep(0.1)
    raise SystemExit(f"FAIL the edge does not listen on port {port}")
def connect(client_id, username, password):
    sock = wait_port(mqtt_port)
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(password)
    sock.sendall(packet(0x10, body))
    return sock, sock.recv(4) == b"\x20\x02\x00\x00"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
secret = os.urandom(8).hex().encode()
first, first_ok = connect(b"lim-a-first", username_a, password_a)
second, second_ok = connect(b"lim-a-second", username_a, password_a)
report("a tenant at its connection limit refuses a new connection", first_ok and not second_ok)
sub, sub_ok = connect(b"lim-b-sub", username_b, password_b)
ready = sub_ok
if sub_ok:
    sub.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    ready = ready and sub.recv(5)[:1] == b"\x90"
pub, pub_ok = connect(b"lim-b-pub", username_b, password_b)
ready = ready and pub_ok
if ready:
    pub.sendall(packet(0x30, field(topic) + secret))
    report("the other tenant still publishes a random value", receive(sub, 5).endswith(secret))
else:
    report("the other tenant still publishes a random value", False)
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

# --- 11. Listener scope and client certificate defaults through the socket ---
# The broker starts from indra.toml with listener-scoped tenant rules.
# The default rule is first, so edge clients use the per-username
# rule. Each tenant gets one user with a random password the broker
# cannot know, plus publish and subscribe rules. Real clients connect
# in each tenant. A publish in one tenant reaches its own subscriber
# only. The check also proves the verify switches stay off by default,
# so the broker requests no client certificate.
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" --json \
    config explain --key listeners.tls.verify_client_cert 2>/dev/null | grep -q 'false'
check "explain shows listeners.tls.verify_client_cert stays off by default" $?
"$CTL" ctl --endpoint "$CTL_API" --api-key "$API_KEY_GOOD" --json \
    config explain --key listeners.wss.verify_client_cert 2>/dev/null | grep -q 'false'
check "explain shows listeners.wss.verify_client_cert stays off by default" $?
LIS_NS_A="t-lis-a"
LIS_NS_B="t-lis-b"
LIS_USER_A="lis-a"
LIS_USER_B="lis-b"
LIS_PASS_A=$(python3 -c 'import os; print(os.urandom(12).hex())')
LIS_PASS_B=$(python3 -c 'import os; print(os.urandom(12).hex())')
LIS_TOPIC="smoke/listener/check"
python3 - "$API_PORT" "$API_KEY_GOOD" "$LIS_NS_A" "$LIS_USER_A" "$LIS_PASS_A" "$LIS_NS_B" "$LIS_USER_B" "$LIS_PASS_B" <<'PY'
import json, sys, urllib.request
api, key, ns_a, user_a, pass_a, ns_b, user_b, pass_b = sys.argv[1:9]
def call(method, path, body=None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{api}{path}", method=method,
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        data=json.dumps(body).encode() if body is not None else None)
    try:
        with urllib.request.urlopen(req) as res:
            return res.status, res.read().decode()
    except urllib.request.HTTPError as err:
        return err.code, err.read().decode()
for ns, user, password in ((ns_a, user_a, pass_a), (ns_b, user_b, pass_b)):
    status, _ = call("POST", f"/api/v5/mt/ns/{ns}", {})
    assert status in (204, 409), f"create managed namespace {ns}: {status}"
    status, _ = call("POST", f"/api/v1/auth/users?tenant={ns}",
                     {"username": user, "password": password})
    assert status in (201, 409), f"create tenant user {user}: {status}"
    for action in ("publish", "subscribe"):
        status, _ = call("POST", f"/api/v1/auth/acls?tenant={ns}",
                         {"client_pattern": "*", "action": action,
                          "topic_pattern": "smoke/listener/#", "allow": True})
        assert status == 201, f"create tenant {action} rule: {status}"
print("listener tenants and users are stored through the API")
PY
check "two listener tenants and users with random passwords are stored through the API" $?
python3 - "$MQTT_PORT" "$LIS_USER_A" "$LIS_PASS_A" "$LIS_USER_B" "$LIS_PASS_B" "$LIS_TOPIC" <<'PY'
import os, socket, struct, sys
mqtt_port, username_a, password_a, username_b, password_b, topic = (
    sys.argv[1], sys.argv[2].encode(), sys.argv[3].encode(),
    sys.argv[4].encode(), sys.argv[5].encode(), sys.argv[6].encode())
failures = 0
def report(name, good):
    global failures
    print(("ok   " if good else "FAIL ") + name)
    failures += 0 if good else 1
def packet(kind, body):
    return bytes([kind, len(body)]) + body
def field(data):
    return struct.pack(">H", len(data)) + data
def wait_port(port):
    import time
    for _ in range(100):
        try:
            return socket.create_connection(("127.0.0.1", int(port)), timeout=5)
        except OSError:
            time.sleep(0.1)
    raise SystemExit("FAIL the edge does not listen")
def mqtt_connect(sock, client_id, username, password):
    body = b"\x00\x04MQTT\x04\xc2\x00\x3c" + field(client_id) + field(username) + field(password)
    sock.sendall(packet(0x10, body))
    return sock.recv(4) == b"\x20\x02\x00\x00"
def receive(sock, seconds):
    sock.settimeout(seconds)
    try:
        return sock.recv(200)
    except OSError:
        return b""
def subscribe(sock, topic):
    sock.sendall(packet(0x82, b"\x00\x01" + field(topic) + b"\x00"))
    return sock.recv(5)[:1] == b"\x90"
secret = os.urandom(8).hex().encode()
a_sub = wait_port(mqtt_port)
a_ok = mqtt_connect(a_sub, b"lis-a-sub", username_a, password_a)
b_sub = wait_port(mqtt_port)
b_ok = mqtt_connect(b_sub, b"lis-b-sub", username_b, password_b)
a_pub = wait_port(mqtt_port)
a_pub_ok = mqtt_connect(a_pub, b"lis-a-pub", username_a, password_a)
ready = a_ok and b_ok and a_pub_ok and subscribe(a_sub, topic) and subscribe(b_sub, topic)
if ready:
    a_pub.sendall(packet(0x30, field(topic) + secret))
    report("a subscriber gets the publish of its own tenant with listener rules present",
           receive(a_sub, 5).endswith(secret))
    report("a subscriber does not get the publish of another tenant with listener rules present",
           secret not in receive(b_sub, 2))
else:
    report("a subscriber gets the publish of its own tenant with listener rules present", False)
    report("a subscriber does not get the publish of another tenant with listener rules present", False)
sys.exit(1 if failures else 0)
PY
[ $? = 0 ] || failures=$((failures + 1))

if [ "$failures" = 0 ]; then
    echo "boot smoke: all checks are satisfactory"
    exit 0
fi
echo "boot smoke: $failures check(s) failed"
echo "--- kernel log"; tail -15 "$ROOT/kernel.log"
echo "--- edge log"; tail -15 "$ROOT/edge.log" 2>/dev/null
exit 1
