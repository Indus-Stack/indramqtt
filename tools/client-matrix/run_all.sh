#!/usr/bin/env bash
# Run the full client matrix against a broker this script starts.
#
# Use:
#   bash run_all.sh [out-dir]
#
# The script starts the kernel from an indra.toml file it writes, sets
# MATRIX_RESTART_CMD so each library can run the restart check, runs
# every library runner, and writes one RESULT line per check into
# out-dir/results-lines.txt. It stops the broker at the end.
#
# Entries the operator may set before the run:
#   MATRIX_KERNEL   path to the indramqtt binary
#   MATRIX_CA       path to a CA certificate for TLS checks
#   MATRIX_API_KEY  management API key (default: generated here)
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
TREE=$(cd "$HERE/../.." && pwd)
OUT=${1:-$HERE/out}
KERNEL=${MATRIX_KERNEL:-/work/target/debug/indramqtt}
API_KEY=${MATRIX_API_KEY:-matrix-run-key-00000001}
if [ -z "${MATRIX_CA:-}" ] || [ -z "${MATRIX_TLS_CERT:-}" ] || [ -z "${MATRIX_TLS_KEY:-}" ]; then
    MATRIX_PKI_DIR=$(mktemp -d)
    bash "$TREE/tools/gen_test_pki.sh" "$MATRIX_PKI_DIR"
fi
CA_FILE=${MATRIX_CA:-$MATRIX_PKI_DIR/ca-cert.pem}
TLS_CERT=${MATRIX_TLS_CERT:-$MATRIX_PKI_DIR/server-cert.pem}
TLS_KEY=${MATRIX_TLS_KEY:-$MATRIX_PKI_DIR/server-key.pem}
# Resolve to absolute paths; the kernel rejects a cert path with `..`.
CA_FILE=$(readlink -f "$CA_FILE")
TLS_CERT=$(readlink -f "$TLS_CERT")
TLS_KEY=$(readlink -f "$TLS_KEY")

TCP_PORT=${MATRIX_TCP_PORT:-41883}
TLS_PORT=${MATRIX_TLS_PORT:-48883}
WS_PORT=${MATRIX_WS_PORT:-48083}
API_PORT=${MATRIX_API_PORT:-41803}

mkdir -p "$OUT"
WORK="$OUT/run"
mkdir -p "$WORK/cfg" "$WORK/data"
RESULTS="$OUT/results-lines.txt"
: > "$RESULTS"

if [ ! -x "$KERNEL" ]; then
  echo "run_all: kernel binary not found at $KERNEL; set MATRIX_KERNEL" >&2
  exit 2
fi

cat > "$WORK/cfg/indra.toml" <<EOF
[node]
id = "matrix"
data_dir = "$WORK/data"

[listeners.tcp]
bind = "127.0.0.1:$TCP_PORT"

[listeners.tls]
enabled = true
bind = "127.0.0.1:$TLS_PORT"
cert_file = "$TLS_CERT"
key_file = "$TLS_KEY"

[listeners.ws]
enabled = true
bind = "127.0.0.1:$WS_PORT"
path = "/mqtt"

[listeners.api]
bind = "127.0.0.1:$API_PORT"

[logging]
level = "warn"

[session]
max_packet_size = 4194304
EOF

BROKER_PID=""
cleanup() {
  if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
}
trap cleanup EXIT

start_broker() {
  INDRA_API_KEYS="$API_KEY" INDRA_API_KEY="$API_KEY" \
    setsid "$KERNEL" --config-dir "$WORK/cfg" \
    > "$WORK/kernel.log" 2>&1 < /dev/null &
  BROKER_PID=$!
  local i
  for i in $(seq 1 80); do
    if python3 -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:$API_PORT/healthz', timeout=1).read()" 2>/dev/null; then
      return 0
    fi
    sleep 0.25
  done
  echo "run_all: broker did not become healthy; see $WORK/kernel.log" >&2
  return 1
}

stop_broker() {
  if [ -n "$BROKER_PID" ] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
    BROKER_PID=""
  fi
}

echo "run_all: starting broker"
start_broker || exit 2
echo "run_all: broker pid $BROKER_PID"

# The restart command stops and starts the broker named in the pid file.
echo "$BROKER_PID" > "$WORK/broker.pid"
MATRIX_RESTART_CMD="OLD=\$(cat $WORK/broker.pid 2>/dev/null); kill \$OLD 2>/dev/null; sleep 1; INDRA_API_KEYS=$API_KEY INDRA_API_KEY=$API_KEY setsid $KERNEL --config-dir $WORK/cfg >> $WORK/kernel.log 2>&1 < /dev/null & echo \$! > $WORK/broker.pid; for i in \$(seq 1 80); do python3 -c \"import urllib.request; urllib.request.urlopen('http://127.0.0.1:$API_PORT/healthz', timeout=1).read()\" 2>/dev/null && exit 0; sleep 0.25; done; exit 1"

export MATRIX_HOST=127.0.0.1
export MATRIX_TCP_PORT=$TCP_PORT
export MATRIX_TLS_PORT=$TLS_PORT
export MATRIX_WS_PORT=$WS_PORT
export MATRIX_WSS_PORT=0
export MATRIX_WS_PATH=/mqtt
export MATRIX_CA="$CA_FILE"
export MATRIX_API_BASE="http://127.0.0.1:$API_PORT"
export MATRIX_API_KEY="$API_KEY"
export MATRIX_USER=""
export MATRIX_PASSWORD=""
export MATRIX_PREFIX="mx$RANDOM"
export MATRIX_RESTART_CMD

# Seed one user so authenticated checks have a credential path.
python3 - <<PY
import json, urllib.request
req = urllib.request.Request(
    "http://127.0.0.1:$API_PORT/api/v1/auth/users",
    data=json.dumps({"username": "mxmatrix", "password": "Mx-matrix-pw-0001"}).encode(),
    headers={"Authorization": "Bearer $API_KEY", "Content-Type": "application/json"},
    method="POST",
)
try:
    with urllib.request.urlopen(req, timeout=5) as resp:
        print("run_all: seeded user HTTP", resp.status)
except Exception as err:
    print("run_all: seed user failed:", err)
PY
export MATRIX_USER=mxmatrix
export MATRIX_PASSWORD=Mx-matrix-pw-0001

run_one() {
  _name=$1; shift
  echo "== $_name =="
  "$@" >> "$RESULTS" 2>&1 || echo "RESULT runner fail $_name exit $?" >> "$RESULTS"
  # Restart leaves a new process; refresh the pid file holder for the next check.
  if [ -f "$WORK/broker.pid" ]; then
    BROKER_PID=$(cat "$WORK/broker.pid")
  fi
}

run_one "paho_python-tcp" bash "$HERE/paho_python/run.sh" tcp
run_one "paho_python-tls" bash "$HERE/paho_python/run.sh" tls
run_one "paho_python-ws"  bash "$HERE/paho_python/run.sh" ws
run_one "node_mqtt-tcp"    bash "$HERE/node_mqtt/run.sh" tcp
run_one "node_mqtt-tls"    bash "$HERE/node_mqtt/run.sh" tls
run_one "node_mqtt-ws"     bash "$HERE/node_mqtt/run.sh" ws
run_one "rust_rumqttc-tcp" bash "$HERE/rust_rumqttc/run.sh" tcp
run_one "rust_paho_mqtt-tcp" bash "$HERE/rust_paho_mqtt/run.sh" tcp
run_one "mosquitto-tcp"    bash "$HERE/mosquitto/run.sh" tcp

stop_broker
echo "run_all: done; lines in $RESULTS"
