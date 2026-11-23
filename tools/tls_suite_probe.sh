#!/usr/bin/env bash
# Probe the kernel TLS listener with legacy cipher suites.
#
# The script starts the kernel from a temp config file with a test
# certificate. It then runs handshake probes with openssl s_client.
# It prints one PROBE line per cipher suite and stops the kernel.
# The probe records the current result for the R1 note. It keeps
# rustls with AEAD suites only until the release decides otherwise.
#
# Usage:
#   tools/tls_suite_probe.sh <dir with indramqtt>
#
# Full scale needs a host with openssl and free loopback ports.
# The script exits 0 after it prints the matrix. It exits 2 for
# bad usage. A failed modern handshake exits 1.
set -u

BIN_DIR=${1:?give the directory that holds indramqtt}
BIN_DIR=$(cd "$BIN_DIR" 2>/dev/null && pwd) || { echo "FAIL: $1 is not a directory"; exit 2; }
KERNEL=$BIN_DIR/indramqtt
[ -x "$KERNEL" ] || { echo "FAIL: $KERNEL is not executable"; exit 2; }
command -v openssl >/dev/null || { echo "FAIL: openssl is not in PATH"; exit 2; }
command -v python3 >/dev/null || { echo "FAIL: python3 is not in PATH"; exit 2; }

# Test certificate for the probe. The files also serve the kernel
# TLS tests, so the probe uses the same chain as the gates.
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
TREE_DIR=$(cd "$SCRIPT_DIR/.." && pwd)
ROOT=$(mktemp -d)
bash "$TREE_DIR/tools/gen_test_pki.sh" "$ROOT/pki"
CERT="$ROOT/pki/server-cert.pem"
KEY="$ROOT/pki/server-key.pem"
KERNEL_PID=
failures=0

cleanup() {
    [ -n "$KERNEL_PID" ] && kill "$KERNEL_PID" 2>/dev/null
    wait 2>/dev/null
    rm -rf "$ROOT"
}
trap cleanup EXIT

# Get free ports. The probe uses these ports and not the defaults.
read -r API_PORT MQTT_PORT TLS_PORT <<EOF
$(python3 - <<'PY'
import socket
socks = [socket.socket() for _ in range(3)]
for s in socks:
    s.bind(("127.0.0.1", 0))
print(" ".join(str(s.getsockname()[1]) for s in socks))
for s in socks:
    s.close()
PY
)
EOF

mkdir -p "$ROOT/cfg" "$ROOT/data"
cat > "$ROOT/cfg/indra.toml" <<EOF
[node]
id = "tls-probe"
data_dir = "$ROOT/data"

[listeners.tcp]
bind = "127.0.0.1:$MQTT_PORT"
native = true

[listeners.tls]
enabled = true
bind = "127.0.0.1:$TLS_PORT"
cert_file = "$CERT"
key_file = "$KEY"
native = true

[listeners.ws]
enabled = false

[listeners.wss]
enabled = false

[listeners.api]
bind = "127.0.0.1:$API_PORT"

[logging]
level = "warn"
EOF

# Start the kernel. The probe sends packets to it through real sockets.
"$KERNEL" --config-dir "$ROOT/cfg" --allow-anonymous > "$ROOT/kernel.log" 2>&1 &
KERNEL_PID=$!

# Wait for the TLS port. A 10 s timeout bounds a stuck start.
# Reason: 10 s matches the CONNECT timeout of the kernel listener.
ready=
for _ in $(seq 1 50); do
    if (echo > /dev/tcp/127.0.0.1/$TLS_PORT) 2>/dev/null; then
        ready=1
        break
    fi
    sleep 0.2
done
[ -n "$ready" ] || { echo "FAIL: kernel never opened TLS $TLS_PORT"; exit 1; }

# Probe one cipher suite. It prints PROBE with OK or FAIL.
# A 10 s timeout bounds each handshake. Reason: the kernel ends a
# silent handshake after 10 s, so the probe matches that bound.
probe() {
    label=$1
    shift
    if timeout 10 openssl s_client -connect "127.0.0.1:$TLS_PORT" "$@" < /dev/null > "$ROOT/$label.out" 2>&1; then
        if grep -q "Cipher is " "$ROOT/$label.out"; then
            cipher=$(grep "Cipher is " "$ROOT/$label.out" | head -1)
            echo "PROBE kernel $label OK ($cipher)"
        else
            echo "PROBE kernel $label FAIL (no cipher line)"
            failures=$((failures + 1))
        fi
    else
        echo "PROBE kernel $label FAIL (handshake failed)"
        failures=$((failures + 1))
    fi
}

# Modern client with default suites. This probe must pass.
probe modern -tls1_3
# Legacy CBC suite on ECDHE. rustls lacks CBC suites, so this probe
# records FAIL on the current listener. The R1 note uses the result.
probe ecdhe-cbc -cipher ECDHE-RSA-AES128-SHA -tls1_2
# Legacy CBC suite on RSA key exchange. This probe also records FAIL
# on the current listener. The R1 note uses the result.
probe rsa-cbc -cipher AES128-SHA -tls1_2

echo "PROBE done failures=$failures backlog=1024"
# The modern handshake must pass. Legacy CBC probes may fail.
grep -q "Cipher is " "$ROOT/modern.out" || exit 1
exit 0
