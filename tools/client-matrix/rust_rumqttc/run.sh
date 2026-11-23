#!/usr/bin/env bash
# Run the Rust rumqttc checklist.
#
# Use:
#   MATRIX_HOST=127.0.0.1 MATRIX_TCP_PORT=1883 bash run.sh [transport]
#
# This client covers plain TCP only. TLS and WebSocket report
# not-supported. Set MATRIX_RESTART_CMD so the restart check can run.
#
# The loop agents cannot run cargo. The runner uses a prebuilt binary
# when one exists. Set MATRIX_ALLOW_CARGO=1 to let cargo build and run
# the project; no automated gate builds or runs the project.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
TRANSPORT=${1:-tcp}

if [ "$TRANSPORT" != "tcp" ]; then
  echo "rust backend: rumqttc over plain TCP only"
  for check in v311-connect v5-connect qos0 qos1 qos2 retained will \
      session-resume shared payload-1mib auth keepalive restart; do
    echo "RESULT $check not-supported rumqttc client uses plain TCP only"
  done
  exit 0
fi

echo "rust backend: rumqttc"

PREBUILT=""
for candidate in \
  "$HERE/target/debug/matrix-rumqttc" \
  "${CARGO_TARGET_DIR:-}/debug/matrix-rumqttc"; do
  if [ -n "$candidate" ] && [ -x "$candidate" ]; then
    PREBUILT=$candidate
    break
  fi
done

if [ -n "$PREBUILT" ]; then
  exec "$PREBUILT" "$TRANSPORT"
fi

if [ "${MATRIX_ALLOW_CARGO:-0}" = "1" ]; then
  # Build outside the product workspace. Keep no artefacts in the tree.
  exec cargo run --quiet --manifest-path "$HERE/Cargo.toml" -- "$TRANSPORT"
fi

echo "rust binary is not built; no automated gate builds or runs the project"
for check in v311-connect v5-connect qos0 qos1 qos2 retained will \
    session-resume shared payload-1mib auth keepalive restart; do
  echo "RESULT $check not-verified rust binary is not built in this run"
done
