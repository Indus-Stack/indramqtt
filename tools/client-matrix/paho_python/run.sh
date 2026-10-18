#!/usr/bin/env bash
# Run the Paho Python checklist.
#
# Use:
#   MATRIX_HOST=127.0.0.1 MATRIX_TCP_PORT=1883 bash run.sh [transport]
#
# The runner installs the Eclipse Paho Python client with
# `pip install --target` into a directory outside the tree when the
# package is missing. Set MATRIX_PYLIBS to choose that directory.
#
# Set MATRIX_RESTART_CMD to a command that restarts the broker so the
# restart check can run. run_all.sh sets it when it owns the broker.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
TRANSPORT=${1:-tcp}
LIBS=${MATRIX_PYLIBS:-$HOME/.cache/client-matrix-paho}
mkdir -p "$LIBS"

if ! PYTHONPATH="$LIBS" python3 -c "import paho.mqtt.client" 2>/dev/null; then
  if python3 -m pip --version >/dev/null 2>&1; then
    python3 -m pip install --target "$LIBS" --quiet paho-mqtt \
      && echo "python backend: paho-mqtt installed into $LIBS" \
      || echo "python backend: paho-mqtt install failed"
  else
    echo "python backend: paho-mqtt not present and no pip; install it outside the tree"
  fi
fi

echo "python backend: $(PYTHONPATH="$LIBS" python3 -c 'import paho.mqtt;print("paho-mqtt", paho.mqtt.__version__)' 2>/dev/null || echo 'missing')"
PYTHONPATH="$LIBS:${PYTHONPATH:-}" exec python3 "$HERE/check.py" "$TRANSPORT"
