#!/usr/bin/env bash
# Run the Node mqtt-package checklist.
#
# Use:
#   MATRIX_HOST=127.0.0.1 MATRIX_TCP_PORT=1883 bash run.sh [transport]
#
# The runner installs the `mqtt` package with npm into a directory
# outside the tree when the package is missing. Set MATRIX_NODELIB to
# choose that directory. Set MATRIX_RESTART_CMD so the restart check
# can run.
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
TRANSPORT=${1:-tcp}
LIBS=${MATRIX_NODELIB:-$HOME/.cache/client-matrix-node}
mkdir -p "$LIBS"

if ! NODE_PATH="$LIBS" node -e "require('mqtt')" 2>/dev/null; then
  NPM_CLI=${MATRIX_NPM_CLI:-}
  if [ -z "$NPM_CLI" ] && command -v npm >/dev/null 2>&1; then
    NPM_CLI=$(command -v npm)
  fi
  if [ -n "$NPM_CLI" ] && [ -f "$NPM_CLI" ]; then
    node "$NPM_CLI" install --prefix "$LIBS" --no-audit --no-fund --loglevel=error mqtt \
      && echo "node backend: mqtt installed into $LIBS" \
      || echo "node backend: mqtt install failed"
  else
    echo "node backend: mqtt not present and no npm; install it outside the tree"
  fi
fi

VERSION=$(NODE_PATH="$LIBS" node -e "console.log(require('mqtt/package.json').version)" 2>/dev/null || echo missing)
echo "node backend: mqtt $VERSION"
NODE_PATH="$LIBS:${NODE_PATH:-}" exec node "$HERE/check.js" "$TRANSPORT"
