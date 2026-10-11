#!/bin/bash
# Build the broker binary from a commit. The host runs this.
# Usage: build-broker.sh <commit> <output-binary>
set -u
COMMIT="${1:?Give a commit.}"
DEST="${2:?Give an output path.}"
D=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$D/../.." && pwd)
mkdir -p "$(dirname "$DEST")"
docker run --rm --label purpose=benchmark-temp \
  -v "$ROOT":/src -w /src rust:1-bookworm \
  bash -c "git checkout $COMMIT --quiet && cargo build --release -p broker-node 2>&1 | tail -5 && cp target/release/indramqtt /src/bench-bin-tmp"
mv "$ROOT/bench-bin-tmp" "$DEST" && ls -la "$DEST"
