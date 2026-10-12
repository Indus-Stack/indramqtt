#!/bin/bash
# Build the broker binary from a source tree. The host runs this.
# Usage: build-broker.sh <source-dir> <output-binary> [<target-dir>]
# The source tree has no .git. This script runs no checkout.
set -u
SRC="${1:?Give a source directory.}"
DEST="${2:?Give an output path.}"
TARGET_DIR="${3:-$(dirname "$DEST")/target}"
D=$(cd "$(dirname "$0")" && pwd)
mkdir -p "$(dirname "$DEST")" "$TARGET_DIR"
EFFECTIVE=$(cat /sys/fs/cgroup/cpuset.cpus.effective 2>/dev/null || echo "")
BROKER_CPUS="${BENCH_BROKER_CPUS:-}"
BUILD_CPUS=$(python3 - "$D" "$EFFECTIVE" "$BROKER_CPUS" <<'EOF'
import sys
sys.path.insert(0, sys.argv[1])
from cpu_layout import format_cpu_list, parse_cpu_list
eff = parse_cpu_list(sys.argv[2]) if len(sys.argv) > 2 else []
brk = parse_cpu_list(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else eff[:4]
rest = [c for c in eff if c not in set(brk)]
print(format_cpu_list(rest))
EOF
)
LOG="$TARGET_DIR/build.log"
set +e
if [ -n "$BUILD_CPUS" ]; then
  CPUSET_ARGS="--cpuset-cpus=$BUILD_CPUS"
else
  CPUSET_ARGS=""
fi
docker run --rm --label purpose=benchmark-temp $CPUSET_ARGS \
  -v "$SRC":/src:ro -v "$TARGET_DIR":/src/target -v indra-cargo-registry:/usr/local/cargo/registry \
  -v "$(dirname "$DEST")":/out -w /src rust:1.96-bookworm \
  bash -c "cargo build --release -p broker-node 2>&1 | tee /src/target/build.log && cp /src/target/release/indramqtt /out/$(basename "$DEST").tmp"
RC=$?
set -u
if [ -f "$LOG" ]; then
  tail -40 "$LOG"
fi
if [ $RC -ne 0 ]; then
  echo "Build failed with code $RC." >&2
  exit $RC
fi
mv "$(dirname "$DEST")/$(basename "$DEST").tmp" "$DEST"
rm -rf "$TARGET_DIR"
ls -la "$DEST"
