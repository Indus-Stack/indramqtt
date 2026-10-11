#!/bin/bash
# Run all benchmark scenarios on the host. Needs Docker.
# Usage: run_all.sh <commit> <outdir> [suite.py options]
# It builds the release binary from a commit. It runs each scenario.
# It calls report.py at the end.
set -u
COMMIT="${1:?Give a commit.}"
OUT="${2:?Give an output directory.}"
shift 2 || true
D=$(cd "$(dirname "$0")" && pwd)
BIN="$OUT/bin/indramqtt"
IMG="${LOADGEN_IMAGE:-mqtt-loadgen:latest}"
LOGBIN="${LOADGEN_BIN:-/loadgen/bin/loadgen}"

mkdir -p "$OUT/bin"
echo "Build binary from $COMMIT."
bash "$D/build-broker.sh" "$COMMIT" "$OUT/bin/indramqtt"
test -x "$BIN" || { echo "Build left no binary at $BIN."; exit 1; }
for S in s1-qos0 s1-qos1 s1-qos2 s2-payload s3-fanin s3-fanout; do
  echo "Run scenario $S."
  python3 "$D/suite.py" run "$S" --binary "$BIN" --out "$OUT" \
    --bench-image "$IMG" --bench-bin "$LOGBIN" "$@" || echo "Scenario $S stops early."
done
python3 "$D/suite.py" summarize "$OUT"
python3 "$D/report.py" "$OUT" "$OUT/report.html"
echo "Wrote $OUT/report.html."
