#!/bin/bash
# Run all benchmark scenarios on the host. Needs Docker.
# Usage: run_all.sh <source-dir> <commit-label> <outdir> [suite options]
# It builds the release binary from a source tree. It runs each scenario.
# It calls report.py at the end.
set -u
SRC="${1:?Give a source directory.}"
COMMIT_LABEL="${2:?Give a commit label.}"
OUT="${3:?Give an output directory.}"
shift 3 || true
D=$(cd "$(dirname "$0")" && pwd)
BIN="$OUT/bin/indramqtt"
if [ -z "${LOADGEN_IMAGE:-}" ] || [ -z "${LOADGEN_BIN:-}" ]; then
  echo "Set LOADGEN_IMAGE and LOADGEN_BIN. Both variables must have values." >&2
  exit 1
fi
test -d "$SRC" || { echo "Source directory $SRC is absent." >&2; exit 1; }

mkdir -p "$OUT/bin"
echo "Build binary from $SRC ($COMMIT_LABEL)."
if ! docker image inspect indra-bench-base >/dev/null 2>&1; then
  echo "Build base image indra-bench-base."
  docker build -t indra-bench-base -f "$D/Dockerfile" "$D"
fi
if [ ! -x "$D/bin/latency-probe" ]; then
  echo "Probe binary is absent. Build it."
  bash "$D/build-probe.sh"
fi
bash "$D/build-broker.sh" "$SRC" "$OUT/bin/indramqtt" "$OUT/target"
test -x "$BIN" || { echo "Build left no binary at $BIN."; exit 1; }
for S in s1-qos0 s1-qos1 s1-qos2 s2-payload s3-fanin s3-fanout; do
  echo "Run scenario $S."
  python3 "$D/suite.py" run "$S" --binary "$BIN" --out "$OUT" \
    --bench-image "$LOADGEN_IMAGE" --bench-bin "$LOADGEN_BIN" "$@" || echo "Scenario $S stops early."
done
python3 "$D/suite.py" summarize "$OUT"
python3 "$D/report.py" "$OUT" "$OUT/report.html"
echo "Wrote $OUT/report.html."
