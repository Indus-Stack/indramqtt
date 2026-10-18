#!/usr/bin/env bash
# Run the wire codec and listener fuzz targets and write the report.
#
# The script runs the two budgeted fuzz targets with FUZZ_SECONDS set
# (600 for the long pass). It collects the WIRE_FUZZ summary line of
# each target and every saved defect, and it writes
# tools/wire-fuzz/results.md.
#
# The targets are:
#   crates/broker-protocol/tests/wire_fuzz.rs
#   crates/broker-node/tests/wire_fuzz_listener.rs
#
# The script does not pass --target-dir. It uses CARGO_TARGET_DIR. The
# loop cannot run cargo; the gates or the CTO run this script.
#
# Usage:
#   FUZZ_SECONDS=600 bash tools/wire-fuzz/run.sh
#
# Environment:
#   FUZZ_SECONDS  budget in seconds (default 600)
#   FUZZ_SEED     fixed seed (default: the clock seed of each target)
set -u

HERE=$(cd "$(dirname "$0")" && pwd)
TREE=$(cd "$HERE/../.." && pwd)
BUDGET=${FUZZ_SECONDS:-600}
OUT="$HERE/results.md"
LOG="$HERE/fuzz-run.log"
: > "$LOG"

# Run one target. Bounds the run at four times the budget plus 120 s.
run_target() {
  local pkg=$1 target=$2
  local limit=$((BUDGET * 4 + 120))
  echo "== $pkg --test $target budget=${BUDGET}s timeout=${limit}s" | tee -a "$LOG"
  ( cd "$TREE" && FUZZ_SECONDS="$BUDGET" FUZZ_SEED="${FUZZ_SEED:-}" \
      timeout "$limit" cargo +1.96.0 test -p "$pkg" --test "$target" -- --nocapture ) >> "$LOG" 2>&1
  echo "exit $?" | tee -a "$LOG"
}

run_target broker-protocol wire_fuzz
run_target broker-node wire_fuzz_listener

{
  echo "# Wire codec and listener fuzz results"
  echo
  echo "- Date (UTC): $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "- Budget: ${BUDGET}s for each target, from \`FUZZ_SECONDS\`."
  echo "- Command: \`FUZZ_SECONDS=${BUDGET} bash tools/wire-fuzz/run.sh\`"
  echo
  echo "## Summary lines"
  echo
  echo '```'
  grep -E 'WIRE_FUZZ' "$LOG" || echo "no WIRE_FUZZ line"
  echo '```'
  echo
  echo "## Defects found"
  echo
  echo '```'
  grep -E 'wire fuzz failure|fuzz failure|panicked at' "$LOG" || echo "none"
  echo '```'
  echo
  echo "The full run log is \`tools/wire-fuzz/fuzz-run.log\`."
} > "$OUT"

echo "wrote $OUT"
