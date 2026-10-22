#!/usr/bin/env bash
# Self-test for check-worker-scaling.sh: a run that is no slower with more
# workers passes, and a seeded worker regression fails.

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/worker-gate.$$.XXXXXX")"
trap 'rm -rf "${WORK}"' EXIT

# run FILE WORKERS Q3_MS: a two-query gb_bench JSON at SF1.
run() {
  cat > "$1" <<JSON
{"engine":"mizuchi","sf":"sf1","repeats":3,"workers":$2,"queries":[
 {"id":"tpch_q1","reported_ms":100.0},
 {"id":"tpch_q3","reported_ms":$3}]}
JSON
}

gate() {
  bash "${ROOT}/scripts/check-worker-scaling.sh" \
    --one "${WORK}/one.json" --many "${WORK}/many.json" --summary /dev/null >/dev/null 2>&1
}

run "${WORK}/one.json" 1 100.0
run "${WORK}/many.json" 4 60.0
gate || { echo "FAIL: a scaling run should pass"; exit 1; }

# Seeded worker regression: q3 is twice as slow with four workers.
run "${WORK}/many.json" 4 200.0
if gate; then
  echo "FAIL: a worker regression should fail the gate"
  exit 1
fi
# A time that rounds to zero reports n/a instead of failing.
run "${WORK}/many.json" 4 0.0
gate || { echo "FAIL: a zero time should not crash the gate"; exit 1; }
echo "check-worker-scaling: ok"
