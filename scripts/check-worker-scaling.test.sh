#!/usr/bin/env bash
# Self-test for check-worker-scaling.sh. Each case asserts the gate's exit
# code and a line of its output, so a crash or usage error never passes as
# a detected regression.

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/worker-gate.$$.XXXXXX")"
trap 'rm -rf "${WORK}"' EXIT

# run FILE WORKERS QUERIES: a gb_bench JSON at SF1 with the given query list.
run() {
  printf '{"engine":"mizuchi","sf":"sf1","repeats":3,"workers":%s,"queries":[%s]}\n' \
    "$2" "$3" > "$1"
}
q() { printf '{"id":"%s","reported_ms":%s}' "$1" "$2"; }

# expect NAME CODE PATTERN [ARGS...]: the gate exits CODE and prints PATTERN.
expect() {
  local name="$1" code="$2" pattern="$3" got=0
  shift 3
  bash "${ROOT}/scripts/check-worker-scaling.sh" --summary /dev/null "$@" \
    > "${WORK}/out" 2>&1 || got=$?
  if [ "${got}" != "${code}" ] || ! grep -q -- "${pattern}" "${WORK}/out"; then
    echo "FAIL: ${name}: want exit ${code} with '${pattern}', got exit ${got}:"
    cat "${WORK}/out"
    exit 1
  fi
}

ONE=(--one "${WORK}/one.json" --many "${WORK}/many.json")
run "${WORK}/one.json" 1 "$(q tpch_q1 100.0),$(q tpch_q3 100.0)"

run "${WORK}/many.json" 4 "$(q tpch_q1 100.0),$(q tpch_q3 60.0)"
expect "a scaling run passes" 0 "worker scaling check passed" "${ONE[@]}"

run "${WORK}/many.json" 4 "$(q tpch_q1 100.0),$(q tpch_q3 200.0)"
expect "a worker regression fails" 1 "tpch_q3: 200.000 ms at 4 workers is >125%" "${ONE[@]}"

run "${WORK}/many.json" 4 "$(q tpch_q1 100.0)"
expect "a query missing from the N-worker run fails" 1 \
  "tpch_q3: missing from the 4-worker run" "${ONE[@]}"

run "${WORK}/many.json" 4 ""
expect "an empty run fails" 1 "many.json has no queries" "${ONE[@]}"

run "${WORK}/many.json" 4 "$(q tpch_q1 100.0),$(q tpch_q3 0.0)"
expect "a time that rounds to zero reports n/a" 0 "| tpch_q3 | 100.000 | 0.000 | n/a |" "${ONE[@]}"

expect "a usage error is not a regression" 2 "usage:" --one "${WORK}/one.json"

echo "check-worker-scaling: ok"
