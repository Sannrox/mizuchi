#!/usr/bin/env bash
# Self-test for check-bench-regression.sh. Each case asserts the gate's exit
# code and a line of its output, so a crash or usage error never passes as
# a detected regression.

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/bench-gate.$$.XXXXXX")"
trap 'rm -rf "${WORK}"' EXIT

cat > "${WORK}/baseline.json" <<'JSON'
{"version":1,"regression_factor":1.2,"min_abs_ms":5.0,"workers":2,
 "tiny":{"tpch_q1":{"mizuchi_ms":100.0},"tpch_q3":{"mizuchi_ms":100.0}}}
JSON

# run QUERIES: a two-worker mizuchi gb_bench JSON with the given query list.
run() {
  printf '{"engine":"mizuchi","sf":"tiny","workers":2,"queries":[%s]}\n' "$1" \
    > "${WORK}/mizuchi.json"
}
q() { printf '{"id":"%s","reported_ms":%s}' "$1" "$2"; }

# expect NAME CODE PATTERN [ARGS...]: the gate exits CODE and prints PATTERN.
expect() {
  local name="$1" code="$2" pattern="$3" got=0
  shift 3
  bash "${ROOT}/scripts/check-bench-regression.sh" --summary "" --results-md "" "$@" \
    > "${WORK}/out" 2>&1 || got=$?
  if [ "${got}" != "${code}" ] || ! grep -q -- "${pattern}" "${WORK}/out"; then
    echo "FAIL: ${name}: want exit ${code} with '${pattern}', got exit ${got}:"
    cat "${WORK}/out"
    exit 1
  fi
}

GATE=(--scale tiny --mizuchi "${WORK}/mizuchi.json" --baseline "${WORK}/baseline.json")

run "$(q tpch_q1 100.0),$(q tpch_q3 110.0)"
expect "a run within the factor passes" 0 "regression check passed" "${GATE[@]}"

run "$(q tpch_q1 100.0),$(q tpch_q3 150.0)"
expect "a regression fails" 1 "tpch_q3: mizuchi 150.000 ms is >120% of baseline" "${GATE[@]}"

run "$(q tpch_q1 100.0)"
expect "a baseline query missing from the run fails" 1 \
  "tpch_q3: missing from the mizuchi run" "${GATE[@]}"

run ""
expect "an empty run fails" 1 "mizuchi.json has no queries" "${GATE[@]}"

run "$(q tpch_q1 100.0)"
expect "a scale without baseline entries fails" 1 "has no entries for scale sf1" \
  --scale sf1 --mizuchi "${WORK}/mizuchi.json" --baseline "${WORK}/baseline.json"

expect "a usage error is not a regression" 2 "usage:" --scale tiny

echo "check-bench-regression: ok"
