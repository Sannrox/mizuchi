#!/usr/bin/env bash
# Generate decision-support Parquet and time mizuchi, DuckDB, and DataFusion.
# Large files stay in bench-data/ (gitignored). Default cargo test does not
# invoke this script. Usage: scripts/bench-gb.sh [tiny|1|10]

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${ROOT}"

SF="${1:-tiny}"
OUT="${BENCH_DATA:-${ROOT}/bench-data}"
REPEATS="${BENCH_REPEATS:-3}"

case "${SF}" in
  tiny) LABEL=tiny ;;
  1|sf1) SF=1; LABEL=sf1 ;;
  10|sf10) SF=10; LABEL=sf10 ;;
  *)
    echo "usage: $0 [tiny|1|10]" >&2
    exit 2
    ;;
esac

if [ -n "${PYTHON:-}" ]; then
  :
elif command -v python3.11 >/dev/null 2>&1; then
  PYTHON=python3.11
else
  PYTHON=python3
fi

echo "bench-gb sf=${SF} label=${LABEL} out=${OUT} python=${PYTHON}"

cargo run --release --example gb_bench -- generate --sf "${SF}" --out "${OUT}"
cargo run --release --example gb_bench -- run --sf "${SF}" --dir "${OUT}" \
  --repeats "${REPEATS}" --json "${OUT}/${LABEL}/mizuchi.json"

"${PYTHON}" "${ROOT}/scripts/bench_gb_ref.py" \
  --sf "${SF}" --dir "${OUT}" --repeats "${REPEATS}" --engine duckdb \
  --json "${OUT}/${LABEL}/duckdb.json"
"${PYTHON}" "${ROOT}/scripts/bench_gb_ref.py" \
  --sf "${SF}" --dir "${OUT}" --repeats "${REPEATS}" --engine datafusion \
  --json "${OUT}/${LABEL}/datafusion.json"

echo "results under ${OUT}/${LABEL}/"
