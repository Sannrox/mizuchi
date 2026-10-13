#!/usr/bin/env bash
# Capture per-operator exclusive-time and peak-bytes profiles for the
# decision-support suite. Large files stay in bench-data/ (gitignored).
# Default cargo test does not invoke this script.
# Usage: scripts/profile-suite.sh [tiny|1|10]

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${ROOT}"

SF="${1:-tiny}"
OUT="${BENCH_DATA:-${ROOT}/bench-data}"

case "${SF}" in
  tiny) LABEL=tiny ;;
  1|sf1) SF=1; LABEL=sf1 ;;
  10|sf10) SF=10; LABEL=sf10 ;;
  *)
    echo "usage: $0 [tiny|1|10]" >&2
    exit 2
    ;;
esac

echo "profile-suite sf=${SF} label=${LABEL} out=${OUT}"

cargo run --release --example gb_bench -- generate --sf "${SF}" --out "${OUT}"
cargo run --release --example gb_bench -- profile --sf "${SF}" --dir "${OUT}" \
  --json "${OUT}/${LABEL}/profile.json"

echo "profile under ${OUT}/${LABEL}/profile.json"
