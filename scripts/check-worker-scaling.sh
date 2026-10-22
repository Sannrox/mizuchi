#!/usr/bin/env bash
# Fail when any query is slower with N workers than with one worker.
# Compares two gb_bench JSON runs of the same suite and scale: one at
# --workers 1 and one at --workers N. A query fails when its N-worker time
# exceeds FACTOR times its 1-worker time (default 1.25, for runner noise)
# and either time is at least FLOOR_MS (default 5). Run it at SF1 or
# larger: at tiny scale every query is below a millisecond and thread
# start-up dominates.
#
# Usage:
#   scripts/check-worker-scaling.sh --one PATH --many PATH
#     [--factor 1.25] [--floor-ms 5] [--summary PATH]

set -o errexit
set -o nounset
set -o pipefail

ONE=""
MANY=""
FACTOR="1.25"
FLOOR_MS="5"
SUMMARY="${GITHUB_STEP_SUMMARY:-}"

while [ $# -gt 0 ]; do
  case "$1" in
    --one) ONE="$2"; shift 2 ;;
    --many) MANY="$2"; shift 2 ;;
    --factor) FACTOR="$2"; shift 2 ;;
    --floor-ms) FLOOR_MS="$2"; shift 2 ;;
    --summary) SUMMARY="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [ -z "${ONE}" ] || [ -z "${MANY}" ]; then
  echo "usage: $0 --one PATH --many PATH" >&2
  exit 2
fi

export ONE MANY FACTOR FLOOR_MS SUMMARY
"${PYTHON:-python3}" - <<'PY'
import json, os, sys

def load(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)

one, many = load(os.environ["ONE"]), load(os.environ["MANY"])
if int(one.get("workers") or 0) != 1:
    sys.exit(f"{os.environ['ONE']} must be a --workers 1 run")
n = int(many.get("workers") or 0)
if n <= 1:
    sys.exit(f"{os.environ['MANY']} must use more than one worker")
if one.get("sf") != many.get("sf"):
    sys.exit("both runs must use the same scale")
factor = float(os.environ["FACTOR"])
floor = float(os.environ["FLOOR_MS"])
single = {q["id"]: float(q["reported_ms"]) for q in one.get("queries", [])}

lines = [
    f"### Worker scaling ({many.get('sf')}, 1 vs {n} workers)",
    "",
    "| query | 1 worker ms | N workers ms | speedup |",
    "| --- | ---: | ---: | ---: |",
]
errors = []
for q in many.get("queries", []):
    qid, ms = q["id"], float(q["reported_ms"])
    base = single.get(qid)
    if base is None:
        errors.append(f"{qid}: missing from the 1-worker run")
        continue
    speedup = f"{base / ms:.2f}x" if ms > 0 else "n/a"
    lines.append(f"| {qid} | {base:.3f} | {ms:.3f} | {speedup} |")
    if max(base, ms) >= floor and ms > base * factor:
        errors.append(
            f"{qid}: {ms:.3f} ms at {n} workers is >{factor:.0%} of {base:.3f} ms at 1 worker"
        )

body = "\n".join(lines) + "\n"
summary = os.environ.get("SUMMARY") or ""
if summary:
    with open(summary, "a", encoding="utf-8") as f:
        f.write(body)
print(body)
if errors:
    print("worker scaling check failed:", file=sys.stderr)
    for e in errors:
        print(f"  {e}", file=sys.stderr)
    sys.exit(1)
print("worker scaling check passed")
PY
