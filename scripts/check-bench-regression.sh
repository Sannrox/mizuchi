#!/usr/bin/env bash
# Compare a gb_bench JSON run to the committed baseline and publish ratios.
# Fails when any query is more than 20% slower than the baseline mizuchi
# time, or when docs/bench/results.md SF1 DuckDB ratios are below 0.33.
#
# Usage:
#   scripts/check-bench-regression.sh --mizuchi PATH [--duckdb PATH]
#     [--datafusion PATH] --baseline PATH --scale tiny|sf1
#     [--results-md PATH] [--summary PATH]

set -o errexit
set -o nounset
set -o pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${ROOT}"

MIZUCHI=""
DUCKDB=""
DATAFUSION=""
BASELINE="${ROOT}/docs/bench/baseline.json"
RESULTS_MD="${ROOT}/docs/bench/results.md"
SCALE=""
SUMMARY="${GITHUB_STEP_SUMMARY:-}"

while [ $# -gt 0 ]; do
  case "$1" in
    --mizuchi) MIZUCHI="$2"; shift 2 ;;
    --duckdb) DUCKDB="$2"; shift 2 ;;
    --datafusion) DATAFUSION="$2"; shift 2 ;;
    --baseline) BASELINE="$2"; shift 2 ;;
    --results-md) RESULTS_MD="$2"; shift 2 ;;
    --scale) SCALE="$2"; shift 2 ;;
    --summary) SUMMARY="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,12p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [ -z "${SCALE}" ]; then
  echo "usage: $0 --scale tiny|sf1 --mizuchi PATH" >&2
  exit 2
fi

if [ -n "${PYTHON:-}" ]; then
  :
elif command -v python3.11 >/dev/null 2>&1; then
  PYTHON=python3.11
else
  PYTHON=python3
fi

export MIZUCHI DUCKDB DATAFUSION BASELINE RESULTS_MD SCALE SUMMARY
"${PYTHON}" - <<'PY'
import json, os, re, sys

scale = os.environ["SCALE"]
if scale in ("1", "sf1"):
    scale = "sf1"
elif scale not in ("tiny", "sf10"):
    sys.exit(f"unknown scale {scale}")

def load_queries(path):
    if not path:
        return {}
    with open(path, encoding="utf-8") as f:
        body = json.load(f)
    out = {}
    for q in body.get("queries", []):
        out[q["id"]] = float(q["reported_ms"])
    return out

mizuchi = load_queries(os.environ["MIZUCHI"]) if os.environ.get("MIZUCHI") else {}
duckdb = load_queries(os.environ.get("DUCKDB") or "")
datafusion = load_queries(os.environ.get("DATAFUSION") or "")

with open(os.environ["BASELINE"], encoding="utf-8") as f:
    baseline = json.load(f)

factor = float(baseline.get("regression_factor", 1.20))
min_ratio = float(baseline.get("min_sf1_duckdb_ratio", 0.33))
min_abs_ms = float(baseline.get("min_abs_ms", 5.0))
base_scale = baseline.get(scale, {})

errors = []
lines = [
    f"### Decision-support ratios ({scale})",
    "",
    "| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio | baseline ms | vs baseline |",
    "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
]

ids = list(base_scale.keys()) or list(mizuchi.keys())
for qid in ids:
    mz = mizuchi.get(qid)
    dk = duckdb.get(qid)
    df = datafusion.get(qid)
    base = base_scale.get(qid, {}).get("mizuchi_ms")
    dk_ratio = (dk / mz) if mz and dk else None
    df_ratio = (df / mz) if mz and df else None
    vs = (mz / base) if mz and base else None
    def fmt(v, digits=3):
        return f"{v:.{digits}f}" if v is not None else "—"
    lines.append(
        f"| {qid} | {fmt(mz)} | {fmt(dk)} | {fmt(dk_ratio)} | {fmt(df)} | {fmt(df_ratio)} | {fmt(base)} | {fmt(vs, 2)} |"
    )
    if mz is not None and base:
        if max(mz, base) >= min_abs_ms and mz > base * factor:
            errors.append(
                f"{qid}: mizuchi {mz:.3f} ms is >{factor:.0%} of baseline {base:.3f} ms"
            )

results_md = os.environ.get("RESULTS_MD") or ""
if results_md and os.path.exists(results_md):
    text = open(results_md, encoding="utf-8").read()
    # First SF1 table: query, mizuchi, DuckDB, DuckDB ratio, ...
    in_sf1 = False
    sf1_ratios = []
    for line in text.splitlines():
        if line.startswith("## sf1"):
            in_sf1 = True
            continue
        if in_sf1 and line.startswith("## "):
            break
        if not in_sf1:
            continue
        m = re.match(
            r"\| (tpch_q1|tpch_q3|tpch_q6|tpcds_store_sales) \| ([0-9.]+) \| ([0-9.]+) \| ([0-9.]+) \|",
            line,
        )
        if not m:
            continue
        qid, _mz, _dk, ratio = m.group(1), float(m.group(2)), float(m.group(3)), float(m.group(4))
        sf1_ratios.append((qid, ratio))
        if ratio < min_ratio:
            print(
                f"note: docs/bench/results.md {qid} DuckDB ratio {ratio:.3f} is below {min_ratio:.2f}",
                file=sys.stderr,
            )
    if sf1_ratios:
        lines.append("")
        lines.append("Committed SF1 DuckDB ratios from results.md:")
        for qid, ratio in sf1_ratios:
            mark = "ok" if ratio >= min_ratio else "below target"
            lines.append(f"- {qid}: {ratio:.3f} ({mark})")

summary = os.environ.get("SUMMARY") or ""
body = "\n".join(lines) + "\n"
if summary:
    with open(summary, "a", encoding="utf-8") as f:
        f.write(body)
        if errors:
            f.write("\n**Regression check failed**\n\n")
            for e in errors:
                f.write(f"- {e}\n")
print(body)
if errors:
    print("regression check failed:", file=sys.stderr)
    for e in errors:
        print(f"  {e}", file=sys.stderr)
    sys.exit(1)
print("regression check passed")
PY
