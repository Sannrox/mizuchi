#!/usr/bin/env python3
"""Time DuckDB or DataFusion on Parquet produced by gb_bench generate.

Loads each table into memory first so scan cost matches mizuchi's registered
Arrow batches. Query SQL matches crates/mizuchi/examples/gb_bench.rs.
"""

from __future__ import annotations

import argparse
import json
import statistics
import time
from pathlib import Path

QUERIES = [
    (
        "tpch_q1",
        "SELECT l_returnflag, l_linestatus, "
        "SUM(l_quantity) AS sum_qty, "
        "SUM(l_extendedprice) AS sum_base_price, "
        "SUM(l_extendedprice * (1.0 - l_discount)) AS sum_disc_price, "
        "SUM(l_extendedprice * (1.0 - l_discount) * (1.0 + l_tax)) AS sum_charge, "
        "AVG(l_quantity) AS avg_qty, "
        "AVG(l_extendedprice) AS avg_price, "
        "AVG(l_discount) AS avg_disc, "
        "COUNT(*) AS count_order "
        "FROM lineitem "
        "WHERE l_shipdate <= 2435 "
        "GROUP BY l_returnflag, l_linestatus "
        "ORDER BY l_returnflag, l_linestatus",
    ),
    (
        "tpch_q3",
        "SELECT l_orderkey, o_orderdate, o_shippriority, "
        "SUM(l_extendedprice * (1.0 - l_discount)) AS revenue "
        "FROM customer "
        "JOIN orders ON c_custkey = o_custkey "
        "JOIN lineitem ON l_orderkey = o_orderkey "
        "WHERE c_mktsegment = 'BUILDING' "
        "AND o_orderdate < 1169 "
        "AND l_shipdate > 1169 "
        "GROUP BY l_orderkey, o_orderdate, o_shippriority "
        "ORDER BY revenue DESC, o_orderdate "
        "LIMIT 10",
    ),
    (
        "tpch_q6",
        "SELECT SUM(l_extendedprice * l_discount) AS revenue "
        "FROM lineitem "
        "WHERE l_shipdate >= 731 AND l_shipdate < 1096 "
        "AND l_discount >= 0.05 AND l_discount <= 0.07 "
        "AND l_quantity < 24.0",
    ),
    (
        "tpcds_store_sales",
        "SELECT i_category, SUM(ss_sales_price * ss_quantity) AS revenue "
        "FROM store_sales "
        "JOIN item ON ss_item_sk = i_item_sk "
        "JOIN date_dim ON ss_sold_date_sk = d_date_sk "
        "WHERE d_year = 1994 "
        "GROUP BY i_category "
        "ORDER BY revenue DESC",
    ),
]

TABLES = [
    "customer",
    "orders",
    "lineitem",
    "item",
    "date_dim",
    "store_sales",
]


def label_for(sf: str) -> str:
    if sf == "tiny":
        return "tiny"
    if sf in ("1", "sf1"):
        return "sf1"
    if sf in ("10", "sf10"):
        return "sf10"
    raise SystemExit(f"unknown --sf {sf}")


def reported_ms(trials: list[float]) -> float:
    kept = trials[1:] if len(trials) > 1 else trials
    if not kept:
        return 0.0
    return float(statistics.median(kept))


def run_duckdb(data_dir: Path, repeats: int) -> tuple[str, int, list[dict]]:
    import duckdb

    con = duckdb.connect()
    input_bytes = 0
    for name in TABLES:
        path = data_dir / f"{name}.parquet"
        con.execute(
            f"CREATE TABLE {name} AS SELECT * FROM read_parquet(?)",
            [str(path)],
        )
        input_bytes += path.stat().st_size
        print(f"  loaded {name}", flush=True)
    queries = []
    for qid, sql in QUERIES:
        trials = []
        rows = 0
        for trial in range(repeats):
            start = time.perf_counter()
            result = con.execute(sql).fetchall()
            ms = (time.perf_counter() - start) * 1000.0
            rows = len(result)
            print(f"  {qid} trial {trial} {ms:.3f} ms rows={rows}", flush=True)
            trials.append(ms)
        queries.append(
            {
                "id": qid,
                "trials_ms": trials,
                "reported_ms": reported_ms(trials),
                "rows": rows,
            }
        )
    return duckdb.__version__, input_bytes, queries


def run_datafusion(data_dir: Path, repeats: int) -> tuple[str, int, list[dict]]:
    import datafusion
    import pyarrow.parquet as pq
    from datafusion import SessionContext

    ctx = SessionContext()
    input_bytes = 0
    for name in TABLES:
        path = data_dir / f"{name}.parquet"
        table = pq.read_table(path)
        batches = table.to_batches()
        ctx.register_record_batches(name, [batches])
        input_bytes += path.stat().st_size
        print(f"  loaded {name}", flush=True)
        del table
        del batches
    queries = []
    for qid, sql in QUERIES:
        trials = []
        rows = 0
        for trial in range(repeats):
            start = time.perf_counter()
            batches = ctx.sql(sql).collect()
            ms = (time.perf_counter() - start) * 1000.0
            rows = sum(b.num_rows for b in batches)
            print(f"  {qid} trial {trial} {ms:.3f} ms rows={rows}", flush=True)
            trials.append(ms)
        queries.append(
            {
                "id": qid,
                "trials_ms": trials,
                "reported_ms": reported_ms(trials),
                "rows": rows,
            }
        )
    version = getattr(datafusion, "__version__", "54")
    return str(version), input_bytes, queries


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sf", required=True)
    parser.add_argument("--dir", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--engine", choices=("duckdb", "datafusion"), required=True)
    parser.add_argument("--json", required=True)
    args = parser.parse_args()
    label = label_for(args.sf)
    data_dir = Path(args.dir) / label
    if not (data_dir / "lineitem.parquet").exists():
        raise SystemExit(f"missing {data_dir / 'lineitem.parquet'}; generate first")
    print(f"{args.engine} load {data_dir}", flush=True)
    if args.engine == "duckdb":
        version, input_bytes, queries = run_duckdb(data_dir, args.repeats)
    else:
        version, input_bytes, queries = run_datafusion(data_dir, args.repeats)
    body = {
        "engine": args.engine,
        "version": version,
        "sf": label,
        "repeats": args.repeats,
        "workers": "default",
        "input_bytes": input_bytes,
        "queries": queries,
    }
    out = Path(args.json)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(body, indent=2) + "\n")
    print(f"wrote {out}", flush=True)


if __name__ == "__main__":
    main()
