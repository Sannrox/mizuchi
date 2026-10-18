# Gigabyte-scale decision-support results

Measured 2026-10-11 with the method in [methodology.md](methodology.md); SF1
re-timed 2026-10-12 after unused-column prune and unique-key join fold
(five trials, drop first, median of the rest). Times are milliseconds.
Ratio is `reference_ms / mizuchi_ms`. Values below 1 mean the reference
engine finished faster.

mizuchi working set (sum of registered Arrow batch sizes): SF1 562_033_392
bytes, SF10 5_617_403_368 bytes. Snappy Parquet on disk: SF1 130_575_375 bytes,
SF10 1_280_729_644 bytes.

## sf1 (6_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 77.864 | 27.995 | 0.359 | 39.155 | 0.503 |
| tpch_q3 | 46.580 | 31.384 | 0.674 | 35.495 | 0.762 |
| tpch_q6 | 13.873 | 8.065 | 0.581 | 8.188 | 0.590 |
| tpcds_store_sales | 32.766 | 16.303 | 0.497 | 45.607 | 1.392 |

## sf10 (60_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 14906.694 | 180.610 | 0.012 | 263.299 | 0.018 |
| tpch_q3 | 35233.877 | 233.308 | 0.007 | 281.838 | 0.008 |
| tpch_q6 | 226.348 | 49.882 | 0.220 | 54.293 | 0.240 |
| tpcds_store_sales | 7518.938 | 104.787 | 0.014 | 391.952 | 0.052 |

SF1 DuckDB ratios are at or above 0.33 on every query. `tpcds_store_sales`
is faster than DataFusion at this scale. SF10 walls were not re-timed
(`bench-data/` is not in the default checkout).

## Parquet scan pushdown

Published SF1/SF10 walls above still time in-memory queries after a full
`read_parquet` load (method unchanged). File scans with
`ParquetReadOptions::filters` skip work before decode. Default tests in
`crates/mizuchi-io/src/read.rs` assert the skip ratios; compressed-byte
counts are writer-dependent so tests check `kept < total`.

| case | row groups kept / total | bytes | rows returned |
| --- | ---: | --- | ---: |
| clustered q6 `l_shipdate` in `[731, 1096)` | 1 / 4 | projected compressed bytes drop | 4 |
| `id > 2` on two Int64 groups | 1 / 2 | projected compressed bytes drop | 2 |
| `price >= 0.05` on two Float64 groups | 1 / 2 | projected compressed bytes drop | 2 |
| bloom equality `id = 3` (stats off) | 1 / 2 | projected compressed bytes drop | 1 |
| mixed-group `id > 2` (late materialization) | 1 / 1 | all groups kept; predicate columns first | 2 |
| page index `id >= 1000` (40 rows, 10-row pages) | 1 / 1 | unmatched pages skipped | 20 |

Filtered q6-style scans therefore read one clustered date group instead of
all four, and mixed-group filters decode surviving columns after the
predicate rather than the whole file.

## SF1 worker scaling

Same SF1 Parquet and method (`gb_bench run --sf 1 --workers N --repeats 3`).
Speedup is `ms_at_1 / ms_at_N`. Grouped aggregate (`tpch_q1`) is 3.75× at 4
workers and 5.60× at 8 on a 10-thread host. Scan-heavy ungrouped `tpch_q6`
is 2.73× / 3.67× at those widths.

| query | 1 worker ms | 2 ms (×) | 4 ms (×) | 8 ms (×) |
| --- | ---: | ---: | ---: | ---: |
| tpch_q1 | 758.427 | 388.075 (1.95) | 202.452 (3.75) | 135.533 (5.60) |
| tpch_q6 | 58.245 | 37.215 (1.57) | 21.356 (2.73) | 15.853 (3.67) |
| tpch_q3 | 404.547 | 339.732 (1.19) | 297.410 (1.36) | 282.073 (1.43) |
| tpcds_store_sales | 184.210 | 123.158 (1.50) | 94.206 (1.96) | 92.131 (2.00) |

## Kernel micro-benchmarks

`cargo run --release -p mizuchi --example kernel_micro` times filter,
projection, ungrouped SUM/COUNT, grouped COUNT/SUM, and an inner Int64 join
over 1_000_000 Int64 rows (same host as the SF1 table). Default `cargo test`
drives the same paths for correctness
(`evaluates_one_million_rows_with_kernels`,
`kernel_filter_project_and_ungrouped_agg_one_million_rows`,
`ungrouped_sum_of_one_million_int64`,
`inner_join_one_hundred_thousand_int64_keys`) without recording wall times.

| path | ms | rows |
| --- | ---: | ---: |
| filter (`id >= 999000`) | 0.128 | 1_000 |
| project (`id + 1`) | 0.331 | 1_000_000 |
| ungrouped SUM/COUNT | 0.065 | 1 |
| grouped COUNT/SUM (8 groups) | 0.136 | 8 |
| inner join (1_000_000 × 1_000 Int64 keys) | 0.167 | 1 |
