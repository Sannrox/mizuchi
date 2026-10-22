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

Same SF1 Parquet and method (`gb_bench run --sf 1 --workers N --repeats 5`),
median of three runs per width on a shared 10-thread host (load about 17,
so the 8-worker column is the noisiest).
Speedup is `ms_at_1 / ms_at_N`. Grouped aggregate (`tpch_q1`) is 2.81× at 4
workers and 3.13× at 8. Scan-heavy ungrouped `tpch_q6`
is 2.03× / 2.41× at those widths. The join queries no longer slow
down with workers (#159): a streamed hash-join probe now spreads whole morsel
groups across workers instead of spawning threads per morsel, and build sides
under 131,072 rows stay one table. Runtime filters no longer cost single-worker
time (#164). `tpch_q3` folds its join and float `SUM` on all workers (#165): 2.37×
at 4 workers; at 8 it measured 1.78× here and 2.0 to 2.4× on a quieter host.

| query | 1 worker ms | 2 ms (×) | 4 ms (×) | 8 ms (×) |
| --- | ---: | ---: | ---: | ---: |
| tpch_q1 | 597.368 | 337.977 (1.77) | 212.962 (2.81) | 190.848 (3.13) |
| tpch_q6 | 32.373 | 25.259 (1.28) | 15.932 (2.03) | 13.435 (2.41) |
| tpch_q3 | 124.447 | 64.904 (1.92) | 52.582 (2.37) | 70.036 (1.78) |
| tpcds_store_sales | 51.267 | 53.365 (0.96) | 43.058 (1.19) | 45.580 (1.12) |

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
