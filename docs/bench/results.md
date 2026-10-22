# Gigabyte-scale decision-support results

Measured 2026-10-22 at tip with the method in [methodology.md](methodology.md),
after the q1 grouped-aggregate fold and the shared join gather (#160). SF1 is
five trials and SF10 three, dropping the first and taking the median of the
rest. Times are milliseconds. Ratio is `reference_ms / mizuchi_ms`. Values
below 1 mean the reference engine finished faster. The host was shared with
other work (load about 7 to 12 on 10 threads), so expect run-to-run noise.

mizuchi working set (sum of registered Arrow batch sizes): SF1 562_033_392
bytes, SF10 5_617_403_368 bytes. Snappy Parquet on disk: SF1 130_575_375 bytes,
SF10 1_280_729_644 bytes.

## sf1 (6_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 28.238 | 17.940 | 0.635 | 28.775 | 1.019 |
| tpch_q3 | 53.511 | 19.028 | 0.356 | 22.746 | 0.425 |
| tpch_q6 | 8.628 | 5.242 | 0.608 | 6.360 | 0.737 |
| tpcds_store_sales | 36.816 | 12.413 | 0.337 | 27.631 | 0.751 |

## sf10 (60_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 332.464 | 258.536 | 0.778 | 380.353 | 1.144 |
| tpch_q3 | 1616.202 | 333.992 | 0.207 | 394.766 | 0.244 |
| tpch_q6 | 118.054 | 74.497 | 0.631 | 62.997 | 0.534 |
| tpcds_store_sales | 458.219 | 185.731 | 0.405 | 511.083 | 1.115 |

SF1 DuckDB ratios are at or above 0.33 on every query. At SF10, q1 is faster
than DataFusion, and q1, q6 and `tpcds_store_sales` are within 3x of DuckDB.
`tpch_q3` at SF10 is the remaining limiter: its probe join and its
million-group aggregate dominate, and its wall varies between runs (about 0.7
to 1.6 s). It is tracked in [#168](https://github.com/Sannrox/mizuchi/issues/168)
under #130.

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
median of three runs per width on the same shared 10-thread host. Speedup is
`ms_at_1 / ms_at_N`. The grouped aggregate (`tpch_q1`) folds each aggregate
column in one pass per batch, so its single-worker time is lower than before
and the speedup ratio smaller: 2.41× at 4 workers and 2.87× at 8. Scan-heavy
ungrouped `tpch_q6` is 2.41× / 3.17×. The join queries no longer slow down
with workers (#159): a streamed hash-join probe spreads whole morsel groups
across workers, and build sides under 131,072 rows stay one table. Runtime
filters no longer cost single-worker time (#164). `tpch_q3` folds its join
and float `SUM` on all workers (#165). `tpcds_store_sales` stays flat because
its join builds are serial.

| query | 1 worker ms | 2 ms (×) | 4 ms (×) | 8 ms (×) |
| --- | ---: | ---: | ---: | ---: |
| tpch_q1 | 90.377 | 62.194 (1.45) | 37.543 (2.41) | 31.531 (2.87) |
| tpch_q6 | 31.593 | 22.937 (1.38) | 13.131 (2.41) | 9.959 (3.17) |
| tpch_q3 | 104.370 | 56.709 (1.84) | 44.300 (2.36) | 47.807 (2.18) |
| tpcds_store_sales | 34.940 | 40.673 (0.86) | 34.356 (1.02) | 33.726 (1.04) |

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
