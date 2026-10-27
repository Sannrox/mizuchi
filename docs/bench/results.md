# Gigabyte-scale decision-support results

Measured 2026-10-27 at tip with the method in [methodology.md](methodology.md),
after the streamed join probe, the dense integer join build and the
memory-limited top-k (#168). SF1 is five trials and SF10 three,
dropping the first and taking the median of the rest. Each engine ran in
three interleaved rounds; the table is the median of the three rounds.
Times are milliseconds. Ratio is `reference_ms / mizuchi_ms`. Values below 1
mean the reference engine finished faster. The host was shared with other
work (load about 9 to 17 on 10 threads), so expect run-to-run noise.

mizuchi working set (sum of registered Arrow batch sizes): SF1 562_033_392
bytes, SF10 5_617_403_368 bytes. Snappy Parquet on disk: SF1 130_575_375 bytes,
SF10 1_280_729_644 bytes.

## sf1 (6_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 27.095 | 18.500 | 0.683 | 26.226 | 0.968 |
| tpch_q3 | 26.803 | 18.545 | 0.692 | 21.664 | 0.808 |
| tpch_q6 | 8.717 | 5.173 | 0.593 | 6.183 | 0.709 |
| tpcds_store_sales | 19.991 | 11.809 | 0.591 | 27.260 | 1.364 |

## sf10 (60_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 249.300 | 177.744 | 0.713 | 247.261 | 0.992 |
| tpch_q3 | 279.272 | 216.979 | 0.777 | 247.187 | 0.885 |
| tpch_q6 | 82.850 | 43.626 | 0.527 | 48.418 | 0.584 |
| tpcds_store_sales | 190.790 | 103.523 | 0.543 | 364.664 | 1.911 |

Every query is within 3x of DuckDB at SF1 and SF10. Every kept `tpch_q3`
SF10 trial took between 260 and 318 ms across the three rounds; before #168
it was about 500 to 630 ms in the same window (DuckDB ratio 0.38), and up to
1.6 s on a busier host. Its orders-customer build now uses one direct index
on the dense customer key instead of hash partitions, the scans that feed a
join build or probe filter on all workers, and a partitioned probe gathers
only matched rows. The `gb_bench profile --sf 10` peak for q3 is 127 MB, down
from 7.84 GB: its top-10 sort held every slice of the 1M-group input, each
charged for the whole parent, and now keeps a running top-k instead.

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
column in one pass per batch, so its single-worker time is low and the
speedup ratio smaller: 2.49× at 4 workers and 3.24× at 8. Scan-heavy
ungrouped `tpch_q6` is 2.32× / 3.51×. The join queries no longer slow down
with workers (#159): a streamed hash-join probe spreads whole morsel groups
across workers, and build sides under 131,072 rows stay one table. Runtime
filters no longer cost single-worker time (#164). `tpch_q3` folds its join
and float `SUM` on all workers (#165). Scans that feed a join build or
probe filter on all workers (#168), so `tpcds_store_sales` now scales too.

| query | 1 worker ms | 2 ms (×) | 4 ms (×) | 8 ms (×) |
| --- | ---: | ---: | ---: | ---: |
| tpch_q1 | 90.454 | 56.120 (1.61) | 36.270 (2.49) | 27.882 (3.24) |
| tpch_q6 | 30.648 | 22.208 (1.38) | 13.224 (2.32) | 8.731 (3.51) |
| tpch_q3 | 102.804 | 42.228 (2.43) | 31.336 (3.28) | 25.731 (4.00) |
| tpcds_store_sales | 34.260 | 27.764 (1.23) | 22.664 (1.51) | 19.523 (1.75) |

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
