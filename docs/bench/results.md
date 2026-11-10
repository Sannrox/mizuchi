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

## Constrained profile

Measured 2026-11-10 with `BENCH_PROFILE=constrained scripts/bench-gb.sh`
(see [methodology.md](methodology.md#constrained-profile)). Every engine
ran with one worker, a 256 MiB memory cap with spill to a temp directory,
and the Parquet files as tables, so each query decodes its input. SF1 is
five trials and SF10 three, dropping the first and taking the median of
the rest, from one round per engine. Ratio is `reference_ms / mizuchi_ms`.
Host class: single-node workstation, 32 GiB RAM, 10 threads, shared with
other work (load about 4 to 16). Peak is the mizuchi memory-pool high
water (`QueryResult::peak_bytes`); the cap is 256.0 MiB. Peaks and the
`tpch_q1` row were re-measured after #226 and the `tpch_q6` row after
#227; the other times are from the first run.

### Constrained sf1

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio | mizuchi peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 561.362 | 197.512 | 0.352 | 159.288 | 0.284 | 0.2 |
| tpch_q3 | 265.446 | 203.939 | 0.768 | 192.544 | 0.725 | 13.0 |
| tpch_q6 | 105.157 | 86.497 | 0.823 | 78.368 | 0.745 | 0.1 |
| tpcds_store_sales | 62.195 | 65.775 | 1.058 | 53.055 | 0.853 | 23.3 |

Geomean ratio: DuckDB 0.697, DataFusion 0.601.

### Constrained sf10

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio | mizuchi peak MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 5640.877 | 1934.772 | 0.343 | 2076.113 | 0.368 | 0.2 |
| tpch_q3 | 2853.257 | 2934.011 | 1.028 | 2553.956 | 0.895 | 129.7 |
| tpch_q6 | 1052.965 | 1414.256 | 1.343 | 806.629 | 0.766 | 0.1 |
| tpcds_store_sales | 603.357 | 2528.256 | 4.190 | out of memory | n/a | 232.9 |

Geomean ratio: DuckDB 1.187; DataFusion 0.632 over the three queries it
finished. DataFusion fails `tpcds_store_sales` at SF10 under the cap: its
hash join build asks for 76 MB more with 229 MB held and does not spill.
DuckDB and mizuchi finish all four queries at both scales.

### Constrained operator ranking

`gb_bench profile --sf 10 --constrained`, exclusive time per operator.

| query | first | second | note |
| --- | --- | --- | --- |
| tpch_q1 | HashAggregate 4372 ms | Scan 1233 ms | 6 groups; before #226 the pool reached the cap and spilled |
| tpch_q3 | Scan `lineitem` 1598 ms | HashAggregate 946 ms | the `lineitem` scan copies 367 MB of filtered rows |
| tpch_q6 | Scan 1896 ms | HashAggregate 3 ms | filtered scan decode is the whole query; 1061 ms after #227 |
| tpcds_store_sales | Scan 299 ms | HashJoin 134 ms | already faster than DuckDB |

`tpch_q1` is the furthest behind (about 3x both references at both
scales), and its time is in the aggregate, not the decode. Since #226 the
aggregate charges its group table instead of every input batch it has
seen: q1 peaks at 0.2 MiB without spilling, and q6 at 0.1 MiB. Its time
did not change, so the gap is aggregate CPU, not spill. `tpch_q6` was
behind DataFusion by about 2x, all in the filtered Parquet scan. Its five
conjuncts each ran as a separate Parquet row-filter predicate, and
intersecting their fine-grained row selections cost more than the decode.
Since #227 the conjuncts run as one predicate over the three filter
columns: q6 now beats DuckDB at SF10 and is within 1.4x of DataFusion.
`tpch_q3`'s `lineitem` filter is a single conjunct, so its scan did not
change.
