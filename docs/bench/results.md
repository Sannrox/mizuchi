# Gigabyte-scale decision-support results

Measured 2026-10-11 with the method in [methodology.md](methodology.md); SF1
re-timed 2026-10-12 after parallel morsel execution (inner join probe, AVG
combine, Sort/Limit peel). Times are milliseconds. Ratio is
`reference_ms / mizuchi_ms`. Values below 1 mean the reference engine
finished faster.

mizuchi working set (sum of registered Arrow batch sizes): SF1 562_033_392
bytes, SF10 5_617_403_368 bytes. Snappy Parquet on disk: SF1 130_575_375 bytes,
SF10 1_280_729_644 bytes.

## sf1 (6_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 133.510 | 19.945 | 0.149 | 30.694 | 0.230 |
| tpch_q3 | 338.443 | 19.282 | 0.057 | 23.828 | 0.070 |
| tpch_q6 | 17.688 | 5.609 | 0.317 | 6.712 | 0.379 |
| tpcds_store_sales | 93.168 | 12.943 | 0.139 | 29.758 | 0.319 |

## sf10 (60_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 14906.694 | 180.610 | 0.012 | 263.299 | 0.018 |
| tpch_q3 | 35233.877 | 233.308 | 0.007 | 281.838 | 0.008 |
| tpch_q6 | 226.348 | 49.882 | 0.220 | 54.293 | 0.240 |
| tpcds_store_sales | 7518.938 | 104.787 | 0.014 | 391.952 | 0.052 |

Scan-heavy `tpch_q6` is the closest gap. Grouped aggregation (`tpch_q1`) and
the three-way joins (`tpch_q3`, `tpcds_store_sales`) remain behind the
reference engines at both scales.

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
