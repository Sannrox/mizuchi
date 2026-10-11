# Gigabyte-scale decision-support results

Measured 2026-10-11 with the method in [methodology.md](methodology.md). Times
are milliseconds. Ratio is `reference_ms / mizuchi_ms`. Values below 1 mean the
reference engine finished faster.

mizuchi working set (sum of registered Arrow batch sizes): SF1 562_033_392
bytes, SF10 5_617_403_368 bytes. Snappy Parquet on disk: SF1 130_575_375 bytes,
SF10 1_280_729_644 bytes.

## sf1 (6_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 923.214 | 19.945 | 0.022 | 30.694 | 0.033 |
| tpch_q3 | 1536.120 | 19.282 | 0.013 | 23.828 | 0.016 |
| tpch_q6 | 18.383 | 5.609 | 0.305 | 6.712 | 0.365 |
| tpcds_store_sales | 695.238 | 12.943 | 0.019 | 29.758 | 0.043 |

## sf10 (60_000_000 `lineitem` rows)

| query | mizuchi ms | DuckDB ms | DuckDB ratio | DataFusion ms | DataFusion ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| tpch_q1 | 14906.694 | 180.610 | 0.012 | 263.299 | 0.018 |
| tpch_q3 | 35233.877 | 233.308 | 0.007 | 281.838 | 0.008 |
| tpch_q6 | 226.348 | 49.882 | 0.220 | 54.293 | 0.240 |
| tpcds_store_sales | 7518.938 | 104.787 | 0.014 | 391.952 | 0.052 |

Scan-heavy `tpch_q6` is the closest gap. Grouped aggregation (`tpch_q1`) and
the three-way joins (`tpch_q3`, `tpcds_store_sales`) remain far behind the
reference engines at both scales.
