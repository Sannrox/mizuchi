# Decision-support limiter profiles

Where time and memory go in the four suite queries, and which follow-up Issues
own each limiter. Method: [methodology.md](methodology.md). Published wall
times: [results.md](results.md).

The CPU profile of a physical plan is exclusive operator time: inclusive
`elapsed_ns` from `Session::query_metrics` minus the children's inclusive
times, ranked highest first. The allocation profile is the query memory-pool
high-water (`peak_bytes`). Join and aggregate reserve only when a
`memory_limit` is set, so `gb_bench profile` uses a 24 GiB cap (above the
SF1 working set). `gb_bench run` does not set a cap. `peak_bytes` on a
blocking operator is the pool high-water observed when that operator
finished. Exclusive time subtracts pipelined children only; blocking
children (join/aggregate/sort) already finished and must not be subtracted. Default `cargo test` drives this path
on the tiny in-memory generator (`tests/suite_profile.rs`). Release Parquet
runs use `bash scripts/profile-suite.sh tiny` or `1` when `bench-data/` is
present; those files stay gitignored.

`EXPLAIN ANALYZE` times the pull operator tree. Combinable aggregates and
scan/filter/project pipelines may use morsel workers on `execute_sql`; joins,
sorts, and limits stay on the single-thread pull path.

## Operator mix

Same physical shape at every generator scale (tiny, SF1, SF10).

| query | operators | SF1 wall ms | DuckDB ratio |
| --- | --- | ---: | ---: |
| tpch_q1 | HashAggregate → Sort over `lineitem` Scan (predicate pushed into Scan) | 923.214 | 0.022 |
| tpch_q3 | two HashJoins, HashAggregate, Sort/Limit over customer/orders/lineitem | 1536.120 | 0.013 |
| tpch_q6 | HashAggregate (ungrouped SUM) over `lineitem` Scan (predicate pushed into Scan) | 18.383 | 0.305 |
| tpcds_store_sales | two HashJoins, HashAggregate, Sort over store_sales/item/date_dim | 695.238 | 0.019 |

Scan-only `tpch_q6` is the closest gap. Grouped aggregation and the three-way
joins dominate the rest of the suite.

## Breakdown

Categories below match the physical work, not separate timers. Exclusive
operator time folds expression eval into Filter / HashAggregate / HashJoin,
and copies into the operator that allocates.

### Scan

In-memory table Scan after `read_parquet` + `Session::register`. Load time is
outside the query (same as the wall-time method). At SF1 the working set is
562_033_392 bytes. Scan exclusive time is small next to hash build/probe and
grouped updates. Parquet projection, predicate, and row-group pruning would
cut decode and the registered working set; they do not run on this in-memory
path today.

### Expression eval

q1 evaluates several arithmetic aggregates per `lineitem` row (`SUM` of
`l_extendedprice * (1 - l_discount) * (1 + l_tax)` and related). q6 is a
selective Filter plus one product. Group keys and aggregate arguments are
extracted as `Scalar` values per row inside HashAggregate, then boxed into
the hash table. That per-row path is the expression limiter.

### Hash aggregate

q1 is a two-key grouped aggregate over six million `lineitem` rows at SF1.
q3 and store_sales group the join output. HashAggregate exclusive time plus
the SF1 923 ms wall for q1 put grouped aggregation in the top limiters.
Updates walk each row, look up a `Vec<Scalar>` key, and mutate per-group
accumulators.

### Hash join

q3 (1536 ms) and store_sales (695 ms) each build two HashJoins. The right
input is collected and hashed; the left is probed. Both sides are fully
materialized before probe. Null keys never match. This is the largest wall
in the suite. Join is not a morsel pipeline, so extra workers do not split
the build.

### Copies

Join `collect` clones Arrow batches into the build/probe sides. Hash keys
clone `Scalar` (including `Utf8(String)`) into the table. Spill reload
clones again. Filter/project emit new batches from Arrow compute; those
copies are smaller than the hash-table copies on q1/q3/store_sales.

### Scheduling

`Session::with_workers` runs independent scan/filter/project morsels and
combinable aggregates. HashJoin, Sort, and Limit stay single-threaded pull.
q3 and store_sales therefore spend their 1.5 s / 0.7 s on one thread
regardless of the worker count used for the published SF1 numbers.

## Ranked limiters

Highest impact first, feeding the follow-up Issues. Rank is exclusive
operator time at SF1 from `gb_bench profile --sf 1`.

1. **Hash join build/probe** on q3 (1663.2 ms + 348.5 ms exclusive) and
   store_sales (663.0 ms + 171.9 ms). Peak pool 2.62 GiB / 4.10 GiB.
   [#104](https://github.com/Sannrox/mizuchi/issues/104)
2. **Hash aggregate updates** on q1 (954.9 ms exclusive, peak 349 MiB).
   [#104](https://github.com/Sannrox/mizuchi/issues/104)
3. **Per-row expression and aggregate kernels** (Scalar boxing inside those
   same operators; q1 arithmetic, q6 Filter). [#103](https://github.com/Sannrox/mizuchi/issues/103)
4. **Single-thread join/sort** (morsel workers unused on the join plans).
   [#105](https://github.com/Sannrox/mizuchi/issues/105)
5. **Batch and key copies** between collect, hash tables, and spill (the
   2–4 GiB peaks). [#107](https://github.com/Sannrox/mizuchi/issues/107)
6. **Scan/decode working set** (Scan exclusive is 0.5 ms on q1 and 0.09 ms
   on q6; Parquet projection and row-group skip would shrink the registered
   tables). [#106](https://github.com/Sannrox/mizuchi/issues/106)

q6 stays the control: ungrouped SUM over a pushed-down Scan, 13.1 ms
HashAggregate exclusive at SF1, published wall 18.4 ms, ratio 0.305.
Closing (1)–(3) is what the 2–3× target needs on q1/q3.

## Tiny in-memory profile (default tests)

`tests/suite_profile.rs` runs `query_metrics` at generator `s=1` (48
`lineitem` rows) with a 64 MiB memory cap so reservations are recorded.
Operator names, HashJoin in the top exclusive times on the join queries, and
`peak_bytes > 0` on those queries are asserted; nanoseconds are not. On tiny
data Sort can outrank HashJoin on store_sales; that ranking is noise. SF1
walls plus the operator mix set the published limiter order. Reproduce a
Parquet profile with `bash scripts/profile-suite.sh tiny`.

Release `--sf tiny` exclusive rank (`gb_bench profile`, 24 GiB cap):

| query | peak_bytes | exclusive rank (name, ns) |
| --- | ---: | --- |
| tpch_q1 | 3640 | Sort 250334, HashAggregate 170166, Scan 417 |
| tpch_q3 | 2912 | Sort 28376, HashJoin 22541, HashJoin 15876 |
| tpch_q6 | 1056 | HashAggregate 3416, Scan 126 |
| tpcds_store_sales | 1280 | Sort 29291, HashJoin 8042, HashJoin 7418 |

Release `--sf 1` exclusive rank (same harness; 6_000_000 `lineitem` rows).
Times in milliseconds.

| query | peak_bytes | exclusive rank (name, ms) |
| --- | ---: | --- |
| tpch_q1 | 349_244_280 | HashAggregate 954.862, Scan 0.505, Sort 0.224 |
| tpch_q3 | 2_619_712_776 | HashJoin 1663.212, HashJoin 348.528, HashAggregate 64.501 |
| tpch_q6 | 30_207_160 | HashAggregate 13.074, Scan 0.093 |
| tpcds_store_sales | 4_100_097_375 | HashJoin 662.997, HashJoin 171.930, HashAggregate 65.228 |

HashAggregate / HashJoin exclusive time at SF1 matches the published walls
in [results.md](results.md) (q1 923 ms, q3 1536 ms, q6 18 ms, store_sales
695 ms). Exclusive time skips blocking children so nested joins are not
undercounted.
