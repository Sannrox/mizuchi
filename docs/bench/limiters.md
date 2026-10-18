# Decision-support limiter profiles

Where time and memory go in the four suite queries, and which follow-up Issues
own each limiter. Method: [methodology.md](methodology.md). Published wall
times: [results.md](results.md).

The CPU profile of a physical plan is exclusive operator time: inclusive
`elapsed_ns` from `Session::query_metrics` minus the children's inclusive
times, ranked highest first. The allocation profile is the query memory-pool
high-water (`peak_bytes`) and bytes rewritten by concat/take
(`copied_bytes`). Join and aggregate reserve only when a
`memory_limit` is set, so `gb_bench profile` uses a 24 GiB cap (above the
SF1 working set). `gb_bench run` does not set a cap. `peak_bytes` on a
blocking operator is the pool high-water observed when that operator
finished. Exclusive time subtracts pipelined children only; blocking
children (join/aggregate/sort) already finished and must not be subtracted. Default `cargo test` drives this path
on the tiny in-memory generator (`tests/suite_profile.rs`). Release Parquet
runs use `bash scripts/profile-suite.sh tiny` or `1` when `bench-data/` is
present; those files stay gitignored.

`EXPLAIN ANALYZE` times the pull operator tree. `execute_sql` with
`Session::with_workers` > 1 runs scan/filter/project morsels, combinable
aggregates (AVG merges per-thread sum and count), inner hash-join probe,
and wrapping sort/limit on that many threads. Integer equi-keys in a tight
min/max span use a compact range map; unique i64 keys probe with a direct
index. Grouped aggregates assign group slots then update in a tight loop.

## Operator mix

Same physical shape at every generator scale (tiny, SF1, SF10).

| query | operators | SF1 wall ms | DuckDB ratio |
| --- | --- | ---: | ---: |
| tpch_q1 | HashAggregate → Sort over `lineitem` Scan (predicate pushed into Scan) | 77.864 | 0.359 |
| tpch_q3 | two HashJoins, HashAggregate, Sort/Limit over customer/orders/lineitem | 46.580 | 0.674 |
| tpch_q6 | HashAggregate (ungrouped SUM) over `lineitem` Scan (predicate pushed into Scan) | 13.873 | 0.581 |
| tpcds_store_sales | two HashJoins, HashAggregate, Sort over store_sales/item/date_dim | 32.766 | 0.497 |

SF1 meets the 0.33 DuckDB-ratio target. Grouped aggregation (`tpch_q1`) is
the remaining gap versus the same-host reference.

## Breakdown

Categories below match the physical work, not separate timers. Exclusive
operator time folds expression eval into Filter / HashAggregate / HashJoin,
and copies into the operator that allocates.

### Scan

In-memory table Scan after `read_parquet` + `Session::register`. Load time is
outside the query (same as the wall-time method). At SF1 the working set is
562_033_392 bytes. Scan exclusive time is small next to hash build/probe and
grouped updates. Parquet projection, footer/page/bloom predicate skip, and
late materialization run on file and object-store reads
(`ParquetReadOptions::filters`); the GB bench still loads full tables so
published walls isolate compute. Skip ratios: [results.md](results.md).

### Expression eval

q1 evaluates several arithmetic aggregates per `lineitem` row (`SUM` of
`l_extendedprice * (1 - l_discount) * (1 + l_tax)` and related). q6 is a
selective Filter plus one product. Filter and projection already run Arrow
compute over whole arrays. Ungrouped aggregate updates (q6) fold COUNT/SUM/AVG
kernels over those arrays. Grouped lookup (q1) indexes single-byte Utf8 keys with a
perfect hash (no per-group `Scalar`). Wider domains hash in place and
radix-partition so workers merge disjoint partitions.

### Hash aggregate

q1 is a two-key grouped aggregate over six million `lineitem` rows at SF1.
q3 and store_sales group the join output. HashAggregate exclusive time plus
the SF1 wall for q1 put grouped aggregation in the top limiters. q1's
CHAR keys use a perfect-hash index; high-cardinality groups radix-partition
and merge in parallel.

### Hash join

q3 (407 ms) and store_sales (196 ms) each build two HashJoins. Equality keys
hash in place. A single integer column in a tight min/max span uses a
compact range map; unique i64 keys probe with a direct index. Inner joins
build the smaller estimated side (right). After that build, integer join
keys become a min/max or small IN-list runtime filter on the probe-side
scan. `EXPLAIN ANALYZE` prints `runtime_filter=[...]` on the scan and the
scan's `rows=` count is the rows that survive the filter. In-memory scans
skip morsels that cannot match; Parquet footer prune treats IN-list as OR.
Null keys never match. Inner probe ranges run on worker threads.

### Copies

An unbounded join keeps probe morsels and concatenates only the smaller
build side, and only when that side arrives as more than one morsel. A
contiguous gather is a slice of the parent arrays. `copied_bytes` on
`EXPLAIN ANALYZE` / `query_metrics` counts bytes written by those
concatenations (slices add 0). Before this change both sides were
concatenated on the unbounded path, so a multi-morsel probe paid a full
rewrite. Grouping keys still allocate `Scalar` on insert. Spill reload
clones again. Filter/project emit new batches from Arrow compute; those
copies are smaller than a full probe rewrite on q3/store_sales.

### Scheduling

`Session::with_workers` runs scan/filter/project morsels, combinable
aggregates, inner join probe, and wrapping sort/limit. Join build stays
single-threaded. SF1 q1 is 5.6× at 8 workers versus 1; q3/store_sales
scale about 1.4–2.0× because build and materialization remain serial.

## Ranked limiters

Highest impact first, feeding the follow-up Issues. Rank is exclusive
operator time at SF1 from `gb_bench profile --sf 1`.

1. **Hash aggregate updates** on q1 (769.3 ms exclusive on the pull
   profile; `execute_sql` wall 133.5 ms at 10 workers). Peak 349 MiB.
   [#104](https://github.com/Sannrox/mizuchi/issues/104)
2. **Hash join build/probe** on q3 (240.2 ms + 64.4 ms exclusive pull) and
   store_sales (103.0 ms + 74.1 ms). Peak pool 2.62 GiB / 4.10 GiB. Probe
   ranges are parallel on `execute_sql`; build is not.
   [#104](https://github.com/Sannrox/mizuchi/issues/104)
3. **Columnar expression and aggregate kernels** (whole-array filter/project
   and ungrouped COUNT/SUM/AVG/MIN/MAX; grouped lookup hashes Utf8 in place).
   [#103](https://github.com/Sannrox/mizuchi/issues/103)
4. **Join build and materialization** (probe is parallel; both sides still
   fully collected). [#105](https://github.com/Sannrox/mizuchi/issues/105)
5. **Batch and key copies** between collect, hash tables, and spill (the
   2–4 GiB peaks). Probe concat on the unbounded path is gone;
   `copied_bytes` now shows remaining rewrites.
   [#107](https://github.com/Sannrox/mizuchi/issues/107)
6. **Scan/decode working set** (Scan exclusive is 0.77 ms on q1 and 0.06 ms
   on q6 after a full in-memory load). Parquet file scans now project,
   skip row groups and pages, and late-materialize remaining columns
   ([#106](https://github.com/Sannrox/mizuchi/issues/106)).

q6 stays the control: ungrouped SUM over a pushed-down Scan, 7.4 ms
HashAggregate exclusive at SF1, published wall 17.7 ms.
q1 wall dropped from 780 ms to 134 ms with 10 workers.

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
| tpch_q1 | 349_244_280 | HashAggregate 769.286, Scan 0.769, Sort 0.239 |
| tpch_q3 | 2_619_712_776 | HashJoin 240.237, HashJoin 64.440, HashAggregate 58.500 |
| tpch_q6 | 30_207_160 | HashAggregate 7.441, Scan 0.063 |
| tpcds_store_sales | 4_100_097_375 | HashJoin 103.013, HashJoin 74.112, HashAggregate 23.536 |

HashAggregate / HashJoin exclusive time at SF1 matches the published walls
in [results.md](results.md) (q1 780 ms, q3 407 ms, q6 16 ms, store_sales
196 ms). Exclusive time skips blocking children so nested joins are not
undercounted.
