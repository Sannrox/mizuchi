# Changelog

All notable changes to this project are documented here.

## Unreleased

### Added

- Workspace skeleton with a placeholder library crate.
- ADR 0001 (record architecture decisions) and ADR 0002 (own query engine as a side project).
- Arrow record-batch API: schema round-trip for supported types, owned and borrowed batches that wrap standard Arrow arrays without a private memory format, and a configurable max-rows policy (default 4096).
- Scalar expressions evaluated column-at-a-time with Arrow compute kernels (arithmetic, comparison, boolean, cast, CASE, null handling, string basics).
- SQL parser: sqlparser-rs with the PostgreSQL dialect; syntax errors and unimplemented SQL fail closed (ADR 0003).
- Logical plan bound from SQL: scan, filter, project, aggregate, join, sort, limit, union; EXPLAIN text; fail-closed analysis errors.
- Pull-based vectorized scan, filter, and projection over in-memory tables; end-to-end SELECT ... WHERE (ADR 0004).
- Hash aggregate: grouped and ungrouped COUNT, SUM, MIN, MAX, AVG, and COUNT DISTINCT over Arrow batches, including empty input and null grouping keys.
- Hash join: inner, left, right, full, semi, and anti over Arrow batches. Null keys never match; CROSS JOIN is a nested loop.
- Sort, top-k, and limit/offset over Arrow batches. ORDER BY honors ASC/DESC and NULLS FIRST/LAST; LIMIT after ORDER BY sorts only the kept prefix.
- Parquet and CSV readers that yield Arrow batches. Parquet projection decodes only requested columns; row-group skip is a footer-stats hook. CSV can infer types or keep Utf8.
- Rule-based optimizer: constant folding, boolean simplification, filter merge, limit pushdown through projection, and redundant projection removal.
- Predicate and projection pushdown into scans and inner joins. Parquet readers skip row groups using footer min/max.
- Table and column statistics (row count, NDV, Int64 min/max) and cost-based inner-join reordering. Dynamic programming for up to 12 tables; greedy above that.
- Stage planning: hash-partition exchanges at equi-joins and grouped aggregates, shuffle-map and result stages, and a single-process worker pool.
- Shuffle blocks written as Arrow IPC files in a caller-provided directory; job subdirectories are removed when the query finishes.
- Per-query memory pool with reservations. Sort, join, and aggregate spill Arrow IPC runs to a caller-provided directory and fail closed when the limit cannot be honoured.
- Morsel work queue across a bounded thread pool for scan/filter/project and combinable aggregates, plus cooperative cancellation checked between morsels.
- SQLLogicTest runner and corpus under `tests/slt`. CI runs the adopted subset; known gaps are listed in `gaps.slt` behind `skipif mizuchi`.
- TPC-H and TPC-DS style decision-support harness: a tiny in-memory generator, a fixed query set, and JSON with per-query time and memory. Larger scales stay out of the default `cargo test` run.
- Decision-support comparison against DuckDB and DataFusion: same SQL and tables, JSON with result diffs and timing ratios. CI uses committed goldens so the default test run stays offline.
- Python DataFrame API via PyO3: lazy select, filter, group_by, join, sort, and SQL, with a pip-installable wheel built in CI.
- LibFuzzer targets for SQL parse, plan, execute, and a supported-SQL grammar. CI runs them for a bounded time; crashes land as regression tests.
- Engine owner shares: reserved memory and worker slices so concurrent owners cannot starve each other.
- Stage tasks retry after a worker failure and reuse committed shuffle files. Partial writes from a failed attempt are discarded.
- Adaptive skew handling: after shuffle-map, oversized join partitions are split by grouping map outputs and the matching other-side partition is replicated. `EXPLAIN ANALYZE` via `Session::explain_analyze` reports the splits.
- Python scalar and aggregate UDFs over Arrow arrays, plus zero-copy interchange through the Arrow C Data Interface (`register_arrow` / `to_arrow`).
- ADR 0005: incremental execution as micro-batches on the same hash aggregate. `MicroBatchAgg` folds append-only batches and emits the complete group table.
- Query result cache on `execute_sql`: identical plans reuse stored Arrow batches while scanned table generations match. Re-registering a table invalidates entries that used it.
- ADR 0006: GPU operators are a no-go for this phase. The default decision-support suite is too small for device copy to pay off; revisit when inputs are gigabyte-scale and a runner grants a device.
- Gigabyte-scale decision-support baseline: SF1 and SF10 TPC-H/TPC-DS-style queries versus DuckDB and DataFusion on the same Snappy Parquet. Reproduce with `scripts/bench-gb.sh`; committed times and method in `docs/bench/`. Optional `workflow_dispatch` smoke at `--sf tiny`; default `cargo test` is unchanged.
- Physical operators implement a pull `Operator` trait. `Session::with_lowering` selects an implementation per operator; CPU scan/filter/project/limit stay the default. Dispatch is once per batch.
