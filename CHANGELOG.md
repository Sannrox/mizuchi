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
