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
