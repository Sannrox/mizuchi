# ADR 0010: Pluggable catalog with namespaces and partition metadata

- Status: Accepted
- Date: 2026-10-11
- Supersedes: —
- Source: [Issue #73](https://github.com/Sannrox/mizuchi/issues/73)

## Context

The planner catalog was an in-memory map from a single table name to a
schema and stats. Callers need namespaces, persistent metadata, Hive
partition inventories that drive scan pruning, and a place for an
embedder to plug in an external catalog. `Catalog::new` and
`register("t")` already appear in plan snapshots and must keep working.

## Decision

- Unqualified names resolve in catalog `mizuchi`, namespace `public`.
  Two-part names are `namespace.table`. Three-part names are
  `catalog.namespace.table`. The name stored on a scan is the shortest
  SQL form that still resolves (`t`, `sales.t`, or `lake.sales.t`).
- [`Catalog`] stays the session facade. Memory is the default.
  [`Catalog::open_file`] mirrors the same map to
  `{catalog}/{namespace}/{table}.json` plus an Arrow schema sidecar.
  [`CatalogProvider`] is the backend trait; an embedder implements it
  for an external service.
- Partition columns and directories live on [`TableStats`]. Scan
  reports fill empty filesystem partitions from the catalog and then
  prune with the same Hive-path rules as object-store reads.
- `CREATE TABLE` / `DROP TABLE` / `ALTER TABLE` (add or rename a
  column) run through [`Session::execute_ddl`]. `SHOW TABLES` is a
  query on `execute_sql`. Access control stays with the embedder.

## Consequences

- Existing `Catalog::new()` / `register("t")` / `plan_sql` tests keep
  their unqualified scan names.
- File catalogs are a directory the caller already named; the engine
  does not open a network catalog. Ident parts cannot be `.`, `..`, or
  contain path separators. Planning copies drop the persist directory
  so `SELECT` does not rewrite sidecars.
- DDL mutates the session (`execute_ddl` takes `&mut self`). Ordinary
  `execute_sql` stays `&self`. `ALTER TABLE` reshapes in-memory batches
  to the new schema and invalidates the result cache.

## Rejected alternatives

- **Replace `Catalog` with a new type.** Would break every plan test
  and the public constructor.
- **Store partitions only on filesystem `TableSource`.** Catalog-only
  tables could not drive pruning.
- **Import the object-store prune helper from `plan`.** `store` already
  depends on `plan`; that import is a cycle.

[`Catalog`]: ../../crates/mizuchi/src/plan/catalog.rs
[`Catalog::open_file`]: ../../crates/mizuchi/src/plan/catalog.rs
[`CatalogProvider`]: ../../crates/mizuchi/src/plan/catalog.rs
[`TableStats`]: ../../crates/mizuchi/src/plan/stats.rs
[`Session::execute_ddl`]: ../../crates/mizuchi/src/exec/mod.rs
