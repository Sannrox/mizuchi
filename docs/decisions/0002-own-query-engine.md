# ADR 0002: Own query engine as a side project

- Status: Accepted
- Date: 2026-10-10
- Supersedes: —

## Context

Sandboxed transform runners such as [kako](https://github.com/Sannrox/kako)
run SQL and DataFrame jobs on existing embedded engines (DuckDB, DataFusion,
Polars). Those engines are excellent, but their internals are not designed
around the needs of a multi-tenant, sandboxed runner:

- **Sandbox fit:** predictable memory accounting, spill locations, thread
  counts and cancellation that a sandbox can enforce, with no hidden file or
  network access.
- **Tenancy:** per-tenant resource limits and fair scheduling inside one
  engine, not only around it.
- **Lineage:** stable access to the logical and executed plan, so a runner can
  record exactly which inputs, columns and operators produced an output.

## Decision

Build our own engine, mizuchi, as a side project:

- Start from scratch but stay Arrow-compatible: Arrow memory, Arrow IPC and
  Parquet at the boundary. Reuse standard crates (arrow-rs, parquet,
  sqlparser-rs) rather than reimplementing formats or parsing.
- Single-node first; distributed execution later.
- Benchmark continuously against DuckDB and DataFusion on the same queries
  and data.
- **It must not block the main plan.** kako keeps using existing engines
  until mizuchi passes a defined bar (correctness, performance and resource
  behavior), documented in kako's engine requirements. mizuchi then plugs in
  behind kako's job contract as one more engine, not as a mandated replacement.

## Consequences

- Full control over the engine's resource, tenancy and lineage behavior.
- A large, long-running effort; progress is measured against the benchmark
  bar, not against dates.
- Arrow compatibility keeps the cost of switching engines low in both
  directions.

## Rejected alternatives

- **Fork DataFusion or another engine:** fastest start, but we would carry a
  large fork and still work against its design for sandbox and tenancy needs.
- **Only use existing engines:** remains the production default; this ADR adds
  an option, it does not remove one.
- **Custom memory format:** breaks interoperability for little gain.
