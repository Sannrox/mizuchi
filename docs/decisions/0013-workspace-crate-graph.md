# ADR 0013: Layered workspace crate graph

- Status: Accepted
- Date: 2026-10-12
- Supersedes: —
- Source: crate-graph survey of the former single `mizuchi` package

## Context

The engine lived in one published crate. A hot edit in hash join
recompiled Parquet, Avro (`serde_derive`), lake tables, and relinked
every integration test. `write` depended on `exec::CancelToken` only
because they shared a crate. Optional Flight (`tonic`) and PostgreSQL
(`native-tls`) sat behind features but still occupied the same rustc
session when enabled.

## Decision

The workspace is layered crates. The published embedder crate `mizuchi`
re-exports today's crate-root API (ADR 0007).

| Crate | Owns |
| --- | --- |
| `mizuchi-core` | `Error`, `Batch`, `Expr`, `CancelToken`, scan/write report records |
| `mizuchi-plan` | SQL parse, logical plan, catalog, optimizer |
| `mizuchi-io` | Parquet/CSV/JSON/Avro/ORC, object store, Delta/Iceberg |
| `mizuchi-postgres` | PostgreSQL source (`client` feature pulls TLS) |
| `mizuchi-exec` | Operators, spill, `Session`, query-history API |
| `mizuchi-flight` | Arrow Flight workers (`tonic`/`tokio`); workspace member, omitted from `default-members` |
| `mizuchi` | Facade re-exports, examples, integration tests, Flight binary |

`CancelToken` lives in core so writers do not depend on execution.
`parse_sql` no longer re-exports `sqlparser::ast::Statement`. Flight
stays a path dependency of `mizuchi` with `--features flight`. It is
omitted from `default-members`; CI and local gates pass
`--exclude mizuchi-flight` so `tonic` is not in the default graph.

Internal crates are `publish = true` at the workspace version so
`cargo publish -p mizuchi` can resolve them from crates.io after they
are published in dependency order.

## Consequences

- A join-operator edit rebuilds `mizuchi-exec` and the facade, not
  Parquet/Avro sources.
- Embedders keep `mizuchi::{Session, Batch, plan_sql, ...}`.
- Releases publish `mizuchi-core`, then plan, io, postgres, exec, then
  `mizuchi`. Flight publishes with the `flight` feature set.
- New code lands in the crate of its layer (AGENTS.md).

## Rejected alternatives

- **Keep one crate.** Incremental `cargo test --no-run` after a join
  edit relinked 19 binaries and always typechecked format readers.
- **Session in the facade, operators in exec.** `Engine`/`Cluster`/
  `StreamQuery` close over `Session`; that split is a cycle unless
  those types move up too. Session stays in `mizuchi-exec`.
- **Publish only `mizuchi` with unpublished path crates.** crates.io
  rejects a package that depends on `publish = false` path crates.
