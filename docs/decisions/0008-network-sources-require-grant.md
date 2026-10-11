# ADR 0008: Network sources require an embedder grant

- Status: Accepted
- Date: 2026-10-11
- Supersedes: —
- Source: [Issue #71](https://github.com/Sannrox/mizuchi/issues/71)

## Context

File readers take a path the caller already named. Path allow-lists are
empty by default so the runner (or the OS sandbox) owns the fence.
Database sources need TCP. An empty path list must not become an implicit
network grant: the engine's default is still no sockets.

## Decision

- JSON, Avro, and ORC are local files. They use the existing read/write
  path allow-lists.
- PostgreSQL scans are a separate source. `Session::register_postgres`
  opens TCP only when `SessionConfig.allow_network` is true. The default
  is false. Connection fields are set on a typed client config, not a
  concatenated DSN. `PostgresSource.ssl` selects TLS.
- Filter, projection, and integer-range partitions are pushed into the
  `SELECT` the engine sends. The engine does not write back to databases.
- The `postgres` crate feature is optional. Default `cargo test` stays
  offline. A `workflow_dispatch` job runs the live source test.

## Consequences

- Embedding runners that never set `allow_network` keep the previous
  no-socket behavior.
- Runners that grant network also supply host, port, database, and role.
- Compressed ORC and types outside the uncompressed DIRECT subset fail
  closed until a later issue.

## Rejected alternatives

- **Treat an empty path allow-list as unrestricted network:** would
  surprise runners that only intended to fence files.
- **Pull an ORC crate pinned to an older Arrow:** breaks the Arrow 60
  workspace pin.
