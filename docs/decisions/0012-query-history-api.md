# ADR 0012: Query history and metrics HTTP API

- Status: Accepted
- Date: 2026-10-12
- Supersedes: —
- Source: [Issue #75](https://github.com/Sannrox/mizuchi/issues/75)

## Context

External UIs such as [mizuchi-console](https://github.com/Sannrox/mizuchi-console)
need query history, plans, per-operator metrics, running queries, and
cancel. mizuchi is a library, not a product UI (ADR 0002). Embedding
runners already own bind addresses, authentication, and path grants
(ADR 0007, ADR 0008).

## Decision

- The observability contract is HTTP JSON under `/v1`, identified as
  `mizuchi.query-api/v1` in `/healthz`. The JSON Schema lives in
  `crates/mizuchi/schemas/query-api-v1.json` and is served at `/v1/schema`.
- History, running-query, and cancel behavior exist on `QueryLog` in
  the library. `QueryApiServer` is a thin HTTP front over that log.
- The API is off until the embedder sets `SessionConfig.query_api`.
  `Session::serve_query_api` binds the configured address. Loopback
  binds do not require `allow_network`. Non-loopback binds do (ADR 0008).
- Authentication is an optional bearer token the embedder supplies.
  mizuchi does not ship identity providers or UI assets.
- Completed queries persist to a caller-provided JSONL file when
  `QueryApiConfig.history_path` is set, so history survives process
  restart. Retention is `max_entries` and optional `max_age_us`.
- Enabling the log instruments that session's executes so the stored
  record can include plan, read/write reports, and per-operator
  metrics. Ordinary sessions without a log keep the unwrapped pull
  path from #66.

## Consequences

- Console and other clients vendor the schema and detect an older
  server from the `/healthz` contract id.
- Runners that never set `query_api` keep the previous no-listen,
  no-history default.
- A history file is a write path: it must be inside
  `allowed_write_paths` when that list is non-empty.

## Rejected alternatives

- **Ship a web UI in mizuchi.** UI belongs in mizuchi-console (its
  ADR 0002). This crate stays embeddable and UI-free.
- **Always-on history.** Recording and operator wrappers have a cost;
  they stay opt-in.
- **Require `allow_network` for loopback.** Would force a general
  network grant just to serve localhost to a sidecar UI.
