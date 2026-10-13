# ADR 0014: Query API may serve a caller-provided console at `/ui/`

- Status: Accepted
- Date: 2026-10-12
- Supersedes: —
- Source: [Issue #129](https://github.com/Sannrox/mizuchi/issues/129)
- Related: [ADR 0012](0012-query-history-api.md)

## Context

[mizuchi-console](https://github.com/Sannrox/mizuchi-console) is a relative-URL
zip meant to sit on the same origin as `/healthz` and `/v1`. ADR 0012 keeps UI
source out of this crate. Embedding runners still need a way to serve that zip
next to the query API without a second origin.

## Decision

- `QueryApiConfig.ui_dir` is an optional directory of unpacked console files.
  When set, `QueryApiServer` serves them at `/ui/` (`GET /` and `GET /ui`
  redirect relatively to `ui/`).
- Existing files are served. A missing path under `assets/` or with a known
  static type is 404. Any other missing path returns `index.html` so client
  routes work. Nested routes get a relative `<base href>` so `./assets/` still
  resolves.
- Path traversal, backslashes, and symlinks are refused. Responses send a
  strict Content-Security-Policy (`connect-src 'self'`). `/ui/` does not
  require the bearer token; `/v1` still does.
- Default is off: no directory means no UI routes.
- This crate still contains no console source. Compiling a GitHub release zip
  into the binary waits on a console pin and is not this decision.

## Consequences

- Operators unpack a console zip (or a local build) and point the query API at
  it. Compatibility remains the `/healthz` contract id.
- Static serving is a trust boundary: only regular files inside `ui_dir`.

## Rejected alternatives

- **Vendor UI source in this repository.** Rejected by ADR 0012.
- **Always compile in a pinned zip.** Needs a published console release and a
  Cargo feature; the embedder-owned directory works without that.
- **Serve from a second bind address.** The console uses relative API URLs and
  expects the same origin as `/v1`.
