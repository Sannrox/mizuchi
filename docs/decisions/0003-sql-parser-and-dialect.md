# ADR 0003: SQL parser and first dialect

- Status: Accepted
- Date: 2026-10-10
- Supersedes: —
- Source: [Issue #3](https://github.com/Sannrox/mizuchi/issues/3)

## Context

The SQL front end needs a parser before a binder can build a logical plan
(Issue #4). ADR 0002 already requires reusing standard crates rather than
reimplementing parsing. The first dialect must cover an ANSI subset plus the
common PostgreSQL and DuckDB extensions embedding runners actually send.
Whatever the parser accepts, the engine must not pretend it ran a clause it
dropped.

## Decision

- **Parser:** [sqlparser-rs](https://crates.io/crates/sqlparser) (`sqlparser`
  crate), pinned in the workspace. Do not fork or vendor it.
- **Dialect:** `PostgreSqlDialect`. It covers ANSI SELECT / FROM / WHERE /
  GROUP BY / HAVING / ORDER BY / LIMIT / OFFSET / JOIN / UNION / WITH, plus
  common PostgreSQL and DuckDB extensions the crate already parses (`::`
  casts, `ILIKE`, `DISTINCT ON`, dollar-quoted strings).
- **Error policy, fail closed:**
  - Lexer and parser failures return `Error::Parse`. The statement is rejected.
  - SQL that parses but the engine does not implement returns
    `Error::NotImplemented` at analysis or planning. Parse success is not
    execute success.
  - The parser never drops a clause and continues as if it were absent. A
    construct is either in the AST or the parse fails.

The parse entry point is a thin wrapper around `Parser::parse_sql`. Binding
the AST to a logical plan is Issue #4.

## Consequences

- Downstream SQL work depends on one pin and one dialect. Bump the pin in a
  focused PR.
- Callers get syntax errors before planning errors.
- Features the dialect parses but the engine does not run yet stay visible in
  the AST so later layers can reject them explicitly.
- sqlparser's AST is an implementation type of the SQL front end, not a
  long-term public contract.

## Rejected alternatives

- **Hand-written parser:** duplicates a large grammar and drifts from the
  dialect embedding runners already use.
- **A generated vendor grammar as the engine parser:** ADR 0002 already chose
  sqlparser-rs; replacing that stack is out of scope.
- **`AnsiDialect` or `GenericDialect` as the first dialect:** the Issue names
  PostgreSQL and DuckDB extensions; `PostgreSqlDialect` is the crate's match
  for that set.
- **Silently ignoring unrecognized clauses:** a query would return a result
  that does not match what the SQL asked for.
