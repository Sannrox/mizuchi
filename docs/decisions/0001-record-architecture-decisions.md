# ADR 0001: Record architecture decisions

- Status: Accepted
- Date: 2026-10-10
- Supersedes: —

## Context

mizuchi spans a SQL front end, a planner, an optimizer, an execution engine
and integration with embedding runners. Choices between these layers are
expensive to reverse and need to be findable after the PR that made them.

## Decision

Record architecture decisions as numbered Markdown files in `docs/decisions/`,
with status, date, context, decision, consequences, and rejected alternatives.

Write an ADR when a choice changes a public boundary (the engine API, the SQL
dialect, the logical plan format, memory and spill guarantees, Arrow
compatibility), chooses among durable alternatives, or would need a migration
to reverse. Small fixes and local refactors do not need one.

## Consequences

Each boundary change links an ADR. Superseded ADRs stay in place and name
their successor.
