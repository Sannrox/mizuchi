# ADR 0015: In-process morsel-driven pipeline scheduler

- Status: Accepted
- Date: 2026-10-19
- Supersedes: —
- Source: [Issue #138](https://github.com/Sannrox/mizuchi/issues/138)
- Related: [ADR 0004](0004-pull-vectorized-execution.md), [Issue #131](https://github.com/Sannrox/mizuchi/issues/131)

## Context

Parallel execution was a set of special cases in `try_parallel`, gated on
`Session::workers > 1`. The default one-worker path collected the pull
operator tree. Anything `try_parallel` did not match also fell back to
pull. Those two paths could diverge: a fast path that missed a case was
silently far slower than serial execution (#131).

ADR 0004 keeps operators as pull iterators of Arrow morsels. The remaining
choice is how a whole plan is driven across `Session::with_workers`
threads.

## Decision

- `schedule::run` is the single `execute_sql` path for any worker count,
  including the default of 1.
- The plan splits at blocking operators (join build, aggregate, sort).
  Scan/filter/project pipelines pull morsels from a shared queue
  (`map_morsels`). Worker count 1 runs that queue on the calling thread.
- Blocking operators execute children, then sink and finalize. Inner join
  plus aggregate stays a fused breaker so `tpcds_store_sales` does not
  rebuild a hash table per probe morsel (#131). Unsorted LIMIT over a
  scan/filter/project pipeline stops after the requested rows so later
  morsels are not evaluated. Sort over a pipeline pulls morsels so spill
  can start before the full projection is retained. Budgeted aggregation
  over a pipeline also pulls morsels into `agg::run`. Worker count 1 uses
  `agg::run` for every aggregate, including UNION and join children, so
  cancellation is checked between batches. Join children that
  are pipelines are pull sources; the build side is collected only when
  a runtime filter needs its keys and the query is unbounded.
  `LIMIT 0` does not evaluate the child.
- Operators the scheduler does not yet pipeline (union, window, recursive
  CTE, and similar) collect the pull tree for that subtree, then continue
  in the scheduler. A custom `Lowering` also collects the pull tree so
  overridden scan/filter/project operators run.
- `EXPLAIN ANALYZE` and `query_metrics` still instrument the pull operator
  tree so per-operator rows, batches, elapsed time, and `copied_bytes`
  stay. Ordinary execute does not wrap operators.

## Consequences

- Default sessions no longer skip the scheduler. Identity tests lock
  workers=1 against workers=2 on the suite, the SQL logic tests, and
  scan/join/aggregate/sort plans.
- Published SF1 timing stays on `scripts/bench-gb.sh` with workers > 1.
  `scripts/check-bench-regression.sh` rejects a 1-worker published run.
- Pull iterators remain the implementation of individual stages (ADR 0004).

## Rejected alternatives

- **Keep the `workers > 1` gate.** Dual paths were the bug in #131.
- **Drop fused join+aggregate.** Rebuilds the hash table per probe pair and
  regresses `tpcds_store_sales`.
- **Push/callback pipelines.** Rejected by ADR 0004.
- **Drive EXPLAIN ANALYZE through the scheduler in this change.**
  Per-operator wrappers live on the pull tree; attaching the same counters
  to `apply_pipe` / `join::run` / `agg::run` is a follow-up.
