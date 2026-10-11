# Project profile for agent-kit skills

<!-- Read by the shared skills in https://github.com/Sannrox/agent-kit (pinned in .agents/agent-kit.lock). Keep repo-specific facts here, not in the shared skills. Section headings are a contract: do not rename them. -->

## Project

- Name: mizuchi
- Feature means: a new observable operator or integration outcome

## Ownership boundaries

Mizuchi is a query engine library; callers such as runners decide what runs and own sandboxing, tenancy and receipts.

Route work owned by another repository (a runner's job contract, sandbox enforcement) to that repository.

## Affected areas

sql/plan/optimizer/exec/io/memory/bench/integration/distributed/python

## Shared surfaces

Issues touching the same shared surface run in sequence, never as parallel lanes:
the public engine API, the logical plan format, the memory and spill manager, and the Arrow/Parquet pins.

## Impact boundaries

- the public engine API (SQL entry point, logical plan, DataFrame) and the SQL dialect it accepts;
- Arrow compatibility: input and output batches stay standard Arrow, no private memory layout leaks across the API;
- resource behavior: memory accounting, spill to disk, thread usage and cancellation, which embedding runners rely on to enforce limits;
- correctness against reference engines (sqllogictest, benchmark suite);
- pinned upstream versions: arrow, parquet, sqlparser;

## Compatibility and failure obligations

Embedding runner and Arrow version pins; cancelled or failed queries (a memory limit hit, a worker or task failure mid-query) must release memory and leave no partial output; cleanup of spill files and threads on success, failure, and cancellation.

## Evidence map

Unit tests for pure logic; sqllogictest and plan snapshots for SQL and optimizer paths; operator tests for spill and memory limits; ignored benchmark or live tests only when a large dataset, Python runtime, or remote store is essential.

## Verification matrix

| Surface | Required evidence |
| --- | --- |
| SQL, planner, optimizer | sqllogictest and plan snapshot tests; results match a reference engine on the affected queries |
| file formats (Parquet, CSV, Arrow IPC) | round-trip tests against fixtures; malformed input rejected with an error, never a panic |
| execution, memory, spill | operator unit tests including spill paths and memory limit hits; no unbounded allocation |
| dependency pins (arrow, parquet, sqlparser) | `Cargo.lock` diff reviewed; the pinned tag exists upstream; upstream release notes read |
| Python bindings (later phase) | binding tests run against the built wheel; Arrow data passes zero-copy |

## Release audit

- workspace `Cargo.toml` version and lockfile consistency;
- pinned Arrow, Parquet and parser versions, and their release notes;
- public API, SQL dialect and plan format changes;
- configuration additions, removals, and defaults;
- Python binding packaging, once the Python API exists;
- packaging, rollback, and upgrade actions.
