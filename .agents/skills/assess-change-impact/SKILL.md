---
name: assess-change-impact
description: Assess a proposed or implemented mizuchi change across trust, API, dependency, and operations boundaries. Use when scoping an Issue, planning tests, reviewing a diff, or identifying compatibility, documentation, and security obligations.
---

<!-- Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e .agents/skills/assess-change-impact/SKILL.md (rewritten for mizuchi; structure kept). Compare against the source to detect drift. -->

# Assess Change Impact

Build an evidence-backed impact map before implementation or review.

## Procedure

1. Read the linked Issue or request, `README.md`, accepted ADRs under
   `docs/decisions/`, and the relevant code. For a diff, inspect every changed
   file and its direct callers or implementors. Complete when the claimed
   outcome and actual change surface are both known.
2. Trace applicable boundaries:
   - the public engine API (SQL entry point, logical plan, DataFrame) and
     the SQL dialect it accepts;
   - Arrow compatibility: input and output batches stay standard Arrow, no
     private memory layout leaks across the API;
   - resource behavior: memory accounting, spill to disk, thread usage and
     cancellation, which embedding runners rely on to enforce limits;
   - correctness against reference engines (sqllogictest, benchmark suite);
   - pinned upstream versions: arrow, parquet, sqlparser;
3. Identify compatibility and failure obligations: embedding runner and Arrow
   version pins, partial runs (crash after output written but before it is
   reported, an adapter losing its claim mid-run), retries owned by the source, cleanup of scratch
   and processes, and memory and spill files. Complete when data-loss,
   duplicate-write, and partial-failure paths are accounted for.
4. Map evidence to risk: unit tests for pure logic; sqllogictest and plan
   snapshots for SQL and optimizer paths; operator tests for spill and memory
   limits; ignored benchmark or live tests only when a large dataset, Python
   runtime, or remote store is essential. Complete when every material risk has a proposed check
   or an explicit residual uncertainty.
5. Determine durable artifacts that must change: README, docs, examples, an
   ADR, upstream Issues in the owning repository, or a repository Skill.
   Complete when no artifact is proposed merely to record temporary planning.

## Output

Return a compact matrix with columns:

| Surface | Evidence found | Required change/check | Risk if missed |
| --- | --- | --- | --- |

Then list scope boundaries, blocking questions, and the smallest safe PR split.
Do not approve an architecture, perform a full security audit, or claim
behavior parity without inspecting the implementations.
