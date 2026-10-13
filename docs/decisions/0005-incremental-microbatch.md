# ADR 0005: Incremental execution as micro-batches

- Status: Accepted
- Date: 2026-10-11
- Supersedes: —
- Source: [Issue #29](https://github.com/Sannrox/mizuchi/issues/29)

## Context

ADR 0004 is a pull iterator of Arrow morsels over a finite plan. Callers
also want to keep a query running as new batches arrive (append-only
ingest, dashboards, incremental views) without rescanning everything
already processed. The engine already has a hash aggregate whose state
is a per-group accumulator table. The choice is how that state lives
across arrivals.

## Decision

Incremental work is **micro-batch execution on the same operators**:

- A streaming query is the existing logical plan over an unbounded
  append-only source. Each trigger takes the batches that arrived since
  the last trigger and runs them as one finite morsel pipeline.
- Stateful grouped aggregates (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`) keep
  their hash table between micro-batches. New rows update existing
  groups or insert new ones. The prototype is [`MicroBatchAgg`].
- The first output mode is **complete**: after each micro-batch the
  operator emits every group accumulated so far. Event-time watermarks,
  tumbling/sliding/session windows, append output, and checkpointed
  sinks are [ADR 0009](0009-stream-watermarks-and-checkpoints.md).
- Stateless operators (scan of the delta, filter, project) are unchanged
  pull iterators over that micro-batch only.
- Cancellation (ADR 0004 / Issue #11) is still checked between morsels
  inside a trigger. A cancelled trigger does not commit partial state.

Joins, retracts, and changelog inputs are out of this ADR. They need a
keyed state store and a change-row contract that the current hash join
does not have.

## Consequences

- One code path for batch and streaming aggregates: the streaming
  operator is the batch hash aggregate with a snapshot instead of a
  consuming finish.
- Memory accounting and spill apply to the running state the same way
  they apply to a long batch aggregate. Fail closed if the limit cannot
  be honoured.
- Complete mode resends the full group table every trigger. That is
  correct and simple; it is not cheap for high-cardinality keys.
- Checkpointing the state table to the caller-provided spill directory
  can reuse Arrow IPC later. The prototype keeps state in process.

## Rejected alternatives

- **Full recompute of the accumulated table on every arrival.** Correct,
  but cost grows with history and wastes the hash aggregate we already
  have.
- **Tuple-at-a-time incremental view maintenance** (per-row deltas
  through every operator). Contradicts vectorized morsels (ADR 0004).
- **Continuous push of individual rows** with operator callbacks.
  Rejected for the same reason as push pipelines in ADR 0004.

[`MicroBatchAgg`]: ../../crates/mizuchi-exec/src/incremental.rs
