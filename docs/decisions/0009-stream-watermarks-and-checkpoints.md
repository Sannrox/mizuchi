# ADR 0009: Continuous queries with watermarks, windows, and checkpoints

- Status: Accepted
- Date: 2026-10-11
- Supersedes: —
- Extends: [ADR 0005](0005-incremental-microbatch.md)
- Source: [Issue #72](https://github.com/Sannrox/mizuchi/issues/72)

## Context

ADR 0005 runs grouped aggregates as micro-batches with complete output
and in-process state. Callers also need unbounded sources, event-time
windows, late-data bounds, and a sink that does not duplicate rows after
a crash between writing output and closing the log.

## Decision

- A [`StreamQuery`] is the existing micro-batch loop over an unbounded
  in-memory source. Each trigger takes the batches ingested since the
  last commit and runs them as one finite aggregate.
- Event time is a `Timestamp` or `Int64` (microseconds) column. The
  watermark after a trigger is
  `max(previous, max_event_in_trigger - delay)`. Rows with event time
  behind the *previous* watermark are dropped.
- Tumbling, sliding, and session windows assign a `[start, end)` range.
  Grouping is window plus caller-supplied columns. `COUNT(*)` always
  runs; an optional `Int64` column is summed.
- **Complete** output writes every group each trigger. **Append** writes
  groups whose window end is at or behind the new watermark and drops
  them from state.
- The checkpoint directory is a write-ahead log: `offsets/{id}` then
  sink file `batch-{id}.parquet` and Arrow IPC state, then
  `commits/{id}`. Offset and commit files are created with `create_new`.
  The sink file for an id is replaced on replay, so a trigger that
  wrote output but not a commit is idempotent.
- On open, the query restores the last *committed* state. An offset
  without a commit is retried with the same id. The in-memory source
  starts empty; the caller re-ingests uncommitted rows.
- Checkpoint and sink paths use the session write (and read) allow-lists.

## Consequences

- Batch and streaming aggregates share the same COUNT/SUM grouping
  semantics; window assignment is extra grouping keys.
- Complete mode resends the full group table every trigger. Append
  mode is the path that evicts state behind the watermark.
- Kill-and-restart correctness is the commit log plus unique sink
  names, not a two-phase commit with the source.
- Stream-stream joins, changelog retracts, and SQL `STREAM` syntax
  stay out of this ADR.

## Rejected alternatives

- **Record-at-a-time continuous operators.** Contradicts vectorized
  morsels (ADR 0004) and ADR 0005.
- **Checkpoint only the sink files.** A crash after writing Parquet
  and before recording the offset would duplicate or drop on replay.
- **Advance the source offset before writing the sink.** A crash would
  skip input.

[`StreamQuery`]: ../../crates/mizuchi-exec/src/stream.rs
