# ADR 0004: Pull-based vectorized execution

- Status: Accepted
- Date: 2026-10-10
- Supersedes: —
- Source: [Issue #5](https://github.com/Sannrox/mizuchi/issues/5)

## Context

Physical operators must consume and produce Arrow record batches. ADR 0002
already requires vectorized, morsel-driven execution. The remaining choice is
how operators are driven: the parent pulls the next batch, or the child pushes
batches into a callback.

## Decision

Operators are **pull iterators** of Arrow record batches (morsels):

- Each operator exposes `next_batch() -> Result<Option<Batch>>`. A parent
  pulls until it receives `None`.
- A batch is the morsel. The default size is [`BatchPolicy`] (4096 rows).
- Work inside a batch is column-at-a-time Arrow kernels, never a row loop.
- The first operators are in-memory scan, filter, and projection. Later
  operators (join, aggregate, sort) use the same pull contract.
- Filter treats a null predicate as false (the row is dropped).

## Consequences

- Execution state lives in the operator tree. Cancellation is a flag checked
  between batches (Issue #11).
- Pipelines compose by wrapping an inner iterator. There is no callback
  inversion.
- Morsel size is a policy, not an operator-specific constant.

## Rejected alternatives

- **Push / callback pipelines:** every operator would take a consumer; error
  and cancellation paths are harder to keep linear.
- **Row-at-a-time iterators:** contradicts ADR 0002 and the expression
  kernels already in the crate.
