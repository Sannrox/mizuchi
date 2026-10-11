# ADR 0006: GPU acceleration is a no-go for this phase

- Status: Accepted
- Date: 2026-10-11
- Supersedes: —
- Source: [Issue #31](https://github.com/Sannrox/mizuchi/issues/31)

## Context

Issue #31 asked whether scans, filters, joins, and aggregates should run on
a GPU through Arrow-compatible device libraries. The engine is already
vectorized over Arrow morsels (ADR 0004). A GPU path would keep the same
logical plan and replace selected physical operators with device kernels,
copying batches to the device and back. The question is whether that pays
off at the sizes this project actually runs.

## Decision

**No-go.** Do not add GPU operators, device crates, or CI GPU jobs in this
phase.

The shape a later GPU path would take, if the bar below is met:

- Logical plan and SQL stay unchanged.
- Physical scan / filter / hash join / hash aggregate optionally lower to
  device kernels on Arrow buffers. Host Arrow remains the API boundary.
- Without a usable device, execution stays on the CPU path. Fail closed if
  a caller asked for a device that is missing.
- The engine still does not open network paths or devices the runner did
  not grant.

Revisit only when all of these hold:

1. Default correctness gates still run on CPU with no device.
2. A GPU is available in a non-default job, not in the offline `cargo test`
   bar.
3. Decision-support inputs are large enough that device work exceeds host
   copy (gigabytes, not kilobytes).
4. The embedding runner has an explicit device-grant in its sandbox policy.

## Evidence

### This repository (CPU, in-process Arrow)

`cargo test --test decision_support -- --include-ignored` on the scale-1
and scale-8 generators (debug profile):

| scale | input bytes | tpch_q1 ms | tpch_q3 ms | tpch_q6 ms | tpcds_store_sales ms |
| --- | --- | --- | --- | --- | --- |
| 1 | 7_232 | 5.8 | 2.6 | 0.7 | 1.3 |
| 8 | 39_104 | 5.0 | 3.0 | 0.8 | 1.4 |

Scale 1 is 48 `lineitem` rows. Scale 8 is 384. Wall time is already a few
milliseconds. A host-to-device copy of a 7–40 KiB table plus kernel launch
is the same order of magnitude as the whole query. GPU cannot win here.

### Published large-scale numbers (CPU vs GPU columnar, ~1 TB TPC-DS)

Public 1 TB decision-support runs of a GPU columnar plugin versus CPU
engines show a split, not a uniform win:

- Scan-heavy and simple aggregation queries: GPU often 2–11× faster
  (example: one grouping query 2.0 s GPU vs 23 s CPU-vectorized).
- Complex multi-stage queries: CPU vectorized often faster (example: one
  query 14 s CPU-vectorized vs 80 s CPU-row vs 118 s GPU).
- Parquet decode on large Snappy files: published GPU microbenchmarks
  about 2× host decode throughput, and more on chunked list columns.
- Cartesian products: published order-of-magnitude GPU wins.
- Grouped SUM: mixed; CPU columnar can be faster wall-clock.

Those wins assume gigabyte-to-terabyte inputs, a dedicated device, and a
shuffle path that keeps buffers on the device. None of that exists in
mizuchi CI or in the default suite.

### Product constraints

- Offline `cargo test` must stay green without large datasets or special
  hardware.
- The engine never opens devices it was not given. A GPU handle is a new
  sandbox surface for embedding runners.
- Arrow GPU crates target a device runtime that default CI images and many
  developer machines do not ship.
- mizuchi is a side project and must not block runners that already embed
  CPU engines (ADR 0002).

## Consequences

- Execution stays on the host Arrow path.
- No new dependency on device runtimes.
- The decision-support harness remains the place to collect CPU numbers;
  GPU numbers are out of the default test run.

## Rejected alternatives

- **Ship GPU operators now, skip them when no device is present.** Adds
  crates, CI matrix, and a sandbox surface for a path that loses on every
  current test input.
- **Require a GPU in CI.** Breaks the offline CPU bar and the side-project
  rule.
- **Rewrite operators as tuple-at-a-time device callbacks.** Contradicts
  vectorized morsels (ADR 0004).
