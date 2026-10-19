# Architecture decision records

Decisions that must outlive a single PR.

| ADR | Title | Status |
| --- | --- | --- |
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-own-query-engine.md) | Own query engine as a side project | Accepted |
| [0003](0003-sql-parser-and-dialect.md) | SQL parser and first dialect | Accepted |
| [0004](0004-pull-vectorized-execution.md) | Pull-based vectorized execution | Accepted |
| [0005](0005-incremental-microbatch.md) | Incremental execution as micro-batches | Accepted |
| [0006](0006-gpu-acceleration.md) | GPU acceleration is a no-go for this phase | Accepted |
| [0007](0007-api-stability-and-releases.md) | API stability, versioning, and releases | Accepted |
| [0008](0008-network-sources-require-grant.md) | Network sources require an embedder grant | Accepted |
| [0009](0009-stream-watermarks-and-checkpoints.md) | Continuous queries: watermarks, windows, checkpoints | Accepted |
| [0010](0010-pluggable-catalog.md) | Pluggable catalog: namespaces, backends, partition metadata | Accepted |
| [0011](0011-flight-workers.md) | Flight workers are separate processes | Accepted |
| [0012](0012-query-history-api.md) | Query history and metrics HTTP API | Accepted |
| [0013](0013-workspace-crate-graph.md) | Layered workspace crate graph | Accepted |
| [0014](0014-serve-console-at-ui.md) | Query API may serve a caller-provided console at `/ui/` | Accepted |
| [0015](0015-morsel-pipeline-scheduler.md) | In-process morsel-driven pipeline scheduler | Accepted |

Template: short context, decision, consequences, rejected alternatives.
