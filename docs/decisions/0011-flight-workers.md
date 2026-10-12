# ADR 0011: Flight workers are separate processes

- Status: Accepted
- Date: 2026-10-12
- Supersedes: —
- Source: [Issue #108](https://github.com/Sannrox/mizuchi/issues/108)

## Context

#69 shipped in-process worker slots. Those slots honor CPU, memory, and
spill grants but never open sockets, so a runner cannot place workers on
other processes. Decision-support work needs a coordinator that can
detect a dead worker and retry.

## Decision

- Coordinator and workers are separate processes. They exchange Arrow
  batches over Arrow Flight (`DoAction` for register/execute/heartbeat,
  `DoGet` for shuffle).
- TCP binds and client connects happen only when the embedder sets
  `SessionConfig.allow_network` or `MIZUCHI_ALLOW_NETWORK` (ADR 0008).
- A worker heartbeat older than two seconds, or a failed Flight call,
  marks that worker dead. The coordinator retries the task on a live
  peer. Committed shuffle files in the shared spill directory are the
  fallback when Flight fetch fails.
- The `flight` crate feature is optional. Default `cargo test` stays
  offline. A `workflow_dispatch` job runs the three-worker suite,
  including a mid-query kill.

## Consequences

- Embedding runners that never grant network keep the previous
  no-socket behavior.
- Compose and Kubernetes manifests start three workers plus a
  coordinator. In-process `Cluster` remains for single-process tests.

## Rejected alternatives

- **Keep only in-process slots:** cannot exercise process failure or
  network shuffle.
- **Open sockets by default when compose is present:** would surprise
  runners that only intended to fence files (ADR 0008).
