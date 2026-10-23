# ADR 0016: Coordinator leadership is a fenced lease

- Status: Accepted
- Date: 2026-10-23
- Supersedes: none
- Source: [Issue #161](https://github.com/Sannrox/mizuchi/issues/161), part of #145

## Context

Flight workers are separate processes (ADR 0011), but there is one
coordinator, and losing it fails every running query. A standby can take
over only if it can tell when the leader is gone, and only safely if a
leader that was merely paused or partitioned cannot keep scheduling after
the takeover. Lease timing alone cannot promise that under clock skew or a
long pause.

## Decision

- Leadership is a lease in a pluggable `LeaseStore`. `MemoryLeaseStore`
  serves tests and single-process hosts. `DirLeaseStore` keeps the lease in
  a directory every coordinator mounts, as a sequence of version files.
  Every transition (takeover, renewal, release) claims the next version by
  publishing an exclusively created, synced staging file with a hard link,
  which fails if that version exists. A renewal and a takeover that race on
  one version are decided by the link, so a healthy leader is never
  superseded by a standby acting on a stale read. A takeover's token is the
  version it claims, so the highest version bounds every token ever granted
  and tokens rise even past damaged or pruned files. A claim below the
  highest version (a contender paused past pruning) is withdrawn. The
  directory is synced before a grant is returned, so an acknowledged token
  is never reissued after a crash.
- Every takeover raises the fencing token; a renewal keeps it. A leader
  renews before each assignment and in the background while it runs. A
  `Leadership` leads under one generation: once it loses that lease it stays
  fenced, and a token it is handed after a lapse is released at once, so
  work started under the old generation never continues under a new token.
- Every assignment carries the token in the `x-mizuchi-fencing-token`
  request metadata. A worker refuses a token lower than the highest it has
  seen, and re-checks it before returning a result or shuffle data and
  before a registration or shuffle write, holding the fence lock across the write, so work admitted
  under a superseded token does not land. Once a worker has seen any token,
  it refuses unfenced assignments too. A worker that mounts the lease
  directory (`--lease-dir`) admits only the directory's current token, so
  neither a superseded nor a not-yet-existing generation passes.
- Each generation has its own shuffle files in the shared spill directory
  and its own worker session, so a superseded generation can never
  overwrite the current one's shuffle data, and a table it registers late
  is invisible to its successor. Heartbeats are not assignments and carry
  no fence.
- A new leader registers its own inputs. A batch query run as stages
  (`RemoteCluster::run_staged`) commits each stage's output to the shared
  spill directory with a digest, per leadership generation. The first time a
  generation opens a query it claims the next seal in one numbered sequence,
  adopting what the previous sealing generation had committed by then, so a
  new leader reuses those stages and re-runs only the rest, while a commit a
  superseded leader publishes after the seal is never adopted. A generation
  older than the last seal is fenced. A stage with no rows keeps its columns.
  Each commit records its stage's definition, so a changed stage is never
  reused, and a coordinator runs one staged query at a time because their
  intermediate tables share the workers' namespace. The spill directory must
  already exist. A committed output that is missing or altered
  fails the query instead of resuming it (#162).
- A coordinator checks its lease again after every response, so a result
  that arrives after it lost the lease is not exposed. One that lost the
  lease stops its query with the typed `Error::Fenced` and does not retry
  on another worker. Queries are not resumed (non-goal of #161; #145 tracks recovery).
- `RemoteCluster::leader_status` reports leadership (only within the TTL of
  the last successful renewal), lease age and token.
- The lease holder is the coordinator's label plus its process id and
  start time, so two processes with one label, or a restarted one, never
  share a lease.
- The `coordinator` binary takes `--lease-dir` / `MIZUCHI_LEASE_DIR`,
  `--holder` / `MIZUCHI_HOLDER` (the label) and `MIZUCHI_LEASE_TTL_MS`
  (default 10 s).
  Without a lease directory it runs unfenced, as before.

## Consequences

- Single-coordinator deployments are unchanged: no lease directory, no
  header, and workers that never see a token accept everything.
- Old and new binaries interoperate during a rolling deploy: an old worker
  ignores the header, and a new worker accepts unfenced work until a fenced
  coordinator appears. Fencing holds once every worker runs the new binary.
- A worker without the lease directory keeps its highest token in memory
  only: after a restart it accepts any token until the next fenced
  assignment raises it, so a delayed request from a superseded leader could
  land in that window. The compose and Kubernetes layouts mount the lease
  directory on workers to close it; a lease directory a worker cannot read
  fails its assignments closed.
- The lease directory must be shared by every coordinator: the compose
  layout uses the shared volume, and the Kubernetes layout a
  `ReadWriteMany` claim.

## Rejected alternatives

- **Lease timing without fencing:** unsafe under clock skew and pauses.
- **A consensus store:** adds a runtime dependency to buy what one shared
  directory and a monotonic token already give.
- **The token in the action body:** changes the wire format for every
  action, which an old worker would misread during a rolling deploy.
