# Repository Guidelines

<!-- Structure and shared policy ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e AGENTS.md (adapted for mizuchi). Compare against the source to detect drift. -->

## Project Structure & Module Organization

`mizuchi` is a Rust 2024 workspace for an Arrow-native query and transform
engine: SQL and DataFrame front ends, a logical plan, an optimizer, and a
vectorized, morsel-driven execution engine. Single-node first, distributed
later. Library crates: `mizuchi-core`, `mizuchi-plan`, `mizuchi-io`,
`mizuchi-postgres`, `mizuchi-exec`, and the published facade `mizuchi`
(ADR [0013](docs/decisions/0013-workspace-crate-graph.md)). Arrow Flight
lives in `mizuchi-flight`, excluded from default workspace members and
pulled in with `--features flight`. The direction is recorded in
[ADR 0002](docs/decisions/0002-own-query-engine.md). This is a side
project and must never block the runners that embed existing engines.

Add new code in the crate of its layer (core, plan, io, exec, postgres,
flight) rather than growing the facade `lib.rs`. Do not commit
local state (`.state/`), benchmark datasets (`bench-data/`), spill files, or
generated artifacts.

Human documentation: [README.md](README.md). Architecture decisions:
[docs/decisions/](docs/decisions/README.md). `CLAUDE.md` only points here;
this file is canonical.

## Dependencies and Ownership

- **Arrow is the boundary.** Inputs and results are standard Apache Arrow
  (arrow-rs); Parquet via the `parquet` crate; SQL parsing via sqlparser-rs.
  Do not fork or vendor these; bump pins in focused Pull Requests.
- **Embedding runners own policy.** A runner such as
  [kako](https://github.com/Sannrox/kako) decides what runs, enforces the
  sandbox ([kekkai](https://github.com/Sannrox/kekkai)) and writes receipts.
  mizuchi exposes resource controls (memory limits, spill directory, threads,
  cancellation) and plan introspection; it opens network connections only
  when the embedder grants them and reads only paths it was given.

A change that needs a new upstream capability lands upstream first. Record
cross-repository predecessors in the Issue's `## GitHub dependencies` section
as full `<owner>/<repo>#<n>` references.

## Build, Test, and Development Commands

The compiler is pinned in `rust-toolchain.toml`. Local gates match CI
(`.github/workflows/ci.yml`):

```sh
cargo fmt --all -- --check
cargo build --locked --workspace --all-targets --exclude mizuchi-flight
cargo clippy --locked --workspace --all-targets --exclude mizuchi-flight -- -D warnings
cargo test --locked --workspace --exclude mizuchi-flight
bash scripts/validate-workflow-skills.sh
python3 scripts/adr-index.py --check
```

- `cargo fmt --all` formats before review.
- `cargo clippy ... -D warnings` is required for ship-level local gates.
- `scripts/validate-workflow-skills.sh` checks the shared Skill stubs and the
  contract headings of this file against agent-kit `main`.
- `python3 scripts/adr-index.py --check` fails when the decision index in
  `docs/decisions/README.md` is stale; without `--check` it rewrites it. Never
  edit the index by hand.

There is no Makefile yet. When one is added, its targets become the source of
truth for these commands and this section must point to them.

GitHub Issues are the planning source of truth. Read `README.md` and accepted
ADRs under `docs/decisions/` before changing system boundaries; record new
boundaries, trust-model changes, and public contracts as ADRs with the
`capture-project-decision` Skill.

## Shared agent skills

The workflow Skills (`advance-issue-frontier`, `assess-change-impact`,
`capture-project-decision`, `deliver-ready-issue`, `prepare-release`,
`refactor-docs`, `shape-work-item`, `technical-documentation`, `verify-change`)
and the opt-in `sekai-ontology` Skill are canonical in
[Sannrox/agent-kit](https://github.com/Sannrox/agent-kit). There is no pin:
this repository always uses the latest `main`. `.agents/skills/<name>/` holds
only a generated stub that points to the shared skill; read the installed
skill under `~/.agents/agent-kit/` (or `$AGENT_KIT_HOME`). Run
`bash scripts/validate-workflow-skills.sh` to clone or update it.

- Repository-specific facts the shared Skills need come from this file: the
  ownership, structure, build, parallel-lane and testing sections, plus
  `## Agent skill profile` below for anything they do not already say. Edit
  this file, not the stubs.
- Never edit the stubs or the vendored `scripts/validate-workflow-skills.sh`
  here; change them upstream in agent-kit, then run
  `bash scripts/validate-workflow-skills.sh --render` here when CI reports
  drift.
- Repository-only Skills live next to the stubs as full folders and stay local.

## Agent skill profile

Facts the shared agent-kit skills read that the sections above do not already
state.

Feature: a new observable operator or integration outcome.

### Compatibility and failure obligations

- embedding runner and Arrow version pins;
- partial runs: a crash after output is written but before it is reported, or
  an adapter losing its claim mid-run;
- retries owned by the source, not the engine;
- cleanup of scratch space, processes, memory reservations, and spill files.

### Additional impact boundaries

- the public engine API (SQL entry point, logical plan, DataFrame) and the SQL
  dialect it accepts;
- Arrow compatibility: input and output batches stay standard Arrow, and no
  private memory layout leaks across the API;
- resource behavior: memory accounting, spill to disk, thread usage, and
  cancellation, which embedding runners rely on to enforce limits;
- correctness against reference engines (sqllogictest, benchmark suite);
- pinned upstream versions: arrow, parquet, sqlparser.

### Verification matrix

Rows beyond `## Testing Guidelines`:

| Surface | Required evidence |
| --- | --- |
| SQL, planner, optimizer | sqllogictest and plan snapshot tests; results match a reference engine on the affected queries |
| staging, hashing, cleanup | deterministic tests: identical inputs give identical hashes; scratch removed on success, failure, and timeout |
| execution, memory, spill | operator unit tests including spill paths and memory-limit hits; no unbounded allocation |
| dependency pins (arrow, parquet, sqlparser) | `Cargo.lock` diff reviewed; the pinned tag exists upstream; upstream release notes read |
| runtime image / Python packages | image builds; package set matches the documented policy |
| docs, templates, Skills | `bash scripts/validate-workflow-skills.sh`; `python3 scripts/adr-index.py --check` for decision records |

### Release audit

- the version source is the workspace `Cargo.toml`; check it against the
  lockfile;
- pinned Arrow, Parquet, and parser versions, and their release notes;
- public API, SQL dialect, and plan format changes;
- runtime image contents and the Python package set;
- release notes: draft with `scripts/changelog-draft.sh <version>` from the
  Pull Requests merged since the last tag, edit them into user-facing notes,
  and commit them as the version section of `CHANGELOG.md`.

## Agent delivery closeout (required)

`cargo test` / Clippy / green CI are **not** a substitute for structured code
review. For non-trivial implementation work (any behavior, security, settings,
or public-API change) that will be **committed, pushed, or opened as a PR**:

1. Run the local gates above via the `verify-change` Skill.
2. Run **`autoreview`** before the ship commit (or before push if the commit
   already exists). **Do not vendor** the autoreview skill into this repo;
   resolve the helper from the shared skill install (first match wins):

   ```bash
   # Preferred: shared agent-skills checkout or global skills home
   if [ -x "${AUTOREVIEW:-}" ]; then :; \
   elif [ -x "$HOME/Projects/agent-skills/skills/autoreview/scripts/autoreview" ]; then
     export AUTOREVIEW="$HOME/Projects/agent-skills/skills/autoreview/scripts/autoreview"
   elif [ -x "${AGENTS_HOME:-$HOME/.agents}/skills/autoreview/scripts/autoreview" ]; then
     export AUTOREVIEW="${AGENTS_HOME:-$HOME/.agents}/skills/autoreview/scripts/autoreview"
   else
     echo "autoreview helper not found; install shared agent-skills or set AUTOREVIEW" >&2
     exit 1
   fi
   # Dirty uncommitted work:
   "$AUTOREVIEW" --mode local
   # Topic branch / open PR (preferred after commit):
   "$AUTOREVIEW" --mode branch --base origin/main
   # Already committed on a clean tree:
   "$AUTOREVIEW" --mode commit --commit HEAD
   ```

3. Treat helper output as advisory: verify each accepted finding in the real
   code, fix actionable ones, rerun tests and autoreview until the helper exits
   0 with no accepted/actionable findings (or document a maintainer judgment
   blocker in the PR).
4. Report in the PR or handoff: commands run, tests, autoreview result
   (clean / findings fixed / consciously rejected).

**Do not skip autoreview** because CI is green, the change is “small,” or the
session is optimizing for throughput unless the user explicitly waived it.
Docs-only typo fixes and pure formatting may skip structured review; state the
waiver. The `deliver-ready-issue` Skill requires this same closeout before
publish/land. Read the shared `autoreview` skill’s `SKILL.md` next to the
resolved helper for engines and findings policy.

## Parallel delivery lanes

A delivery lane is one Issue, one branch, one isolated checkout, one Pull
Request, and one owner. Several agents may deliver several Issues at the same
time, on one machine or on many, only as separate lanes. One Issue is never
split across lanes, and one lane never carries a second Issue.

### Claims live on GitHub

Machines cannot see each other's worktrees, so a claim is only what GitHub
shows. An Issue is claimed, and therefore active, when any of the following
exists:

1. an open Pull Request, draft or ready, that references the Issue;
2. a branch `<type>/<issue>`, or any branch matching `*/<issue>-*`;
3. the authenticated login assigned to the Issue less than six hours ago.

An assignment older than six hours with neither branch nor Pull Request is an
ownership hint, not a veto: report it and ask the maintainer before claiming.
A claim is stale once its Issue is closed or its Pull Request has merged; its
owner or the maintainer removes it. Any other takeover first preserves the
existing branch, Pull Request, and evidence and requires the maintainer's
confirmation, unless the lead of the same parallel run owns the stalled lane.

A lane claims before it implements, under Publish or Land authority or with an
explicit claim authorization under Implement, using
`bash "${AGENT_KIT_HOME:-$HOME/.agents/agent-kit}"/skills/deliver-ready-issue/scripts/issue-lane.sh claim <issue>`:

- the branch `<type>/<issue>` is created on GitHub from the default branch by
  an atomic ref creation, so when two machines race exactly one claim
  succeeds and the other sees `claimed`;
- the authenticated login is assigned to the Issue, which shows the claim in
  the Issue list and timestamps it in the Issue timeline.

`<type>` is derived deterministically so that every machine computes the same
branch: the Conventional Commit type in the Issue title, with `bug` mapped to
`fix`; otherwise the type label (`bug` → `fix`, `enhancement` → `feat`,
`documentation` → `docs`); otherwise `chore`. Lane branches carry no slug
because the name is the claim; the Pull Request title carries the description.
Human topic branches may keep `<type>/<issue>-<slug>` and are detected as
claims by the same rule.

An Implement-only lane cannot claim and is invisible to other machines. Say so
in the report, or ask for claim authority before starting.

### Isolation on each machine

Local isolation protects lanes that share a machine; it is not a claim.

- Each lane checks out its claim branch in `.worktrees/issue-<issue>` inside
  the repository, ignored by Git. A fresh clone dedicated to the lane, as on a
  cloud agent, is equivalent. The base SHA is recorded in the lane report.
- The primary checkout belongs to the maintainer. Agents never switch its
  branch, reset it, stash it, or run long jobs in it while another lane is
  active.
- A worktree serves exactly one Issue. Finished lanes are removed; a worktree is
  never reused for a different Issue or renamed to hide its origin.
- Shared Git state is mutated in short, serialized slots: `git fetch --prune`,
  worktree creation and removal, local branch deletion, and merging belong to
  the lead of a parallel run or happen one lane at a time on a shared machine.
  A lane commits and publishes its own branch from its own worktree; that is
  not a shared mutation. Nobody holds a slot across implementation, test runs,
  or a remote wait.

### Limits, collisions, and landing

The three-lane limit counts claims visible on GitHub per repository: open
implementation Pull Requests plus claim branches without one. Assigned or
planned work with neither is not a running lane.

Parallel lanes must not collide. Collision surfaces in this repository are the
public engine API, the logical plan format, the memory and spill manager,
and the Arrow/Parquet/sqlparser pins. Lanes that would
both change one of these surfaces run in sequence, not in parallel.

A lane publishes its first Verified commit to the claim branch as a draft Pull
Request that closes the Issue. Public GitHub lane fields may list only: claim
branch, repo-relative worktree (for example `.worktrees/issue-N` — never an
absolute path), base SHA, and published SHA. Keep absolute paths and
machine/host names in the private agent session only. It marks the Pull
Request ready when verification and review are complete. Immediately before
publishing, the lane fetches and confirms that the default branch is an
ancestor of its head; it refreshes onto `main` only for a conflict, a failing
gate, an explicit request, or a sibling landing on a shared surface, not
merely because `main` advanced.

Each lane lands its own reviewed head with `issue-lane.sh land`; lanes may
watch concurrently, and a lane that conflicts with a sibling's landing stops
instead of merging. After each merge, fetch, recompute the frontier, and let
the remaining lanes recheck `mergeable` against the new `main`.

The lead of a parallel run keeps a ledger per lane: Issue, branch, base SHA,
owner, state, Pull Request, evidence, blockers, and cleanup. Keep hostnames
and absolute checkout paths in the session with the maintainer, never on
GitHub. Report verified outcomes, not launched work. The executable lead
procedure is `skills/deliver-ready-issue/references/parallel-delivery.md` in
the installed agent-kit.

## Ontology Policy

For work involving portable ontology definitions, classes, relations,
provenance, validation, import, export, or structural queries, always use the
shared `sekai-ontology` Skill (stub in `.agents/skills/sekai-ontology/`).

Select the ontology database explicitly with `--db <path>` or `SEKAI_DB` when
the resolved default is not the intended file, then run `sekai --json validate`
before relying on its contents. Treat successful ontology output as structured
repository evidence, preserve its provenance in answers, and state when
validation fails or the requested fact is absent rather than inferring it.

## Coding Style & Naming Conventions

Follow standard Rust formatting with `cargo fmt` and keep modules aligned with
the layers above. Use `snake_case` for files, modules, functions, and
variables; use `PascalCase` for types and traits; use `SCREAMING_SNAKE_CASE` for
constants. Operators work on Arrow record batches; avoid row-at-a-time paths.
Every allocation that scales with input goes through the memory accountant.
Fail closed: when a memory limit cannot be honoured and spill is unavailable,
return an error rather than exceed the limit. Long-running work checks
cancellation.

## Testing Guidelines

Add focused tests for expressions, planning, optimizer rules (plan snapshot
tests), each operator including spill and memory-limit paths, and readers.
Use sqllogictest files for SQL behavior. Benchmarks and comparisons against
reference engines live outside the default test run, with their data
prerequisites documented. Offline `cargo test` must stay green without large
datasets or network access.

## Commit & Pull Request Guidelines

Use short imperative subjects, often Conventional Commit style:
`feat(plan): build a logical plan from SELECT`,
`fix(exec): release memory reservations on cancel`,
`docs: describe the spill directory contract`. Keep commits narrow and describe the affected
subsystem when useful. Pull requests should include a concise behavior
summary, **tests run**, **autoreview result** (for non-trivial code), linked
issue or context, and any configuration, compatibility, or security
implications. `.agents/skills/` is tracked; do not treat it as gitignored.
Pull requests do not edit `CHANGELOG.md`; it is release-owned. The Pull
Request title (Conventional Commit type) and body carry the change note, which
the squash merge keeps on `main`. Add a `Breaking: <note>` or `Upgrade: <note>`
line (or a `## Breaking` / `## Upgrade` section) to the body and the
`risk:breaking` or `risk:migration` label when callers or operators must act. Release prep drafts the version section with
`scripts/changelog-draft.sh <version>`.

### Public hygiene

This repository is public. Never put hostnames, FQDNs, IP addresses, home
directories, absolute worktree paths, LAN or employer network names, deployment
or environment state, image or artifact digests, tenant or customer names,
release-hold or rollout-hold language, or other private environment inventory on
Issues, Pull Requests, comments, commit messages, ADRs, or docs. Describe
behavior and versions instead (for example "after the sandbox release" rather
than an environment name, a digest, or a hold).

### Verified commits on GitHub

Lane branches are published with GitHub-verified commits and landed through
the shared lane tool, never with a plain `git push` or a hand-run merge:

1. Implement and commit locally as usual (`commit.gpgsign` may still apply).
   Never pass `--no-gpg-sign`; if GPG fails, stop and fix it.
2. Publish from the lane worktree:
   `bash "${AGENT_KIT_HOME:-$HOME/.agents/agent-kit}"/skills/deliver-ready-issue/scripts/issue-lane.sh push <issue>`.
   It creates the Verified commit through GraphQL `createCommitOnBranch` and
   resets the worktree to it, so local `HEAD` is the Pull Request head.
   Confirm it reports `verification.verified=true`.
3. Land with
   `bash "${AGENT_KIT_HOME:-$HOME/.agents/agent-kit}"/skills/deliver-ready-issue/scripts/issue-lane.sh land <issue> --pr <pr>`.
   It squash-merges with the reviewed head as the precondition, so the land
   commit on `main` is Verified and history stays linear.

Never queue GitHub auto-merge and avoid GitHub **rebase** merges: rebase-merge
rewrites commits and drops signatures. Do not rewrite protected `main` after
merging unless the user explicitly approves; if protection is temporarily
relaxed, restore force-push and status-check settings immediately after the
correction.

## Security & Configuration Tips

Never commit secrets, tokens, credentials, datasets, spill files, logs, or
local state. The engine opens network connections only when
`SessionConfig.allow_network` is set and reads only the inputs it is given. Report issues where crafted SQL or input reads
data it should not, escapes memory limits, or crashes the host privately
(GitHub private vulnerability reporting or the maintainer), never in a public
Issue. Never put hostnames, FQDNs, IP addresses, home directories, absolute
worktree paths, LAN or employer network names, deployment or environment
state, image or artifact digests, tenant or customer names, release-hold or
rollout-hold language, or other private environment inventory on public
Issues, Pull Requests, comments, or commit messages.
