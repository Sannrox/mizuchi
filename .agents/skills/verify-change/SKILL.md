---
name: verify-change
description: Verify a mizuchi Rust, documentation, configuration, or workflow change with proportionate deterministic checks. Use after implementation, before review, or when a contributor needs an exact evidence report without overstating unrun tests.
---

<!-- Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e .agents/skills/verify-change/SKILL.md (rewritten for mizuchi; structure kept). Compare against the source to detect drift. -->

# Verify Change

Run the narrowest useful checks first, then expand according to change risk.

## Procedure

1. Inspect `git status`, the diff, and the stated outcome. Preserve unrelated
   worktree changes. Use `assess-change-impact` when risk is unclear. Complete
   when every changed path is classified.
2. Run focused tests for the affected module or crate first. Add checks
   based on the surface:

   | Surface | Required evidence |
   | --- | --- |
   | Rust source | focused tests plus the full local gates below |
   | SQL, planner, optimizer | sqllogictest and plan snapshot tests; results match a reference engine on the affected queries |
   | staging, hashing, cleanup | deterministic tests: identical inputs give identical hashes; scratch removed on success, failure, and timeout |
   | execution, memory, spill | operator unit tests including spill paths and memory limit hits; no unbounded allocation |
   | dependency pins (arrow, parquet, sqlparser) | `Cargo.lock` diff reviewed; the pinned tag exists upstream; upstream release notes read |
   | runtime image / Python packages | image builds; package set matches the documented policy |
   | configuration | parsing/default tests and the docs that describe the setting |
   | docs/templates/Skills | links and commands where practical; `bash scripts/validate-workflow-skills.sh` |

3. Before ship-level handoff, run the same gates as CI
   (`.github/workflows/ci.yml`) unless the user explicitly requested a
   narrower check:

   ```sh
   cargo fmt --all -- --check
   cargo build --locked --workspace --all-targets --exclude mizuchi-flight
   cargo clippy --locked --workspace --all-targets --exclude mizuchi-flight -- -D warnings
   cargo test --locked --workspace --exclude mizuchi-flight
   bash scripts/validate-workflow-skills.sh
   ```

   CI is the source of truth for the command set; if it changes, follow it and
   update `AGENTS.md`. Complete when every applicable local gate has a result.
4. Keep service- or kernel-dependent tests ignored or explicitly skipped unless
   prerequisites are intentionally available. Never print secrets or persist
   live payloads. Complete when skipped checks name both the reason and
   residual risk.
5. Review failures against the changed scope. Report pre-existing failures with
   evidence; do not relabel a failure as pre-existing without comparison.

## Output

Report:

- commands run and pass/fail result;
- focused behavior covered;
- checks skipped with reasons;
- failures and whether they block the stated outcome; and
- remaining uncertainty.

Never use “all tests pass” unless all stated tests actually ran and passed.
Remote CI may be unavailable; local evidence must then stand on its own and the
report says so.

## Relationship to autoreview

`verify-change` is **deterministic** local evidence only. It does **not** replace
structured second-model review. For non-trivial code that will be committed or
opened as a PR, run the **shared** `autoreview` helper after verify-change and
before ship (see `AGENTS.md` “Agent delivery closeout” and
`deliver-ready-issue`). Do not vendor that skill into this repository.
