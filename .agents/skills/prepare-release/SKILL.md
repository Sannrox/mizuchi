---
name: prepare-release
description: Prepare a mizuchi release by auditing version scope, compatibility, validation, artifacts, and release notes. Use when a maintainer asks for release readiness, a version bump plan, or a draft GitHub Release.
---

<!-- Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e .agents/skills/prepare-release/SKILL.md (rewritten for mizuchi; structure kept). Compare against the source to detect drift. -->

# Prepare Release

Assemble decision-ready release evidence. Do not tag, push, publish, or alter
GitHub state unless the maintainer explicitly authorizes that action.

## Procedure

1. Identify the target version, base tag, target commit, and milestone or merged
   PR range. Read `Cargo.toml`, CI, and open release-blocking Issues. Complete
   when the exact release contents are bounded.
2. Classify changes as `Added`, `Changed`, `Fixed`, `Security`, or `Migration`.
   Check SemVer fit; before `1.0`, call out all public breaking changes even when
   they fit a minor bump. Complete when every user-visible merged change is
   represented once.
3. Audit release impact:
   - workspace `Cargo.toml` version and lockfile consistency;
   - pinned Arrow, Parquet and parser versions, and their release
     notes;
   - public API, SQL dialect and plan format changes;
   - configuration additions, removals, and defaults;
   - runtime image contents and the Python package set;
   - packaging, rollback, and upgrade actions.
   Complete when every applicable item is resolved or a named blocker.
4. Use `verify-change` for the full local gates. Confirm current GitHub CI and
   security checks when access is available; when CI cannot run, say so and
   rely on recorded local evidence for the target commit. Complete when
   evidence is current for the target commit.
5. Draft concise user-facing release notes and update `CHANGELOG.md`. Put upgrade and consumer actions before internal
   implementation detail. Release notes describe versions and behavior, never
   hostnames, FQDNs, IP addresses, home directories, absolute worktree paths,
   LAN or employer network names, deployment or environment state, image or
   artifact digests, tenant or customer names, release-hold or rollout-hold
   language, or other private environment inventory.
6. Report go/no-go. A release is `go` only when required checks pass, no known
   blocker remains, and rollback/upgrade implications are explicit.

## Output

Return the target, commit range, readiness checklist, validation results,
compatibility notes, release-note draft, and blockers. Separate verified facts
from recommendations.
