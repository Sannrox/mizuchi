---
name: shape-work-item
description: Shape a raw mizuchi idea, bug report, refactoring proposal, or research question into a focused GitHub Issue. Use when work needs scope, acceptance evidence, risk routing, or the correct Issue template before implementation.
---

<!-- Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e .agents/skills/shape-work-item/SKILL.md (adapted for mizuchi). Compare against the source to detect drift. -->

# Shape Work Item

Turn intent into a decision-ready unit of work. Produce a draft unless the user
explicitly authorizes publishing to GitHub.

## Procedure

1. Read `AGENTS.md`, `README.md`, accepted ADRs under `docs/decisions/`, and the
   matching form under `.github/ISSUE_TEMPLATE/`. Inspect affected code or docs
   when named. Complete when the request is framed against actual project
   boundaries (mizuchi is a query engine library; callers such as runners decide what runs
   and own sandboxing, tenancy and receipts).
2. Search open and closed Issues, Discussions, and PRs when GitHub access is
   available. Record possible duplicates or state that the search was not run.
3. Classify the work:
   - `bug`: reproducible expected-versus-actual behavior;
   - `feature`: a new observable operator or integration outcome;
   - `refactor`: preserved behavior with concrete structural evidence; or
   - `research`: a time-boxed question that ends in a decision.
   Route sensitive/exploitable behavior to private vulnerability reporting,
   never a public Issue. Route work owned by another repository (a runner's job
   contract, sandbox enforcement) to that repository and record it
   under `## GitHub dependencies`. Route cross-boundary,
   public-contract, trust-model, or difficult-to-reverse choices to a Design
   Discussion before implementation.
4. Draft with problem, observable outcome, non-goals, acceptance evidence,
   affected area (sql/plan/optimizer/exec/io/memory/bench/integration/distributed/python), and
   compatibility/security risks. Omit hostnames, FQDNs, IP addresses, home
   directories, absolute worktree paths, LAN or employer network names,
   deployment or environment state, image or artifact digests, tenant or
   customer names, release-hold or rollout-hold language, or other private
   environment inventory from any text that will be published to GitHub.
5. Recommend labels (`type:*`, `area:*`, `status:ready` when ready). Do not
   invent priority, assignment, or sprint.

## Output

1. route and rationale;
2. possible duplicates;
3. issue title and body ready for GitHub;
4. recommended labels;
5. unresolved questions blocking `status:ready`.

Do not publish without explicit authorization.
