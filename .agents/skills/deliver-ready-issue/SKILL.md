---
name: deliver-ready-issue
description: Deliver a dependency-ready GitHub Issue through a bounded implementation workflow in an isolated worktree lane. Use when asked to implement, publish, or land a specific ready Issue, to take the next explicitly approved frontier item through verification and review, or to run several ready Issues as parallel lanes. Uses the shared Sannrox agent-kit installation.
---

# Deliver Ready Issue

Use the canonical [deliver-ready-issue skill](https://github.com/Sannrox/agent-kit/tree/v0.1.0/skills/deliver-ready-issue).
Read the complete installed `~/.agents/agent-kit/v0.1.0/skills/deliver-ready-issue/SKILL.md` and the
references it requires before starting. If installed at another operator-selected location
(`$AGENT_KIT_HOME/v0.1.0`), use that installation. This repository entrypoint contains no
workflow implementation.

If the skill is missing, run `bash scripts/validate-workflow-skills.sh` from this repository
(it installs the pinned kit), or clone it directly:
`git clone --depth 1 --branch v0.1.0 https://github.com/Sannrox/agent-kit ~/.agents/agent-kit/v0.1.0`.
The pin lives in `.agents/agent-kit.lock`.

Run the shared skill's scripts from the repository being changed, preserving its working
directory. Follow this repository's `AGENTS.md` and `.agents/project.md` in addition to the
canonical skill.

Keep shared fixes and instructions upstream in `Sannrox/agent-kit`. To adopt a new version,
change `ref` and `commit` in `.agents/agent-kit.lock`, then run
`bash scripts/validate-workflow-skills.sh --render`. Do not add repository-local helpers or copies.
