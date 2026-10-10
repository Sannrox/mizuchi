---
name: assess-change-impact
description: Assess a proposed or implemented change across trust, API, dependency, and operations boundaries. Use when scoping an Issue, planning tests, reviewing a diff, or identifying compatibility, documentation, and security obligations. Uses the shared Sannrox agent-kit installation.
---

# Assess Change Impact

Use the canonical [assess-change-impact skill](https://github.com/Sannrox/agent-kit/tree/v0.1.0/skills/assess-change-impact).
Read the complete installed `~/.agents/agent-kit/v0.1.0/skills/assess-change-impact/SKILL.md` and the
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
