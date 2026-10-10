#!/usr/bin/env bash

set -o errexit

# Ported from Sannrox/shikigami@331897846885e4a55697b0f1006d03b4f2a7495e scripts/validate-workflow-skills.sh (adapted for mizuchi).
# Compare against the source to detect drift.
set -o nounset
set -o pipefail

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${ROOT_PATH}"

SKILLS_ROOT=".agents/skills"

fail() {
  printf 'workflow skill check failed: %s\n' "$1" >&2
  exit 1
}

require_file() {
  [ -f "$1" ] || fail "missing $1"
}

require_line() {
  grep -Fqx "$2" "$1" || fail "$1 is missing canonical line: $2"
}

require_fragment() {
  grep -Fq "$2" "$1" || fail "$1 is missing required metadata: $2"
}

for skill in \
  advance-issue-frontier \
  assess-change-impact \
  deliver-ready-issue \
  verify-change
do
  skill_file="${SKILLS_ROOT}/${skill}/SKILL.md"
  require_file "${skill_file}"
  require_line "${skill_file}" "name: ${skill}"
done
require_fragment "${SKILLS_ROOT}/advance-issue-frontier/SKILL.md" "mizuchi"
require_fragment "${SKILLS_ROOT}/deliver-ready-issue/SKILL.md" "mizuchi"

# Ported Skills must name the right product (drift guard).
if grep -rIl --include=SKILL.md -i -e "shikigami GitHub" -e "sekai-chisei release" -e "sekai-chisei change" -e "sekai-chisei design" "${SKILLS_ROOT}"; then
  fail "a Skill still names another product as its own (see files above)"
fi
require_file "CLAUDE.md"
require_fragment "CLAUDE.md" "AGENTS.md"

for stage in \
  '## Establish authority and scope' \
  '## Build the dependency graph' \
  '## Compute the frontier' \
  '## Apply authorized status changes' \
  '## Report the frontier' \
  '## Boundaries'
do
  require_line "${SKILLS_ROOT}/advance-issue-frontier/SKILL.md" "${stage}"
done

for stage in \
  '## Establish the authority ceiling' \
  '## Deliver the Issue' \
  '### 1. Prove readiness' \
  '### 2. Isolate the work' \
  '### 3. Bound the implementation' \
  '### 4. Verify and review' \
  '### 5. Publish when authorized' \
  '### 6. Land when authorized' \
  '## Report completion' \
  '## Boundaries'
do
  require_line "${SKILLS_ROOT}/deliver-ready-issue/SKILL.md" "${stage}"
done

# Parallel lanes: the lead procedure lives with deliver-ready-issue, the policy
# lives in AGENTS.md, and both must stay linked.
PARALLEL_REFERENCE="${SKILLS_ROOT}/deliver-ready-issue/references/parallel-delivery.md"
LANE_SCRIPT="${SKILLS_ROOT}/deliver-ready-issue/scripts/issue-lane.sh"
require_file "${PARALLEL_REFERENCE}"
require_file "${LANE_SCRIPT}"
require_fragment "${SKILLS_ROOT}/deliver-ready-issue/SKILL.md" "references/parallel-delivery.md"
require_fragment "${SKILLS_ROOT}/deliver-ready-issue/SKILL.md" "scripts/issue-lane.sh"
require_fragment "${SKILLS_ROOT}/advance-issue-frontier/SKILL.md" "issue-lane.sh check"
require_line "AGENTS.md" "## Parallel delivery lanes"
require_fragment "AGENTS.md" "issue-lane.sh claim"
require_fragment "${PARALLEL_REFERENCE}" "AGENTS.md"
grep -Fq "/.worktrees/" .gitignore || fail ".gitignore does not ignore the /.worktrees/ lane directory"

printf 'workflow skill check passed\n'
