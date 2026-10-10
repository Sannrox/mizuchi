#!/usr/bin/env bash
# Validate this repository's agent-kit stubs against the pinned Sannrox/agent-kit.
#
# Canonical source: Sannrox/agent-kit scripts/validate-workflow-skills.sh.
# Consuming repositories vendor this file byte-for-byte.
#
# Usage: bash scripts/validate-workflow-skills.sh [--render]
#
# Reads .agents/agent-kit.lock, installs the pinned kit into
# ${AGENT_KIT_HOME:-$HOME/.agents/agent-kit}/<ref> when missing (or uses
# AGENT_KIT_DIR), verifies the checkout is the pinned commit, and runs the
# kit's scripts/check-consumer.sh. --render rewrites stubs and vendored scripts.
set -euo pipefail

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
lock="${ROOT_PATH}/.agents/agent-kit.lock"
[ -f "$lock" ] || { echo "workflow skill check failed: missing .agents/agent-kit.lock" >&2; exit 1; }
field() { sed -n "s/^$1=//p" "$lock" | head -n 1; }
repo="$(field repo)"; ref="$(field ref)"; commit="$(field commit)"

kit="${AGENT_KIT_DIR:-${AGENT_KIT_HOME:-$HOME/.agents/agent-kit}/${ref}}"
if [ ! -f "${kit}/scripts/check-consumer.sh" ]; then
  mkdir -p "$(dirname "$kit")"
  git clone --quiet --depth 1 --branch "$ref" "https://github.com/${repo}" "$kit"
fi
actual="$(git -C "$kit" rev-parse HEAD 2>/dev/null || true)"
if [ -n "$commit" ] && [ "$actual" != "$commit" ]; then
  echo "workflow skill check failed: ${kit} is at ${actual:-unknown}, lock pins ${repo}@${ref} = ${commit}" >&2
  exit 1
fi

bash "${kit}/scripts/check-consumer.sh" "$kit" "$ROOT_PATH" "$@"
printf 'workflow skill check passed\n'
