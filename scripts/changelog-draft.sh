#!/usr/bin/env bash
# Draft the CHANGELOG.md section for a release from the pull requests merged
# to main since the last v* tag (or the first commit when there is no tag).
# Pull requests do not edit CHANGELOG.md; the squash commit (pull request
# title and body) carries the note. Release prep runs this, edits the output
# and commits it as the new version section.
#
#   scripts/changelog-draft.sh VERSION [SINCE]
#
# Added = feat, Fixed = fix, Changed = refactor and perf. Other types appear
# only under Breaking. A change is breaking for a `type!:` subject, a
# `BREAKING CHANGE:` or `Breaking:` body line, or the risk:breaking label; it
# needs an upgrade note for an `Upgrade:` body line or the risk:migration
# label. Labels are read with gh.
set -euo pipefail

version="${1:?usage: scripts/changelog-draft.sh VERSION [SINCE]}"

ROOT_PATH="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "${ROOT_PATH}"

git fetch --quiet --tags origin main
since="${2:-$(git describe --tags --abbrev=0 --match 'v*' origin/main 2>/dev/null || git rev-list --max-parents=0 origin/main)}"

labelled() {
  gh pr list --state merged --base main --label "$1" --limit 1000 --json number --jq '.[].number'
}
breaking_prs=" $(labelled risk:breaking | tr '\n' ' ') "
migration_prs=" $(labelled risk:migration | tr '\n' ' ') "

breaking="" upgrade="" added="" changed="" fixed=""
for sha in $(git log --first-parent --reverse --format=%H "${since}..origin/main"); do
  subject="$(git log -1 --format=%s "${sha}")"
  body="$(git log -1 --format=%b "${sha}")"
  pr="$(sed -nE 's/.*\(#([0-9]+)\)$/\1/p' <<<"${subject}")"

  type="other" bang=""
  if [[ "${subject}" =~ ^([a-z]+)(\([^\)]*\))?(!)?:\ (.*)$ ]]; then
    type="${BASH_REMATCH[1]}" bang="${BASH_REMATCH[3]}" subject="${BASH_REMATCH[4]}"
  fi
  [[ -n "${pr}" && "${subject}" != *"(#${pr})" ]] && subject="${subject} (#${pr})"
  line="- ${subject}"$'\n'

  # A body note replaces the subject line in Breaking or Upgrade.
  note="$(sed -nE 's/^(BREAKING CHANGE|Breaking): *(.+)$/\2/p' <<<"${body}" | head -n 1)"
  if [[ -n "${note}" ]]; then
    breaking+="- ${note}${pr:+ (#${pr})}"$'\n'
  elif [[ -n "${bang}" || (-n "${pr}" && "${breaking_prs}" == *" ${pr} "*) ]]; then
    breaking+="${line}"
  fi
  note="$(sed -nE 's/^Upgrade: *(.+)$/\1/p' <<<"${body}" | head -n 1)"
  if [[ -n "${note}" ]]; then
    upgrade+="- ${note}${pr:+ (#${pr})}"$'\n'
  elif [[ -n "${pr}" && "${migration_prs}" == *" ${pr} "* ]]; then
    upgrade+="${line}"
  fi

  case "${type}" in
    feat) added+="${line}" ;;
    fix) fixed+="${line}" ;;
    refactor | perf) changed+="${line}" ;;
  esac
done

echo "## [${version}] - $(date -u +%F)"
section() {
  [[ -z "$2" ]] || printf '\n### %s\n\n%s' "$1" "$2"
}
section Breaking "${breaking}"
section Upgrade "${upgrade}"
section Added "${added}"
section Changed "${changed}"
section Fixed "${fixed}"
