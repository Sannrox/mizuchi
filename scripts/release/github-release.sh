#!/usr/bin/env bash
# The only script that changes a GitHub Release. Every command first refuses a
# published release, so "Re-run failed jobs" can resume a draft but never
# rewrite what was published. A tag with a "-" (vX.Y.Z-rc.N) is a pre-release
# and is never marked latest. Needs GH_TOKEN and GH_REPO; draft and publish
# run in a checkout whose origin holds the tag.
#
# Usage:
#   bash scripts/release/github-release.sh guard <tag>
#       Exit 0 when the release is a draft or does not exist yet.
#   bash scripts/release/github-release.sh draft <tag> <sha> <notes> <asset>...
#       Create the draft, or resume it: drop the earlier run's assets, upload
#       these, and reset title, notes, and pre-release state.
#   bash scripts/release/github-release.sh publish <tag> <sha>
#       Publish the draft.
# draft and publish first check that the remote tag still names <sha>.
set -euo pipefail

TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)\.[0-9]+)?$'

fail() {
  printf 'github-release: %s\n' "$1" >&2
  exit 1
}

# Prints "draft" or "missing"; fails on a published release or a lookup error.
# A token that cannot see drafts reports a draft as not found.
release_state() {
  local tag="$1" state
  if state="$(gh release view "$tag" --json isDraft --jq .isDraft 2>&1)"; then
    [[ "$state" == true ]] || fail "release ${tag} is already published; cut a new tag instead of re-running"
    echo draft
  elif [[ "$state" == *"release not found"* ]]; then
    echo missing
  else
    fail "cannot read release ${tag}: ${state}"
  fi
}

# The release attaches to the remote tag; it must still name the built commit.
require_remote_tag() {
  local tag="$1" sha="$2" remote
  remote="$(git ls-remote origin "refs/tags/${tag}^{}")" || fail "cannot read remote tag ${tag}"
  [[ "${remote%%$'\t'*}" == "$sha" ]] || fail "remote tag ${tag} no longer points at ${sha}"
}

cmd="${1:-}"
[[ $# -ge 1 ]] && shift
[[ $# -ge 1 && "$1" =~ $TAG_RE ]] || fail "usage: $0 {guard|draft|publish} <vX.Y.Z[-rc.N]> ..."
tag="$1"
kind=()
[[ "$tag" == *-* ]] && kind=(--prerelease --latest=false)
case "$cmd" in
  guard)
    [[ $# -eq 1 ]] || fail "usage: $0 guard <tag>"
    release_state "$tag" >/dev/null
    ;;
  draft)
    [[ $# -ge 4 ]] || fail "usage: $0 draft <tag> <sha> <notes> <asset>..."
    sha="$2" notes="$3"
    shift 3
    for asset in "$notes" "$@"; do
      [[ -f "$asset" ]] || fail "missing release file: ${asset}"
    done
    require_remote_tag "$tag" "$sha"
    # Assign first: a failure inside [[ "$(...)" ]] would not stop the script.
    state="$(release_state "$tag")"
    if [[ "$state" == draft ]]; then
      # Every run rebuilds every asset; drop the earlier run's so a renamed or
      # removed asset is not published.
      names="$(gh release view "$tag" --json assets --jq '.assets[].name')"
      while IFS= read -r name; do
        [[ -n "$name" ]] && gh release delete-asset "$tag" "$name" --yes
      done <<<"$names"
      gh release upload "$tag" "$@" --clobber
      gh release edit "$tag" --title "$tag" --notes-file "$notes" --draft ${kind[@]+"${kind[@]}"}
    else
      gh release create "$tag" "$@" --draft --verify-tag ${kind[@]+"${kind[@]}"} \
        --title "$tag" --notes-file "$notes"
    fi
    ;;
  publish)
    [[ $# -eq 2 ]] || fail "usage: $0 publish <tag> <sha>"
    require_remote_tag "$tag" "$2"
    state="$(release_state "$tag")"
    [[ "$state" == draft ]] || fail "release ${tag} has no draft to publish"
    gh release edit "$tag" --draft=false ${kind[@]+"${kind[@]}"}
    echo "Published GitHub Release ${tag}"
    ;;
  *)
    fail "usage: $0 {guard|draft|publish} <tag> ..."
    ;;
esac
