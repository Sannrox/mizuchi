#!/usr/bin/env bash
# Build the release image under a staging tag, and later point the version tag
# at the attested digest. Never pushes latest.
#
# Usage:
#   bash scripts/release/image.sh push <tag> <dist-dir>
#     Build and push ghcr.io/sannrox/mizuchi:staging-<version>, save the image
#     as a release asset, and print digest=sha256:<hex> for $GITHUB_OUTPUT.
#     The version tag is not touched, so a re-run cannot move it.
#   bash scripts/release/image.sh promote <tag> <digest>
#     Point ghcr.io/sannrox/mizuchi:<version> at <digest>, after that digest
#     is attested and the draft release exists. Succeeds without change when
#     the version tag already names <digest>; fails when it names another.
set -euo pipefail

IMAGE=ghcr.io/sannrox/mizuchi
TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)\.[0-9]+)?$'
DIGEST_RE='^sha256:[0-9a-f]{64}$'

fail() {
  printf 'image: %s\n' "$1" >&2
  exit 1
}

# Prints the digest a registry reference names, or nothing when the tag does
# not exist. Any other lookup failure fails the script.
registry_digest() {
  local out
  if out="$(docker buildx imagetools inspect --format '{{.Manifest.Digest}}' "$1" 2>&1)"; then
    [[ "$out" =~ $DIGEST_RE ]] || fail "unexpected digest for $1: ${out}"
    printf '%s' "$out"
  elif [[ "$out" == *": not found"* ]]; then
    return 0
  else
    fail "cannot look up $1: ${out}"
  fi
}

push() {
  local version="$1" dist="$2"
  local staging="${IMAGE}:staging-${version}" digest
  docker build -f deploy/Dockerfile -t "$staging" . >&2
  docker push "$staging" >&2
  # RepoDigests holds name@sha256:... once the push has landed.
  digest="$(docker image inspect --format '{{index .RepoDigests 0}}' "$staging")"
  digest="${digest#*@}"
  [[ "$digest" =~ $DIGEST_RE ]] || fail "no registry digest for ${staging}"
  mkdir -p "$dist"
  docker save "$staging" | gzip >"${dist}/mizuchi-${version}-image.tar.gz"
  printf 'digest=%s\n' "$digest"
}

promote() {
  local version="$1" digest="$2"
  local final="${IMAGE}:${version}" current
  [[ "$digest" =~ $DIGEST_RE ]] || fail "invalid digest: ${digest}"
  current="$(registry_digest "$final")"
  if [[ "$current" == "$digest" ]]; then
    printf 'image: %s already names %s\n' "$final" "$digest" >&2
    return 0
  fi
  [[ -z "$current" ]] || fail "${final} already names ${current}, not ${digest}; cut a new version"
  # One source and no annotations: the tag names the source manifest as is.
  docker buildx imagetools create --tag "$final" "${IMAGE}@${digest}" >&2
  current="$(registry_digest "$final")"
  [[ "$current" == "$digest" ]] || fail "${final} names ${current:-nothing} after tagging, not ${digest}"
}

[[ $# -eq 3 ]] || fail "usage: $0 push <tag> <dist-dir> | promote <tag> <digest>"
cmd="$1" tag="$2"
[[ "$tag" =~ $TAG_RE ]] || fail "invalid release tag: ${tag}"
version="${tag#v}"
case "$cmd" in
  push) push "$version" "$3" ;;
  promote) promote "$version" "$3" ;;
  *) fail "unknown command: ${cmd}" ;;
esac
