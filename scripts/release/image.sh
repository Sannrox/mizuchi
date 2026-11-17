#!/usr/bin/env bash
# Build and push the versioned release image, save it as a release asset, and
# print its registry digest. Only the version tag is ever pushed, never latest.
#
# Usage: bash scripts/release/image.sh <tag> <dist-dir>
#   Prints digest=sha256:<hex>, ready for $GITHUB_OUTPUT.
set -euo pipefail

IMAGE=ghcr.io/sannrox/mizuchi
TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)\.[0-9]+)?$'

fail() {
  printf 'image: %s\n' "$1" >&2
  exit 1
}

[[ $# -eq 2 ]] || fail "usage: $0 <tag> <dist-dir>"
tag="$1" dist="$2"
[[ "$tag" =~ $TAG_RE ]] || fail "invalid release tag: ${tag}"
version="${tag#v}"
image="${IMAGE}:${version}"

docker build -f deploy/Dockerfile -t "$image" . >&2
docker push "$image" >&2
# RepoDigests holds name@sha256:... once the push has landed.
digest="$(docker image inspect --format '{{index .RepoDigests 0}}' "$image")"
digest="${digest#*@}"
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "no registry digest for ${image}"
mkdir -p "$dist"
docker save "$image" | gzip >"${dist}/mizuchi-${version}-image.tar.gz"
printf 'digest=%s\n' "$digest"
