#!/usr/bin/env bash
# Refuse a v* tag that is lightweight, unsigned, signed by an unknown key,
# or whose version does not equal the workspace version.
#
# Usage:
#   bash scripts/release/verify-tag.sh verify <tag> [commit-sha]
#   bash scripts/release/verify-tag.sh release <tag>
#       verify, plus: fetch the tag and main from origin, require the tagged
#       commit on main and checked out (the release workflow's check).
set -euo pipefail
trap '' PIPE

ROOT_PATH="${RELEASE_ROOT:-$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)}"
FINGERPRINTS_FILE="${RELEASE_TAG_FINGERPRINTS:-$ROOT_PATH/security/release-tag-fingerprints}"
KEY_FILE="${RELEASE_TAG_KEY:-$ROOT_PATH/security/release-tag-key.asc}"
TAG_RE='^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)\.[0-9]+)?$'

fail() {
  printf 'verify-tag: %s\n' "$1" >&2
  exit 1
}

normalize_fpr() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]' | tr -d '[:space:]'
}

load_allowed_fingerprints() {
  local line
  ALLOWED_FPRS=()
  [[ -f "$FINGERPRINTS_FILE" ]] || fail "missing fingerprints file: ${FINGERPRINTS_FILE}"
  while IFS= read -r line || [[ -n "$line" ]]; do
    [[ "$line" =~ ^[[:space:]]*# ]] && continue
    [[ -z "${line// }" ]] && continue
    ALLOWED_FPRS+=("$(normalize_fpr "$line")")
  done <"$FINGERPRINTS_FILE"
  [[ ${#ALLOWED_FPRS[@]} -gt 0 ]] || fail "no fingerprints in ${FINGERPRINTS_FILE}"
}

fingerprint_allowed() {
  local got="$1" allowed
  for allowed in "${ALLOWED_FPRS[@]}"; do
    [[ "$got" == "$allowed" ]] && return 0
  done
  return 1
}

workspace_version() {
  python3 -c 'import sys,tomllib; print(tomllib.load(open(sys.argv[1], "rb"))["workspace"]["package"]["version"])' "$ROOT_PATH/Cargo.toml"
}

import_allowed_key() {
  [[ -f "$KEY_FILE" ]] || fail "missing release-tag public key: ${KEY_FILE}"
  gpg --batch --import "$KEY_FILE" >/dev/null 2>&1 || fail "could not import ${KEY_FILE}"
}

verify_release_tag() {
  local tag="$1" expected_commit="${2:-}" objtype peeled raw status_fpr primary_fpr
  [[ "$tag" =~ $TAG_RE ]] || fail "invalid release tag: ${tag}"
  load_allowed_fingerprints
  import_allowed_key

  objtype="$(git cat-file -t "refs/tags/${tag}" 2>/dev/null)" || fail "tag ${tag} was not found"
  [[ "$objtype" == "tag" ]] || fail "tag ${tag} is lightweight; release tags must be annotated"
  # A signed tag object pushed under another ref name keeps its own name.
  local signed_name
  signed_name="$(git cat-file tag "refs/tags/${tag}" | sed -n '/^$/q;s/^tag //p')"
  [[ "$signed_name" == "$tag" ]] || fail "tag ${tag} points at a tag object named ${signed_name}"

  if [[ -n "$expected_commit" ]]; then
    [[ "$expected_commit" =~ ^[0-9a-f]{40}$ ]] || fail "expected commit must be a full lowercase SHA"
    peeled="$(git rev-parse "${tag}^{commit}")"
    [[ "$peeled" == "$expected_commit" ]] || fail "tag ${tag} resolves to ${peeled}, expected ${expected_commit}"
  fi

  raw="$(git verify-tag --raw "$tag" 2>&1)" || fail "tag ${tag} is unsigned or the signature is not valid"
  # GnuPG still reports VALIDSIG for a revoked or expired key; refuse those.
  if grep -qE '^\[GNUPG:\] (BADSIG|ERRSIG|EXPSIG|EXPKEYSIG|REVKEYSIG) ' <<<"$raw" \
    || ! grep -qE '^\[GNUPG:\] GOODSIG ' <<<"$raw" \
    || ! grep -qE '^\[GNUPG:\] VALIDSIG ' <<<"$raw"; then
    fail "tag ${tag} is unsigned or the signature is not valid"
  fi
  status_fpr="$(awk '/^\[GNUPG:\] VALIDSIG / { print $3; exit }' <<<"$raw")"
  primary_fpr="$(awk '/^\[GNUPG:\] VALIDSIG / { print $12; exit }' <<<"$raw")"
  status_fpr="$(normalize_fpr "$status_fpr")"
  primary_fpr="$(normalize_fpr "$primary_fpr")"
  fingerprint_allowed "$status_fpr" || fingerprint_allowed "$primary_fpr" || fail "tag ${tag} is signed by an unknown key"

  local version="${tag#v}" expected
  expected="$(workspace_version)"
  [[ "$version" == "$expected" ]] || fail "tag ${tag} does not match workspace version ${expected}"
  printf 'Release tag %s is annotated, signed, and matches workspace version %s\n' "$tag" "$expected"
}

# The release workflow's tagged-run check, run from the tag checkout: fetch
# the annotated tag object and main, require the tagged commit checked out,
# verify the tag against it, and require it on main.
verify_release_checkout() {
  local tag="$1" sha
  [[ "$tag" =~ $TAG_RE ]] || fail "invalid release tag: ${tag}"
  # Checkout leaves the annotated tag object behind; verify needs it.
  git fetch --force origin "refs/tags/${tag}:refs/tags/${tag}" "+refs/heads/main:refs/remotes/origin/main" \
    || fail "cannot fetch tag ${tag} and main"
  sha="$(git rev-parse "${tag}^{commit}")"
  # The version check reads RELEASE_ROOT, this checkout; it must be the tag.
  [[ "$(git rev-parse HEAD)" == "$sha" ]] || fail "checkout is not at tag ${tag} commit ${sha}"
  verify_release_tag "$tag" "$sha"
  git merge-base --is-ancestor "$sha" refs/remotes/origin/main || fail "tag ${tag} commit ${sha} is not on main"
}

case "${1:-}" in
  verify)
    shift
    [[ $# -ge 1 && $# -le 2 ]] || fail "usage: $0 verify <tag> [commit-sha]"
    verify_release_tag "$1" "${2:-}"
    ;;
  release)
    shift
    [[ $# -eq 1 ]] || fail "usage: $0 release <tag>"
    verify_release_checkout "$1"
    ;;
  *)
    fail "usage: $0 {verify <tag> [commit-sha]|release <tag>}"
    ;;
esac
