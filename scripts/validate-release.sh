#!/usr/bin/env bash
# Release dry run for pull requests: self-test the release scripts, generate
# and schema-check the SBOM, write a manifest and the CHANGELOG notes for the
# workspace version, and check the release workflows' dispatch, main-only,
# tagged-commit and pre-release invariants. Never attests or publishes. The
# SBOM check needs cyclonedx-python-lib[json-validation].
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
fail() { printf 'validate-release: %s\n' "$1" >&2; exit 1; }

python3 scripts/changelog-notes.py --self-test
python3 scripts/release-manifest.py --self-test
python3 scripts/release-sbom.py --self-test
bash scripts/verify-release-tag.sh self-test

workflow=.github/workflows/release.yml
dispatcher=.github/workflows/release-dispatch.yml
[[ -f "$dispatcher" ]] || fail "missing ${dispatcher}"

# The dispatcher only turns a v* tag push into a main run of release.yml.
grep -qF 'tags: ["v*"]' "$dispatcher" || fail "${dispatcher} must trigger on v* tags"
grep -qF 'actions: write' "$dispatcher" || fail "${dispatcher} must be able to dispatch workflows"
if grep -qE '^[[:space:]]*(-[[:space:]]+)?uses:|workflow_dispatch|pull_request|contents: write' "$dispatcher"; then
  fail "${dispatcher} must only dispatch: no actions, other triggers, or write access"
fi
dispatch_runs="$(grep -E '^[[:space:]]*(-[[:space:]]+)?run:' "$dispatcher")"
[[ "$dispatch_runs" =~ ^[[:space:]]*run:\ gh\ workflow\ run\ release\.yml\ --ref\ main\ -f\ tag=\"\$TAG\"$ ]] \
  || fail "${dispatcher} must run only: gh workflow run release.yml --ref main -f tag=\"\$TAG\""

preamble="$(sed -n '1,/^jobs:/p' "$workflow")"
grep -q 'pull_request' <<<"$preamble" && fail "${workflow} must not run on pull requests"
grep -qE '^  push:|^[[:space:]]+tags:' <<<"$preamble" && fail "${workflow} must not run on push; release-dispatch.yml starts it from main"
grep -qF 'workflow_dispatch:' <<<"$preamble" || fail "${workflow} must allow a tag input and a dry run"
grep -qF 'group: release-${{ inputs.tag || github.ref }}' <<<"$preamble" || fail "${workflow} must run one release per tag"
grep -qE 'id-token: write|attestations: write' <<<"$preamble" && fail "${workflow} grants attestation rights to every job"
grep -qF ':latest' "$workflow" && fail "${workflow} must not use a latest image tag"
grep -qF 'scripts/changelog-notes.py' "$workflow" || fail "${workflow} must take the release body from CHANGELOG.md"

# A tagged run starts from main, checks out the tag, and verifies it with
# main's verifier and keys against the tagged commit, which must be on main.
grep -qF '[[ "$WORKFLOW_REF" == refs/heads/main ]]' "$workflow" || fail "${workflow} must refuse tagged runs that are not from main"
grep -qF "ref: \${{ inputs.tag && format('refs/tags/{0}', inputs.tag) || '' }}" "$workflow" || fail "${workflow} must check out the release tag"
grep -qF 'git fetch --force origin "refs/tags/${TAG}:refs/tags/${TAG}"' "$workflow" || fail "${workflow} must fetch the annotated tag object"
grep -qF 'sha="$(git rev-parse "${TAG}^{commit}")"' "$workflow" || fail "${workflow} must resolve the tagged commit"
grep -qF 'bash "${MAIN_TREE}/scripts/verify-release-tag.sh" verify "$TAG" "$sha"' "$workflow" || fail "${workflow} must verify the tag with main's verifier against the tagged commit"
grep -qF 'RELEASE_TAG_FINGERPRINTS="${MAIN_TREE}/security/release-tag-fingerprints"' "$workflow" || fail "${workflow} must verify against main's allowed keys"
grep -qF 'git merge-base --is-ancestor "$sha" refs/remotes/origin/main' "$workflow" || fail "${workflow} must require the tagged commit on main"

# Every later job builds the tagged commit, never the event SHA or ref name.
checkouts="$(grep -c 'uses: actions/checkout@' "$workflow")"
sha_checkouts="$(grep -c 'ref: ${{ needs.verify-tag.outputs.sha }}' "$workflow")"
head_checks="$(grep -c 'test "$(git rev-parse HEAD)" = "$SHA"' "$workflow")"
[[ "$checkouts" -eq $((sha_checkouts + 2)) ]] || fail "${workflow} must check out the release commit in every job after verify-tag"
[[ "$head_checks" -eq "$sha_checkouts" ]] || fail "${workflow} must check HEAD after every release-commit checkout"
if grep -qE 'github\.sha|github\.ref_name|GITHUB_SHA|GITHUB_REF|github\.event_name' "$workflow"; then
  fail "${workflow} must use the tagged commit and tag input, not the event SHA or ref name"
fi
grep -qF -- '--git-sha "$SHA"' "$workflow" || fail "${workflow} must record the tagged commit as the manifest git_sha"
grep -qF "if: needs.verify-tag.outputs.tag != ''" "$workflow" || fail "${workflow} must gate image, attest and publish on a verified tag"

# Pre-releases are never latest; a re-run resumes a draft and refuses a
# published release early, when drafting, and before publishing.
grep -qF -- '--draft --verify-tag "${kind[@]}"' "$workflow" || fail "${workflow} must draft the release for the existing tag"
grep -qF '[[ "$TAG" == *-* ]]' "$workflow" || fail "${workflow} must treat a tag with - as a pre-release"
grep -qF -- 'kind=(--prerelease --latest=false)' "$workflow" || fail "${workflow} must draft a pre-release as never latest"
grep -qF -- 'gh release edit "$TAG" --draft=false --prerelease --latest=false' "$workflow" || fail "${workflow} must publish a pre-release as never latest"
grep -qE -- '--latest( |$|")' "$workflow" && fail "${workflow} must never force a release to latest"
[[ "$(grep -c 'is already published; cut a new tag instead of re-running' "$workflow")" -ge 3 ]] \
  || fail "${workflow} must refuse a published release early, when drafting, and before publishing"
# "Re-run failed jobs" reuses verify-tag's outputs, so every job that changes a
# release or pushes an image refuses a published release itself.
python3 - "$workflow" <<'PY_JOBS' || fail "${workflow} must refuse a published release in every job that uploads, publishes, or pushes"
import re, sys
body = open(sys.argv[1]).read().split("\njobs:\n", 1)[1]
mutating = ("gh release upload", "gh release create", "gh release edit", "docker push")
missing = [
    block.split(":", 1)[0].strip()
    for block in re.split(r"(?m)^(?=  [a-z][a-z0-9-]*:\n)", body)
    if any(m in block for m in mutating)
    and "is already published; cut a new tag instead of re-running" not in block
]
if missing:
    sys.exit("unguarded jobs: " + ", ".join(missing))
PY_JOBS
grep -qF 'gh release delete-asset "$TAG" "$name" --yes' "$workflow" \
  || fail "${workflow} must drop an earlier run's assets when it resumes a draft"
grep -qF 'needs: [verify-tag, source, wheels, image, attest]' "$workflow" \
  || fail "${workflow} must publish only after every asset job, so a re-run can resume the draft"

version="$(python3 -c 'import pathlib,tomllib; print(tomllib.loads(pathlib.Path("Cargo.toml").read_text())["workspace"]["package"]["version"])')"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/mizuchi-release-dry-run.$$.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
dist="$scratch/dist"
mkdir -p "$dist"

python3 scripts/release-sbom.py "$dist/mizuchi.cdx.json" >/dev/null
python3 scripts/release-sbom.py --check "$dist/mizuchi.cdx.json"

python3 scripts/changelog-notes.py "$version" > "$dist/RELEASE_NOTES.md"
printf 'source\n' > "$dist/mizuchi-${version}-source.tar.gz"
printf '[]\n' > "$scratch/prs.json"
: > "$scratch/commits"
python3 scripts/release-manifest.py --version "$version" --git-sha "$(git rev-parse HEAD)" \
  --tag "v${version}" --dist "$dist" --out "$dist/mizuchi-release.json" \
  --image "ghcr.io/sannrox/mizuchi:${version}=sha256:$(printf '0%.0s' {1..64})" \
  --commits "$scratch/commits" --prs "$scratch/prs.json" >/dev/null
python3 - "$dist/mizuchi-release.json" "$version" <<'PY'
import json, sys
manifest = json.load(open(sys.argv[1]))
assert manifest["schema"] == "tenkai.family-release-manifest.v1", manifest
assert manifest["component"] == "mizuchi" and manifest["version"] == sys.argv[2], manifest
assert [b["name"] for b in manifest["binaries"]] == [f"mizuchi-{sys.argv[2]}-source.tar.gz"], manifest
assert "latest" not in json.dumps(manifest), manifest
PY
printf 'validate-release passed (version %s)\n' "$version"
