#!/usr/bin/env bash
# Release dry run for pull requests: self-test the release scripts, generate
# and schema-check the SBOM, write a manifest and the CHANGELOG notes for the
# workspace version, and check the workflow's tag-only gates. Never attests
# or publishes. The SBOM check needs cyclonedx-python-lib[json-validation].
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"
fail() { printf 'validate-release: %s\n' "$1" >&2; exit 1; }

python3 scripts/changelog-notes.py --self-test
python3 scripts/release-manifest.py --self-test
python3 scripts/release-sbom.py --self-test
bash scripts/verify-release-tag.sh self-test

workflow=.github/workflows/release.yml
preamble="$(sed -n '1,/^jobs:/p' "$workflow")"
grep -q 'pull_request' <<<"$preamble" && fail "${workflow} must not run on pull requests"
grep -qE 'id-token: write|attestations: write' <<<"$preamble" && fail "${workflow} grants attestation rights to every job"
grep -qF ':latest' "$workflow" && fail "${workflow} must not use a latest image tag"
grep -qF 'scripts/verify-release-tag.sh verify' "$workflow" || fail "${workflow} must verify the signed tag"
grep -qF 'scripts/changelog-notes.py' "$workflow" || fail "${workflow} must take the release body from CHANGELOG.md"

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
