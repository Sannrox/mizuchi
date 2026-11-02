#!/usr/bin/env bash
# Package the complete workspace, retaining path dependencies and git pins.
set -euo pipefail

root="$(git rev-parse --show-toplevel)"
cd "$root"
git diff --quiet HEAD -- || { echo 'Source packaging requires a clean tracked tree' >&2; exit 1; }
out="${1:?usage: package-source.sh OUTPUT_DIRECTORY}"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
version="$(python3 -c 'import pathlib,tomllib; print(tomllib.loads(pathlib.Path("Cargo.toml").read_text())["workspace"]["package"]["version"])')"
if [[ "${GITHUB_REF_TYPE:-}" == tag && "${GITHUB_REF_NAME:-}" != "v$version" ]]; then
  echo 'Release tag must match the workspace version' >&2
  exit 1
fi
scratch="$(mktemp -d "$out/.source-package.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
archive="$scratch/mizuchi-${version}-source.tar.gz"
git archive --format=tar.gz --prefix="mizuchi-${version}/" HEAD > "$archive"
tar -xzf "$archive" -C "$scratch"
# Check the shipped bytes rather than accidentally compiling the checkout.
CARGO_TARGET_DIR="$root/target/source-package-check" cargo check --locked --workspace \
  --manifest-path "$scratch/mizuchi-${version}/Cargo.toml"
mv "$archive" "$out/mizuchi-${version}-source.tar.gz"
printf '%s\n' "$out/mizuchi-${version}-source.tar.gz"
