#!/usr/bin/env bash
# Run exactly one named test, and fail when the name selects no test.
#
# `cargo test <filter>` exits 0 when the filter matches nothing, so a renamed
# test turns a workflow step into a silent no-op. This lists first with
# `--exact` and requires exactly one match.
#
# Usage: cargo-test-exact.sh <full::test::path> [cargo args...] [-- libtest args...]
# Example: cargo-test-exact.sh plan::x::tests::t --locked -p mizuchi-plan --lib -- --ignored
set -euo pipefail

if [ "$#" -lt 1 ]; then
  echo "usage: $0 <full::test::path> [cargo args...] [-- libtest args...]" >&2
  exit 2
fi
name=$1
shift

cargo_args=()
while [ "$#" -gt 0 ] && [ "$1" != "--" ]; do
  cargo_args+=("$1")
  shift
done
[ "$#" -gt 0 ] && shift
libtest_args=("$@")

list=$(cargo test "${cargo_args[@]}" -- "$name" --exact "${libtest_args[@]}" --list)
n=$(printf '%s\n' "$list" | grep -c ': test$' || true)
if [ "$n" -ne 1 ]; then
  echo "expected exactly one test named $name, selected $n" >&2
  exit 1
fi
cargo test "${cargo_args[@]}" -- "$name" --exact "${libtest_args[@]}"
