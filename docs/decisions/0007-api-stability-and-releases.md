# ADR 0007: API stability, versioning, and releases

- Status: Accepted
- Date: 2026-10-11
- Supersedes: —
- Source: [Issue #67](https://github.com/Sannrox/mizuchi/issues/67)

## Context

The library crate is `publish = false`. There is no written public-API
surface, SemVer policy, deprecation window, or release job. Embedding
runners (#19) and the Python wheel need a contract they can pin.

## Decision

### Public surface

The Rust public API is the crate-root re-exports in
`crates/mizuchi/src/lib.rs` (`Session`, `Engine`, `Batch`, `plan_sql`,
readers/writers, and the types those signatures name). Modules that are
`pub` exist so those types are reachable; `pub(crate)` items are not a
compatibility promise.

The Python public API is the `mizuchi` extension module built from
`python/` (lazy DataFrame, SQL, Arrow interchange).

### Versioning

Cargo SemVer:

- **0.y.z** (now): a bump in `y` may break the public surface. `z` is
  compatible fixes. Record breaking changes in `CHANGELOG.md`.
- **1.0.0 and after**: major for a breaking public-API change, minor for
  compatible additions, patch for compatible fixes.

`workspace.package.version` and `python/pyproject.toml` version stay in
lockstep.

### Deprecation

After 1.0, a public item is marked deprecated in one minor release and
is not removed until a later minor (at least one published release of
warning). Before 1.0, deprecation is best-effort and an item may leave
in the next 0.y.

### Releases

1. Commit the version section to `CHANGELOG.md` as `## [<version>]`,
   drafted from the merged pull requests (see the amendment below).
2. Tag `v<version>` on `main`.
3. CI on that tag packages every published workspace crate, dry-runs
   `cargo publish` for `mizuchi-core` (no unpublished path deps), and
   builds Python wheels on Linux, macOS, and Windows. The same workflow
   writes `SHA256SUMS`, attests those files, and creates a GitHub
   Release with the crate packages, wheels, checksums, and (on Linux)
   the Flight container image archive. `workflow_dispatch` packages the
   same crate and wheel files and does not attest or upload.
4. The tag also pushes `ghcr.io/sannrox/mizuchi:<version>` (never
   `latest`). `deploy/tenkai.toml` is the Tenkai product entry for that
   version.
5. crates.io and PyPI stay unpublished while any workspace crate pins a
   git dependency (`sekai-keys` today). A maintainer may publish
   `mizuchi-core`, then `mizuchi-plan`, `mizuchi-io`, `mizuchi-postgres`,
   `mizuchi-exec`, and `mizuchi` in that order (ADR 0013) only after
   those pins are registry crates. `mizuchi-flight` publishes with the
   Flight feature set. The workflow never uploads to those registries.

The `mizuchi` crate may be packaged (`publish = true`) with license,
readme, repository, and keywords. The Python cdylib crate stays
`publish = false`; it ships as a wheel. 0.1 is experimental.

### Amendment (2026-10-27): release-owned changelog

Pull requests no longer edit `CHANGELOG.md`; concurrent lanes kept
conflicting on it. The squash commit (pull request title and body)
carries the change note, with `Breaking:` / `Upgrade:` body lines and
the `risk:breaking` / `risk:migration` labels for breaks and upgrades.
Release prep runs `scripts/changelog-draft.sh <version>`, which groups
the pull requests merged since the last `v*` tag, and commits the
section. SemVer and the rest of this decision are unchanged.

## Consequences

- Callers pin `mizuchi = "0.1"` and expect breaks at 0.2.
- Default `cargo test` stays offline; the release job is tag/dispatch
  only. A `v*` tag is what produces the GitHub Release; cutting the tag
  is a maintainer action.
- #19 can treat the crate-root API as the embedder contract.

## Rejected alternatives

- **Stay git-only.** Blocks crates.io/PyPI consumers and a version pin.
- **Guarantee 1.0 stability now.** The surface is still growing (catalog,
  embedder API, SQL families).
- **Upload to crates.io/PyPI from every tag without a dry-run.** A bad
  tag would publish an unbuildable artifact. Git-pinned `sekai-keys`
  also makes a registry crate unpublishable until that pin is a
  registry crate.
- **Keep the tag job as a pack dry-run only.** A `v*` tag would not
  produce a GitHub Release or a Tenkai-deliverable image.
