# Releasing mizuchi

The release version is `[workspace.package].version` in `Cargo.toml`.
Choose a commit on `main` with green CI and a matching dated CHANGELOG
section. Create an annotated, signed `vX.Y.Z` tag on that commit. Never move
or reuse a tag. The workflow refuses a lightweight or unsigned tag, a
signature from a key outside `security/release-tag-fingerprints` (public key
in `security/release-tag-key.asc`), a tag that is not `v` plus the workspace
version (`scripts/verify-release-tag.sh`), and a tagged commit that is not on
`main`.

## Release candidates

Cut `vX.Y.Z-rc.N` (or `vX.Y.Z-beta.N`) before the final tag:

1. Land a version bump that sets the workspace version to `X.Y.Z-rc.N` and
   adds a `## [X.Y.Z-rc.N]` section to `CHANGELOG.md`. The final `vX.Y.Z`
   needs its own section; it never reuses the rc notes.
2. Tag `vX.Y.Z-rc.N` on that commit.
3. The release publishes as a GitHub pre-release and is never marked latest.

A failed release candidate costs only an rc number. A final tag on a commit
whose workspace version is still `X.Y.Z-rc.N` is refused.

## Start, retry, and dry run

A `v*` tag push runs `.github/workflows/release-dispatch.yml`. It only runs
`gh workflow run release.yml --ref main -f tag=<tag>`, so a release always
runs main's `release.yml`, never the copy at the tagged commit.

`release.yml` refuses a tagged run that does not start from `main`. It checks
out the tag, verifies the signed tag with main's
`scripts/verify-release-tag.sh` and allowed keys against the tagged commit's
`Cargo.toml`, and requires that commit to be on `main`. Every later job checks
out the tagged commit and checks `HEAD` before it packages, builds, tags the
image, writes the manifest `git_sha`, or creates the release with
`gh release create <tag> --verify-tag`.

To retry a failed release, fix the workflow on `main` and run it again for the
same tag:

```sh
gh workflow run release.yml --ref main -f tag=vX.Y.Z
```

A retry picks up fixes to `release.yml` and to the tag verifier and allowed
keys only. Everything else (the release scripts, `CHANGELOG.md`, the
Dockerfile and the sources) comes from the tagged commit, so a bug there needs
a new rc or patch version.

A re-run resumes the draft release: it deletes the earlier run's assets and
uploads the fresh ones. It refuses a release that is already published before
it builds, in every job before it pushes the image, attests or changes the
release, and before it publishes; cut a new version instead. A failed release
lookup fails the run rather than counting as no release. Before drafting and
before publishing, the run also checks that the remote tag still names the
tagged commit. One run per tag executes at a time. A
re-run rebuilds and re-pushes the versioned image, so the image digest in the
manifest is the one from the run that published.

`workflow_dispatch` without a tag is a dry run on the chosen ref.

## Artifacts

The release contains a complete source-workspace archive, platform wheels,
the versioned image archive, a CycloneDX SBOM (`mizuchi.cdx.json`), the
Tenkai family release manifest (`mizuchi-release.json`), and `SHA256SUMS`.
Provenance attestations cover the files and the image digest; an SBOM
attestation covers the source archive and wheels. The source
archive retains internal path dependencies and pinned git dependencies.
Individual registry packages would discard those relationships; registry
publication is outside this release process.

Run `bash scripts/package-source.sh dist` from a clean checkout to create the
source archive and compile the unpacked workspace with its lockfile. The
script uses this checkout's `target/source-package-check` directory and needs
Python 3.11 or newer. Cargo still needs access to the pinned git dependencies
and registry dependencies when checking or building the archive.

A dry run verifies source packaging, writes and checks the SBOM, and builds
the wheels without publishing. A tagged run first verifies the tag, then
builds and pushes the versioned image, checksums and attests all artifacts,
and writes and attests the manifest. It uploads all assets to a draft with
the CHANGELOG section as the body (`scripts/changelog-notes.py`) before
publishing. A failed check, build or attestation prevents publication and
leaves any draft for a re-run.

## Provenance

Attestations record the workflow that built the artifacts, which is main's
`release.yml` at `refs/heads/main`, not the tag. The source is pinned by the
signed tag and by the manifest `git_sha`, which is the tagged commit.

The manifest (`tenkai.family-release-manifest.v1`) lists the version, git
SHA, asset digests, the image by registry digest, the attestation URLs, and
gate flags: `breaking` or `migration` when a pull request merged since the
previous stable `v*` tag carries `risk:breaking` or `risk:migration`. A squash
subject naming a pull request whose labels cannot be read fails the release.

Every pull request runs `scripts/validate-release.sh` in CI: script
self-tests, a CycloneDX schema and SPDX check of the generated SBOM, a dry
manifest, and the CHANGELOG notes for the workspace version. Run it locally
with `cyclonedx-python-lib[json-validation]` installed.
