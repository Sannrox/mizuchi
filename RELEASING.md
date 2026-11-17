# Releasing mizuchi

The release version is `[workspace.package].version` in `Cargo.toml`.
Choose a commit on `main` with green CI and a matching dated CHANGELOG
section. Create an annotated, signed `vX.Y.Z` tag on that commit. Never move
or reuse a tag. The workflow refuses a lightweight or unsigned tag, a
signature from a key outside `security/release-tag-fingerprints` (public key
in `security/release-tag-key.asc`), a tag that is not `v` plus the workspace
version (`scripts/release/verify-tag.sh`), and a tagged commit that is not on
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
`scripts/release/verify-tag.sh` and allowed keys against the tagged commit's
`Cargo.toml`, and requires that commit to be on `main`. Every later job checks
out the tagged commit and checks `HEAD` before it packages, builds, tags the
image, writes the manifest `git_sha`, or creates the release with
`gh release create <tag> --verify-tag`.

To retry a failed release, fix the workflow on `main` and run it again for the
same tag:

```sh
gh workflow run release.yml --ref main -f tag=vX.Y.Z
```

A retry picks up fixes to `release.yml`, the allowed keys, and the two
scripts the `verify-tag` job runs from main's tree (`verify-tag.sh` and the
first `github-release.sh guard`) only. Everything else (the other release
scripts, `CHANGELOG.md`, the Dockerfile and the sources) comes from the
tagged commit, so a bug there needs a new rc or patch version.

A re-run resumes the draft release: it deletes the earlier run's assets and
uploads the fresh ones. It refuses a release that is already published before
it builds, in every job before it pushes the image, attests or changes the
release, and before it publishes; cut a new version instead. A failed release
lookup fails the run rather than counting as no release. Before drafting and
before publishing, the run also checks that the remote tag still names the
tagged commit. One run per tag executes at a time.

The image job pushes only `staging-<version>`. The version tag is applied
after attestation and the draft, to the attested digest only, and
`image.sh promote` refuses to move a version tag that already names another
digest. A re-run rebuilds the image at a new digest, so a run that failed
after tagging the image cannot be resumed; cut a new version instead.

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

Run `bash scripts/release/package.sh dist` from a clean checkout to create the
source archive and compile the unpacked workspace with its lockfile. The
script uses this checkout's `target/source-package-check` directory and needs
Python 3.11 or newer. Cargo still needs access to the pinned git dependencies
and registry dependencies when checking or building the archive.

A dry run verifies source packaging, writes and checks the SBOM, and builds
the wheels without publishing. A tagged run first verifies the tag, then
builds and pushes the image under its staging tag, checksums and attests all
artifacts, and writes and attests the manifest. It uploads all assets to a
draft with the CHANGELOG section as the body (`scripts/release/notes.py`),
then points the version tag at the attested image digest, before
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

## Checks

The workflows orchestrate; the logic lives in `scripts/release/`, one entry
point per stage: `verify-tag.sh` (signed tag), `package.sh` (source
archive), `sbom.py`, `image.sh` (staging image and its registry digest, then the version tag),
`tenkai_profile.py`, `manifest.py`, `notes.py`, and `github-release.sh`, the
only script that changes a GitHub Release. Each refusal above is enforced in
the script at the step it protects.

Every pull request runs two CI jobs:

- `Lint workflows`: `bash scripts/check-workflows.sh` runs pinned actionlint
  and zizmor over `.github/workflows/`, the same binaries locally and in CI.
- `Release tests`: `tests/release/` tests each release script (real signed
  tags in a scratch repository, a fake `gh` and `docker`, a schema-checked
  SBOM of this workspace) and the release workflows' structure on the parsed
  YAML (triggers, permissions, job graph, step order).

Run them locally with:

```sh
bash scripts/check-workflows.sh
python3 -m pip install --require-hashes -r tests/requirements.txt
python3 -m pip install 'cyclonedx-python-lib[json-validation]==11.12.0'
python3 -m unittest discover -s tests -t .
```

The tests need `git`, `gpg`, and `cargo`.
