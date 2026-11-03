# Releasing mizuchi

The release version is `[workspace.package].version` in `Cargo.toml`.
Choose a commit with green CI and a matching dated CHANGELOG section. Create
an annotated, signed `vX.Y.Z` tag on that commit. Never move or reuse a tag.
The workflow refuses a lightweight or unsigned tag, a signature from a key
outside `security/release-tag-fingerprints` (public key in
`security/release-tag-key.asc`), and a tag that is not `v` plus the workspace
version (`scripts/verify-release-tag.sh`).

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

The release workflow's manual run verifies source packaging, writes and
checks the SBOM, and builds the wheels without publishing. A tag push first
verifies the tag, then builds and pushes the versioned image, checksums and
attests all artifacts, and writes and attests the manifest. It uploads all
assets to a draft with the CHANGELOG section as the body
(`scripts/changelog-notes.py`) before publishing. A failed check, build or
attestation prevents publication.

The manifest (`tenkai.family-release-manifest.v1`) lists the version, git
SHA, asset digests, the image by registry digest, the attestation URLs, and
gate flags: `breaking` or `migration` when a pull request merged since the
previous stable `v*` tag carries `risk:breaking` or `risk:migration`. A squash
subject naming a pull request whose labels cannot be read fails the release.

Every pull request runs `scripts/validate-release.sh` in CI: script
self-tests, a CycloneDX schema and SPDX check of the generated SBOM, a dry
manifest, and the CHANGELOG notes for the workspace version. Run it locally
with `cyclonedx-python-lib[json-validation]` installed.
