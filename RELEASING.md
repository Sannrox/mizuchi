# Releasing mizuchi

The release version is `[workspace.package].version` in `Cargo.toml`.
Choose a commit with green CI and a matching dated CHANGELOG section. Create
an annotated, signed `vX.Y.Z` tag on that commit. Never move or reuse a tag.

## Artifacts

The release contains a complete source-workspace archive, platform wheels,
the versioned image archive, `SHA256SUMS`, and build provenance. The source
archive retains internal path dependencies and pinned git dependencies.
Individual registry packages would discard those relationships; registry
publication is outside this release process.

Run `bash scripts/package-source.sh dist` from a clean checkout to create the
source archive and compile the unpacked workspace with its lockfile. The
script uses this checkout's `target/source-package-check` directory and needs
Python 3.11 or newer. Cargo still needs access to the pinned git dependencies
and registry dependencies when checking or building the archive.

The release workflow's manual run verifies source packaging and builds the
wheels without publishing. A tag push also builds the versioned image, then
checksums and attests all artifacts. It uploads all assets to a draft before
publishing. A failed build or attestation prevents publication.
