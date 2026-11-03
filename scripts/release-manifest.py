#!/usr/bin/env python3
"""Write the Tenkai family release manifest (tenkai.family-release-manifest.v1).

    scripts/release-manifest.py --version V --git-sha SHA --dist DIR --out FILE
        [--tag vV] [--image NAME=sha256:...] [--attestation URL]...
        [--commits FILE --prs FILE]
    scripts/release-manifest.py --self-test

`binaries` are the downloadable release assets in DIR; the image archive is
listed under `images` by its registry digest instead. Gate flags come from the
labels of the pull requests whose squash commits (`... (#N)`) are in the
release: `--commits` holds one subject per line, `--prs` the JSON of
`gh pr list --json number,labels`. A commit naming a pull request missing from
that list fails, so a truncated lookup cannot drop a gate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
import tempfile

SCHEMA = "tenkai.family-release-manifest.v1"
COMPONENT = "mizuchi"
GATE_LABELS = {"risk:breaking": "breaking", "risk:migration": "migration"}
NOT_BINARIES = {"SHA256SUMS", "RELEASE_NOTES.md", f"{COMPONENT}-release.json", f"{COMPONENT}.cdx.json"}
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def binaries(dist: pathlib.Path) -> list[dict[str, str]]:
    return [
        {"name": path.name, "digest": sha256_file(path)}
        for path in sorted(dist.iterdir())
        if path.is_file() and path.name not in NOT_BINARIES and not path.name.endswith("-image.tar.gz")
    ]


def parse_image(raw: str) -> dict[str, str]:
    name, _, digest = raw.partition("=")
    if not name or "@" in name or name.endswith(":latest") or not DIGEST.fullmatch(digest):
        raise SystemExit(f"image must be NAME=sha256:<hex> without latest or an embedded digest: {raw}")
    return {"name": name, "digest": digest}


def gate_flags(subjects: list[str], prs: list[dict]) -> list[str]:
    labels = {pr["number"]: [label["name"].lower() for label in pr["labels"]] for pr in prs}
    flags: set[str] = set()
    for subject in subjects:
        match = re.search(r"\(#(\d+)\)\s*$", subject)
        if not match:
            continue
        number = int(match.group(1))
        if number not in labels:
            raise SystemExit(f"no labels for pull request #{number}; refusing to guess gate flags")
        flags.update(GATE_LABELS[name] for name in labels[number] if name in GATE_LABELS)
    return sorted(flags)


def build_manifest(
    *,
    version: str,
    git_sha: str,
    tag: str | None,
    dist: pathlib.Path,
    images: list[dict[str, str]],
    attestations: list[str],
    flags: list[str],
) -> dict:
    if not re.fullmatch(r"[0-9a-f]{40}", git_sha):
        raise SystemExit("git sha must be a full lowercase SHA")
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?", version):
        raise SystemExit(f"invalid version: {version}")
    if tag is not None and tag != f"v{version}":
        raise SystemExit(f"tag {tag} does not match version {version}")
    return {
        "schema": SCHEMA,
        "component": COMPONENT,
        "version": version,
        "git_sha": git_sha,
        "tag": tag,
        "binaries": binaries(dist),
        "images": images,
        "attestations": attestations,
        "gate_flags": flags,
    }


def expect_exit(message: str, call) -> None:
    try:
        call()
    except SystemExit as exc:
        assert message in str(exc), exc
    else:
        raise AssertionError(f"expected failure: {message}")


def self_test() -> None:
    prs = [
        {"number": 7, "labels": [{"name": "risk:breaking"}, {"name": "area:exec"}]},
        {"number": 8, "labels": [{"name": "RISK:migration"}]},
        {"number": 9, "labels": []},
    ]
    subjects = ["feat!: drop x (#7)", "fix: y (#8)", "docs: z (#9)", "direct push"]
    assert gate_flags(subjects, prs) == ["breaking", "migration"]
    expect_exit("#10", lambda: gate_flags(["fix: lost (#10)"], prs))
    for bad in ["ghcr.io/x:latest=sha256:" + "a" * 64, "ghcr.io/x=abc", "ghcr.io/x@sha256:1=sha256:" + "a" * 64]:
        expect_exit("image must be", lambda bad=bad: parse_image(bad))
    image = parse_image("ghcr.io/sannrox/mizuchi:0.1.0=sha256:" + "b" * 64)
    with tempfile.TemporaryDirectory() as tmp:
        dist = pathlib.Path(tmp)
        for name in ["mizuchi-0.1.0-source.tar.gz", "mizuchi-0.1.0-image.tar.gz", "SHA256SUMS", "mizuchi.cdx.json"]:
            (dist / name).write_bytes(b"x")
        manifest = build_manifest(
            version="0.1.0", git_sha="a" * 40, tag="v0.1.0", dist=dist,
            images=[image], attestations=["https://example.invalid/a/1"], flags=[],
        )
        assert [item["name"] for item in manifest["binaries"]] == ["mizuchi-0.1.0-source.tar.gz"]
        assert DIGEST.fullmatch(manifest["binaries"][0]["digest"])
        assert manifest["images"] == [image]
        expect_exit("full lowercase SHA", lambda: build_manifest(
            version="0.1.0", git_sha="abc", tag=None, dist=dist, images=[], attestations=[], flags=[]))
        expect_exit("does not match", lambda: build_manifest(
            version="0.1.0", git_sha="a" * 40, tag="v0.2.0", dist=dist, images=[], attestations=[], flags=[]))
    print("release-manifest self-test passed")


def main(argv: list[str]) -> None:
    if argv == ["--self-test"]:
        self_test()
        return
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--tag")
    parser.add_argument("--dist", required=True, type=pathlib.Path)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    parser.add_argument("--image", action="append", default=[], type=parse_image)
    parser.add_argument("--attestation", action="append", default=[])
    parser.add_argument("--commits", type=pathlib.Path)
    parser.add_argument("--prs", type=pathlib.Path)
    args = parser.parse_args(argv)
    if (args.commits is None) != (args.prs is None):
        parser.error("--commits and --prs go together")
    flags = []
    if args.commits is not None:
        flags = gate_flags(args.commits.read_text().splitlines(), json.loads(args.prs.read_text()))
    manifest = build_manifest(
        version=args.version, git_sha=args.git_sha, tag=args.tag, dist=args.dist,
        images=args.image, attestations=args.attestation, flags=flags,
    )
    args.out.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(args.out)


if __name__ == "__main__":
    main(sys.argv[1:])
