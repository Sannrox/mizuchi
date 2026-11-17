#!/usr/bin/env python3
"""Write the Tenkai family release manifest (tenkai.family-release-manifest.v1).

    scripts/release/manifest.py --version V --git-sha SHA --dist DIR --out FILE
        [--tag vV] [--image NAME=sha256:...] [--attestation URL]...

`binaries` are the downloadable release assets in DIR; the image archive is
listed under `images` by its registry digest instead. With `--tag`, gate flags
come from the labels of the pull requests whose squash commits (`... (#N)`)
are in the release since the previous stable tag, read with `git log` from the
current directory and `gh pr list` (needs GH_TOKEN and GH_REPO). A failed
lookup, a commit whose subject names no pull request, or one naming a pull
request missing from the list fails, so no gate is dropped silently.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import subprocess
import sys

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


def gate_flags(commits: list[tuple[str, str]], prs: list[dict]) -> list[str]:
    """Gate flags from the labels of each (sha, subject) commit's pull request."""
    labels = {pr["number"]: [label["name"].lower() for label in pr["labels"]] for pr in prs}
    flags: set[str] = set()
    for sha, subject in commits:
        match = re.search(r"\(#(\d+)\)\s*$", subject)
        if not match:
            raise SystemExit(f"commit {sha} names no pull request ({subject!r}); refusing to guess gate flags")
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


def run(*args: str) -> str:
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        raise SystemExit(f"{' '.join(args[:3])} failed: {result.stderr.strip()}")
    return result.stdout


def release_commits(tag: str) -> list[tuple[str, str]]:
    """(sha, subject) of each commit since the previous stable v* tag, so an
    upgrade from that release sees every gate, including ones shipped in a
    pre-release."""
    previous = subprocess.run(
        ["git", "describe", "--tags", "--abbrev=0", "--match", "v*", "--exclude", "v*-*", f"{tag}^"],
        capture_output=True, text=True,
    )
    span = f"{previous.stdout.strip()}..{tag}" if previous.returncode == 0 else tag
    lines = run("git", "log", "--format=%H %s", span).splitlines()
    return [(sha, subject) for sha, _, subject in (line.partition(" ") for line in lines)]


def merged_prs() -> list[dict]:
    return json.loads(run("gh", "pr", "list", "--state", "merged", "--limit", "1000", "--json", "number,labels"))


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--git-sha", required=True)
    parser.add_argument("--tag")
    parser.add_argument("--dist", required=True, type=pathlib.Path)
    parser.add_argument("--out", required=True, type=pathlib.Path)
    parser.add_argument("--image", action="append", default=[], type=parse_image)
    parser.add_argument("--attestation", action="append", default=[])
    args = parser.parse_args(argv)
    flags = gate_flags(release_commits(args.tag), merged_prs()) if args.tag else []
    manifest = build_manifest(
        version=args.version, git_sha=args.git_sha, tag=args.tag, dist=args.dist,
        images=args.image, attestations=args.attestation, flags=flags,
    )
    args.out.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(args.out)


if __name__ == "__main__":
    main(sys.argv[1:])
