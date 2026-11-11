#!/usr/bin/env python3
"""Render and check the Tenkai compatibility profile mizuchi publishes.

    scripts/tenkai-profile.py --render --version V --digest sha256:<hex> --out FILE
    scripts/tenkai-profile.py --check

`deploy/tenkai.toml` is the committed template and carries no
`[compatibility]` block: a tagged commit cannot hold the digest of the image
built from it. The release renders the template plus a profile v2 block whose
`worker` component is pinned to the pushed image digest and provides
`CONTRACT` at `CONTRACT_VERSION`, and publishes it as `mizuchi-tenkai.toml`.

`CONTRACT_VERSION` is the single source for the contract version. It is not
the crate version. Bump the major when the Flight worker's query surface
(SQL accepted, result schema, Flight endpoints) changes incompatibly, the
minor when it grows compatibly, and the patch for fixes only.

The render also pins the install command's `IMG=` to the same digest, so the
deployed worker is the image the profile names.

`--check` (CI) fails when the template already has a compatibility block,
`deploy/README.md` does not state the contract and version, or a sample render
does not parse back to the expected profile. `--render` refuses a digest that
is not canonical and a release version that differs from `[product].version`.
"""

from __future__ import annotations

import argparse
import pathlib
import re
import sys
import tomllib

CONTRACT = "mizuchi.query"
CONTRACT_VERSION = "1.0.0"
COMPONENT = "worker"
PROFILE_VERSION = 2
# Evidence a dependent's environment owner records stays valid for a day.
MAX_EVIDENCE_AGE_MS = 86_400_000

IMAGE = "ghcr.io/sannrox/mizuchi"
ROOT = pathlib.Path(__file__).resolve().parent.parent
TEMPLATE = ROOT / "deploy" / "tenkai.toml"
README = ROOT / "deploy" / "README.md"
DIGEST = re.compile(r"sha256:[0-9a-f]{64}")
SEMVER = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")


def fail(message: str) -> None:
    raise SystemExit(f"tenkai-profile: {message}")


def render(template: str, version: str, digest: str) -> str:
    """Return the template with its install image pinned to `digest` and the
    profile v2 block appended, so the deployed bytes match the pin."""
    if not DIGEST.fullmatch(digest):
        fail(f"digest must be sha256:<64 lowercase hex>: {digest}")
    parsed = tomllib.loads(template)
    if "compatibility" in parsed:
        fail("the template must not carry a [compatibility] block; the release renders it")
    product = parsed.get("product", {}).get("version")
    if product != version:
        fail(f"[product].version {product!r} does not match release version {version!r}")
    tagged = f"IMG={IMAGE}:{version}\n"
    if template.count(tagged) != 1:
        fail(f"the template must set {tagged.strip()} exactly once")
    template = template.replace(tagged, f"IMG={IMAGE}@{digest}\n")
    block = f"""
# Rendered by scripts/tenkai-profile.py at release; not in the source tree.
[compatibility]
version = {PROFILE_VERSION}
max_evidence_age_ms = {MAX_EVIDENCE_AGE_MS}

[compatibility.components.{COMPONENT}.pin]
kind = "digest"
value = "{digest}"

[compatibility.components.{COMPONENT}.provides]
"{CONTRACT}" = "{CONTRACT_VERSION}"
"""
    return template.rstrip("\n") + "\n" + block


def check() -> None:
    if not SEMVER.fullmatch(CONTRACT_VERSION):
        fail(f"contract version must be MAJOR.MINOR.PATCH: {CONTRACT_VERSION}")
    template = TEMPLATE.read_text()
    readme = README.read_text()
    if f"`{CONTRACT}`" not in readme or f"`{CONTRACT_VERSION}`" not in readme:
        fail(f"{README.name} must state `{CONTRACT}` at `{CONTRACT_VERSION}`")
    version = tomllib.loads(template)["product"]["version"]
    digest = "sha256:" + "0" * 64
    rendered = render(template, version, digest)
    if f"IMG={IMAGE}@{digest}\n" not in rendered or f"{IMAGE}:{version}" in rendered:
        fail("the rendered install must pull the image by digest only")
    profile = tomllib.loads(rendered)["compatibility"]
    expected = {
        "version": PROFILE_VERSION,
        "max_evidence_age_ms": MAX_EVIDENCE_AGE_MS,
        "components": {
            COMPONENT: {
                "pin": {"kind": "digest", "value": digest},
                "provides": {CONTRACT: CONTRACT_VERSION},
            }
        },
    }
    if profile != expected:
        fail(f"rendered profile {profile} != {expected}")
    for bad in ["sha256:abc", "sha256:" + "A" * 64, "0" * 64]:
        try:
            render(template, version, bad)
        except SystemExit:
            continue
        fail(f"render accepted digest {bad}")
    try:
        render(template, version + "-other", digest)
    except SystemExit:
        pass
    else:
        fail("render accepted a release version that differs from [product].version")


def main(argv: list[str]) -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--render", action="store_true")
    mode.add_argument("--check", action="store_true")
    parser.add_argument("--version")
    parser.add_argument("--digest")
    parser.add_argument("--out", type=pathlib.Path)
    args = parser.parse_args(argv)
    if args.check:
        check()
        print(f"tenkai-profile: {CONTRACT} {CONTRACT_VERSION} ok")
        return
    if not (args.version and args.digest and args.out):
        parser.error("--render needs --version, --digest and --out")
    args.out.write_text(render(TEMPLATE.read_text(), args.version, args.digest))


if __name__ == "__main__":
    main(sys.argv[1:])
