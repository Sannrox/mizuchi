#!/usr/bin/env python3
"""Write and check the release CycloneDX 1.5 SBOM.

    scripts/release-sbom.py OUT          # from `cargo metadata --locked`
    scripts/release-sbom.py --check OUT  # schema + SPDX check; needs
                                         # cyclonedx-python-lib[json-validation]
    scripts/release-sbom.py --self-test

The SBOM covers what the release ships: the library workspace with all
features (the image enables `flight`) and the separate `python/` workspace
behind the wheels. Dev-only dependencies are left out.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import urllib.parse
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
WORKSPACES = [ROOT, ROOT / "python"]
SPEC_VERSION = "1.5"


def cargo_metadata(workspace: Path) -> dict:
    raw = subprocess.check_output(
        ["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"],
        cwd=workspace,
        text=True,
    )
    return json.loads(raw)


def shipped_ids(meta: dict) -> set[str]:
    """Package ids reachable from workspace members through non-dev edges."""
    nodes = {node["id"]: node for node in meta["resolve"]["nodes"]}
    seen: set[str] = set()
    stack = list(meta["workspace_members"])
    while stack:
        pid = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        for dep in nodes[pid]["deps"]:
            if any(kind["kind"] != "dev" for kind in dep["dep_kinds"]):
                stack.append(dep["pkg"])
    return seen


def purl(pkg: dict) -> str:
    base = f"pkg:cargo/{pkg['name']}@{pkg['version']}"
    source = pkg.get("source") or ""
    # A git pin is not the crates.io package of that name; say where it is.
    if source.startswith("git+"):
        return f"{base}?vcs_url={urllib.parse.quote(source, safe='')}"
    return base


# Cargo `license` is an SPDX expression or the legacy `A/B` form. Either way it
# belongs in CycloneDX's `expression`: a compound value under `license.id` is
# not a valid document and the SBOM attestation rejects it.
def license_expression(raw: str) -> str:
    return re.sub(r"\s*/\s*", " OR ", raw.strip())


def component(pkg: dict) -> dict:
    ref = purl(pkg)
    item = {"type": "library", "bom-ref": ref, "name": pkg["name"], "version": pkg["version"], "purl": ref}
    if pkg.get("license"):
        item["licenses"] = [{"expression": license_expression(pkg["license"])}]
    return item


def bom_from_metadata(metas: list[dict]) -> dict:
    root = None
    components: dict[str, dict] = {}
    for meta in metas:
        shipped = shipped_ids(meta)
        for pkg in meta["packages"]:
            if pkg["id"] not in shipped:
                continue
            if pkg["name"] == "mizuchi" and pkg["id"] in meta["workspace_members"]:
                root = pkg
                continue
            item = component(pkg)
            components[item["bom-ref"]] = item
    if root is None:
        raise SystemExit("cargo metadata has no mizuchi workspace package")
    metadata_component = component(root) | {"type": "application"}
    # `python/` reaches the facade as a path dependency; it is the root here.
    components.pop(metadata_component["bom-ref"], None)
    bom = {
        "bomFormat": "CycloneDX",
        "specVersion": SPEC_VERSION,
        "version": 1,
        "metadata": {"component": metadata_component},
        "components": [components[ref] for ref in sorted(components)],
    }
    # The same lockfile yields the same document identity on a rebuild.
    document = json.dumps(bom, sort_keys=True, separators=(",", ":"))
    bom["serialNumber"] = uuid.uuid5(uuid.NAMESPACE_URL, document).urn
    return bom


def check(path: Path) -> None:
    from cyclonedx.schema import SchemaVersion
    from cyclonedx.validation.json import JsonStrictValidator
    from license_expression import get_spdx_licensing

    text = path.read_text()
    error = JsonStrictValidator(SchemaVersion.from_version(SPEC_VERSION)).validate_str(text)
    if error is not None:
        detail = error.data  # jsonschema.ValidationError
        where = "/".join(map(str, detail.absolute_path))
        raise SystemExit(f"{path} is not valid CycloneDX {SPEC_VERSION} at /{where}: {detail.message}")
    bom = json.loads(text)
    if not re.fullmatch(r"urn:uuid:[0-9a-f-]{36}", bom.get("serialNumber", "")):
        raise SystemExit(f"{path} has no urn:uuid serialNumber")
    licensing = get_spdx_licensing()
    items = [bom["metadata"]["component"], *bom["components"]]
    for item in items:
        for entry in item.get("licenses", []):
            if "expression" not in entry:
                raise SystemExit(f"{item['bom-ref']}: license must be an SPDX expression, got {entry}")
            errors = licensing.validate(entry["expression"]).errors
            if errors:
                raise SystemExit(f"{item['bom-ref']}: {entry['expression']}: {errors}")
    print(f"{path}: valid CycloneDX {SPEC_VERSION}, {len(bom['components'])} components")


def self_test() -> None:
    def pkg(name: str, version: str, source: str | None, license: str | None) -> dict:
        pid = f"{source or 'path+file:///w'}#{name}@{version}"
        return {"id": pid, "name": name, "version": version, "source": source, "license": license}

    registry = "registry+https://github.com/rust-lang/crates.io-index"
    root = pkg("mizuchi", "0.1.0", None, "Apache-2.0")
    libc = pkg("libc", "0.2.0", registry, "MIT/Apache-2.0")
    keys = pkg("sekai-keys", "0.1.0", "git+https://example.invalid/k.git?rev=ab#ab", None)
    dev = pkg("proptest", "1.0.0", registry, "MIT OR Apache-2.0")
    meta = {
        "workspace_members": [root["id"]],
        "packages": [root, libc, keys, dev],
        "resolve": {
            "nodes": [
                {
                    "id": root["id"],
                    "deps": [
                        {"pkg": libc["id"], "dep_kinds": [{"kind": None}]},
                        {"pkg": keys["id"], "dep_kinds": [{"kind": "build"}]},
                        {"pkg": dev["id"], "dep_kinds": [{"kind": "dev"}]},
                    ],
                },
                {"id": libc["id"], "deps": []},
                {"id": keys["id"], "deps": []},
                {"id": dev["id"], "deps": []},
            ]
        },
    }
    wheel = pkg("mizuchi-py", "0.1.0", None, "Apache-2.0")
    python_meta = {
        "workspace_members": [wheel["id"]],
        "packages": [wheel, root, libc],
        "resolve": {
            "nodes": [
                {"id": wheel["id"], "deps": [{"pkg": root["id"], "dep_kinds": [{"kind": None}]}]},
                {"id": root["id"], "deps": [{"pkg": libc["id"], "dep_kinds": [{"kind": None}]}]},
                {"id": libc["id"], "deps": []},
            ]
        },
    }
    bom = bom_from_metadata([meta, python_meta])
    by_name = {item["name"]: item for item in bom["components"]}
    assert sorted(by_name) == ["libc", "mizuchi-py", "sekai-keys"], by_name
    assert by_name["libc"]["licenses"] == [{"expression": "MIT OR Apache-2.0"}]
    assert by_name["sekai-keys"]["purl"].startswith("pkg:cargo/sekai-keys@0.1.0?vcs_url=git%2Bhttps")
    assert bom["metadata"]["component"]["type"] == "application"
    assert bom["serialNumber"] == bom_from_metadata([meta, python_meta])["serialNumber"]
    print("release-sbom self-test passed")


def main(argv: list[str]) -> None:
    if argv == ["--self-test"]:
        self_test()
    elif len(argv) == 2 and argv[0] == "--check":
        check(Path(argv[1]))
    elif len(argv) == 1 and not argv[0].startswith("-"):
        bom = bom_from_metadata([cargo_metadata(workspace) for workspace in WORKSPACES])
        Path(argv[0]).write_text(json.dumps(bom, indent=2) + "\n")
        print(argv[0])
    else:
        raise SystemExit("usage: release-sbom.py OUT | --check OUT | --self-test")


if __name__ == "__main__":
    main(sys.argv[1:])
