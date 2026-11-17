"""sbom.py: which packages ship, and the schema check of the SBOM built from
this workspace's lockfiles (needs cargo and cyclonedx-python-lib[json-validation])."""

import contextlib
import io
import os
import pathlib
import tempfile
import unittest

from tests.release import load

sbom = load("sbom")
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"


def pkg(name: str, version: str, source: str | None, license: str | None) -> dict:
    pid = f"{source or 'path+file:///w'}#{name}@{version}"
    return {"id": pid, "name": name, "version": version, "source": source, "license": license}


def node(package: dict, *deps: tuple[dict, str | None]) -> dict:
    return {"id": package["id"], "deps": [{"pkg": d["id"], "dep_kinds": [{"kind": kind}]} for d, kind in deps]}


class BomFromMetadata(unittest.TestCase):
    def setUp(self):
        root = pkg("mizuchi", "0.1.0", None, "Apache-2.0")
        libc = pkg("libc", "0.2.0", REGISTRY, "MIT/Apache-2.0")
        keys = pkg("sekai-keys", "0.1.0", "git+https://example.invalid/k.git?rev=ab#ab", None)
        dev = pkg("proptest", "1.0.0", REGISTRY, "MIT OR Apache-2.0")
        wheel = pkg("mizuchi-py", "0.1.0", None, "Apache-2.0")
        workspace = {
            "workspace_members": [root["id"]],
            "packages": [root, libc, keys, dev],
            "resolve": {"nodes": [
                node(root, (libc, None), (keys, "build"), (dev, "dev")), node(libc), node(keys), node(dev),
            ]},
        }
        # python/ reaches the facade as a path dependency.
        python = {
            "workspace_members": [wheel["id"]],
            "packages": [wheel, root, libc],
            "resolve": {"nodes": [node(wheel, (root, None)), node(root, (libc, None)), node(libc)]},
        }
        self.metas = [workspace, python]

    def test_lists_shipped_dependencies_of_both_workspaces_without_dev_only_ones(self):
        bom = sbom.bom_from_metadata(self.metas)
        by_name = {item["name"]: item for item in bom["components"]}
        self.assertEqual(sorted(by_name), ["libc", "mizuchi-py", "sekai-keys"])
        self.assertEqual(bom["metadata"]["component"]["type"], "application")
        self.assertEqual(bom["metadata"]["component"]["name"], "mizuchi")

    def test_legacy_license_becomes_an_spdx_expression(self):
        [libc] = [c for c in sbom.bom_from_metadata(self.metas)["components"] if c["name"] == "libc"]
        self.assertEqual(libc["licenses"], [{"expression": "MIT OR Apache-2.0"}])

    def test_git_pin_names_its_source(self):
        [keys] = [c for c in sbom.bom_from_metadata(self.metas)["components"] if c["name"] == "sekai-keys"]
        self.assertTrue(keys["purl"].startswith("pkg:cargo/sekai-keys@0.1.0?vcs_url=git%2Bhttps"))

    def test_serial_number_is_stable_for_the_same_lockfile(self):
        self.assertEqual(
            sbom.bom_from_metadata(self.metas)["serialNumber"], sbom.bom_from_metadata(self.metas)["serialNumber"]
        )


class WorkspaceSbom(unittest.TestCase):
    def test_sbom_of_this_workspace_is_valid_cyclonedx(self):
        with tempfile.TemporaryDirectory(prefix=f"sbom{os.getpid()}-") as tmp:
            out = pathlib.Path(tmp) / "mizuchi.cdx.json"
            with contextlib.redirect_stdout(io.StringIO()):
                sbom.main([str(out)])
                sbom.main(["--check", str(out)])


if __name__ == "__main__":
    unittest.main()
