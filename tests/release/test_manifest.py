"""manifest.py: gate flags, the listed binaries, and the tagged-run lookups
against a scratch repository and a fake gh."""

import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

from tests.release import RELEASE, fake_bin, git, init_repo, load

manifest = load("manifest")
SCRIPT = RELEASE / "manifest.py"
DIGEST = "sha256:" + "b" * 64


class GateFlags(unittest.TestCase):
    PRS = [
        {"number": 7, "labels": [{"name": "risk:breaking"}, {"name": "area:exec"}]},
        {"number": 8, "labels": [{"name": "RISK:migration"}]},
        {"number": 9, "labels": []},
    ]

    def test_maps_gate_labels_of_squash_commits(self):
        commits = [("a1", "feat!: drop x (#7)"), ("a2", "fix: y (#8)"), ("a3", "docs: z (#9)")]
        self.assertEqual(manifest.gate_flags(commits, self.PRS), ["breaking", "migration"])

    def test_a_pull_request_without_labels_fails(self):
        with self.assertRaisesRegex(SystemExit, "#10"):
            manifest.gate_flags([("a1", "fix: lost (#10)")], self.PRS)

    def test_a_commit_naming_no_pull_request_fails_and_names_the_commit(self):
        commits = [("a1", "fix: y (#8)"), ("deadbeef", "Merge branch 'x'")]
        with self.assertRaisesRegex(SystemExit, "deadbeef"):
            manifest.gate_flags(commits, self.PRS)


class Images(unittest.TestCase):
    def test_refuses_latest_a_bad_digest_and_an_embedded_digest(self):
        for bad in ("ghcr.io/x:latest=" + DIGEST, "ghcr.io/x=abc", "ghcr.io/x@sha256:1=" + DIGEST):
            with self.subTest(image=bad), self.assertRaisesRegex(SystemExit, "image must be"):
                manifest.parse_image(bad)


class BuildManifest(unittest.TestCase):
    def build(self, dist: pathlib.Path, **overrides):
        args = dict(version="0.1.0", git_sha="a" * 40, tag="v0.1.0", dist=dist, images=[], attestations=[], flags=[])
        return manifest.build_manifest(**(args | overrides))

    def test_lists_release_binaries_by_digest_and_the_image_by_registry_digest(self):
        image = manifest.parse_image(f"ghcr.io/sannrox/mizuchi:0.1.0={DIGEST}")
        with tempfile.TemporaryDirectory(prefix=f"man{os.getpid()}-") as tmp:
            dist = pathlib.Path(tmp)
            for name in ("mizuchi-0.1.0-source.tar.gz", "mizuchi-tenkai.toml", "mizuchi-0.1.0-image.tar.gz",
                         "SHA256SUMS", "mizuchi.cdx.json", "mizuchi-release.json", "RELEASE_NOTES.md"):
                (dist / name).write_bytes(b"x")
            doc = self.build(dist, images=[image])
        self.assertEqual(doc["schema"], "tenkai.family-release-manifest.v1")
        self.assertEqual([b["name"] for b in doc["binaries"]], ["mizuchi-0.1.0-source.tar.gz", "mizuchi-tenkai.toml"])
        self.assertTrue(all(b["digest"].startswith("sha256:") for b in doc["binaries"]))
        self.assertEqual(doc["images"], [image])
        self.assertNotIn("latest", json.dumps(doc))

    def test_refuses_a_short_sha_and_a_tag_for_another_version(self):
        with self.assertRaisesRegex(SystemExit, "full lowercase SHA"):
            self.build(pathlib.Path("."), git_sha="abc")
        with self.assertRaisesRegex(SystemExit, "does not match"):
            self.build(pathlib.Path("."), tag="v0.2.0")


class TaggedRun(unittest.TestCase):
    """The CLI with --tag reads commits since the previous stable tag and the merged pull requests."""

    @classmethod
    def setUpClass(cls):
        tmp = tempfile.TemporaryDirectory(prefix=f"mant{os.getpid()}-")
        cls.addClassCleanup(tmp.cleanup)
        cls.tmp = pathlib.Path(tmp.name)
        cls.repo = cls.tmp / "repo"
        init_repo(cls.repo)
        for subject, tag in (
            ("feat: old (#1)", "v0.1.0"),
            ("feat!: break (#2)", "v0.2.0-rc.1"),
            ("fix: migrate (#3)", "v0.2.0"),
        ):
            git(cls.repo, "commit", "-q", "--allow-empty", "-m", subject)
            git(cls.repo, "tag", "-a", tag, "-m", tag)
        cls.sha = git(cls.repo, "rev-parse", "HEAD")
        (cls.tmp / "dist").mkdir()

    def run_cli(self, gh: str) -> subprocess.CompletedProcess:
        env = fake_bin(self.tmp / "bin", "gh", gh)
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--version", "0.2.0", "--git-sha", self.sha, "--tag", "v0.2.0",
             "--dist", str(self.tmp / "dist"), "--out", str(self.tmp / "out.json")],
            cwd=self.repo, env=env, capture_output=True, text=True,
        )

    def test_gates_cover_every_pull_request_since_the_previous_stable_tag(self):
        prs = [
            {"number": 1, "labels": [{"name": "risk:migration"}]},  # before v0.1.0: not in range
            {"number": 2, "labels": [{"name": "risk:breaking"}]},  # shipped in the rc: in range
            {"number": 3, "labels": []},
        ]
        result = self.run_cli(f"echo '{json.dumps(prs)}'\n")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads((self.tmp / "out.json").read_text())["gate_flags"], ["breaking"])

    def test_fails_closed_when_pull_requests_cannot_be_read(self):
        (self.tmp / "out.json").unlink(missing_ok=True)
        result = self.run_cli("echo 'HTTP 403' >&2\nexit 1\n")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("HTTP 403", result.stderr)
        self.assertFalse((self.tmp / "out.json").exists())


if __name__ == "__main__":
    unittest.main()
