"""image.sh against a fake docker that records every call."""

import os
import pathlib
import subprocess
import tempfile
import unittest

from tests.release import RELEASE, fake_bin

SCRIPT = RELEASE / "image.sh"
DIGEST = "sha256:" + "c" * 64

# Logs every call to $FAKE_LOG; `image inspect` answers $FAKE_REPO_DIGEST.
FAKE_DOCKER = """printf '%s\\n' "$*" >>"$FAKE_LOG"
case "$1" in
  image) echo "$FAKE_REPO_DIGEST" ;;
  save) echo image-bytes ;;
esac
"""


class Image(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix=f"img{os.getpid()}-")
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)
        self.log = self.tmp / "docker.log"
        self.log.touch()
        self.env = {**fake_bin(self.tmp / "bin", "docker", FAKE_DOCKER), "FAKE_LOG": str(self.log)}

    def run_script(self, tag: str, repo_digest: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(SCRIPT), tag, str(self.tmp / "dist")],
            env={**self.env, "FAKE_REPO_DIGEST": repo_digest}, capture_output=True, text=True,
        )

    def test_pushes_only_the_version_tag_and_prints_the_registry_digest(self):
        result = self.run_script("v1.2.0-rc.1", f"ghcr.io/sannrox/mizuchi@{DIGEST}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, f"digest={DIGEST}\n")
        calls = self.log.read_text().splitlines()
        self.assertIn("push ghcr.io/sannrox/mizuchi:1.2.0-rc.1", calls)
        self.assertNotIn("latest", "\n".join(calls))
        self.assertTrue((self.tmp / "dist" / "mizuchi-1.2.0-rc.1-image.tar.gz").stat().st_size > 0)

    def test_fails_without_a_registry_digest(self):
        result = self.run_script("v1.2.0", "")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no registry digest", result.stderr)

    def test_refuses_an_invalid_tag_before_building(self):
        self.assertNotEqual(self.run_script("latest", f"x@{DIGEST}").returncode, 0)
        self.assertEqual(self.log.read_text(), "")


if __name__ == "__main__":
    unittest.main()
