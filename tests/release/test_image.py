"""image.sh against a fake docker that records every call and keeps a registry."""

import os
import pathlib
import subprocess
import tempfile
import unittest

from tests.release import RELEASE, fake_bin

SCRIPT = RELEASE / "image.sh"
DIGEST = "sha256:" + "c" * 64
OTHER = "sha256:" + "d" * 64
FINAL = "ghcr.io/sannrox/mizuchi:1.2.0"

# Logs every call to $FAKE_LOG. `image inspect` answers $FAKE_REPO_DIGEST.
# The registry is $FAKE_TAGS, one "<ref> <digest>" line per tag: `imagetools
# inspect` answers the last line for a ref, or "not found" (or
# $FAKE_INSPECT_ERROR when set); `imagetools create` appends the source digest
# (or $FAKE_CREATE_DIGEST when set) for the tag.
FAKE_DOCKER = r"""printf '%s\n' "$*" >>"$FAKE_LOG"
case "$1" in
  image) echo "$FAKE_REPO_DIGEST" ;;
  save) echo image-bytes ;;
  buildx)
    case "$3" in
      inspect)
        ref="${*: -1}"
        if [ -n "${FAKE_INSPECT_ERROR:-}" ]; then echo "$FAKE_INSPECT_ERROR" >&2; exit 1; fi
        line="$(grep -F "$ref " "$FAKE_TAGS" | tail -n 1)"
        if [ -n "$line" ]; then echo "${line#* }"; else echo "ERROR: $ref: not found" >&2; exit 1; fi ;;
      create)
        src="${*: -1}"
        echo "$5 ${FAKE_CREATE_DIGEST:-${src#*@}}" >>"$FAKE_TAGS" ;;
    esac ;;
esac
"""


class Image(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix=f"img{os.getpid()}-")
        self.addCleanup(tmp.cleanup)
        self.tmp = pathlib.Path(tmp.name)
        self.log = self.tmp / "docker.log"
        self.log.touch()
        self.tags = self.tmp / "tags"
        self.tags.touch()
        self.env = {
            **fake_bin(self.tmp / "bin", "docker", FAKE_DOCKER),
            "FAKE_LOG": str(self.log),
            "FAKE_TAGS": str(self.tags),
        }

    def run_script(self, *args: str, **env: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(SCRIPT), *args], env={**self.env, **env}, capture_output=True, text=True
        )

    def push(self, tag: str, repo_digest: str) -> subprocess.CompletedProcess:
        return self.run_script("push", tag, str(self.tmp / "dist"), FAKE_REPO_DIGEST=repo_digest)

    def calls(self) -> list[str]:
        return self.log.read_text().splitlines()

    def creates(self) -> list[str]:
        return [c for c in self.calls() if c.startswith("buildx imagetools create")]

    def test_push_pushes_only_the_staging_tag_and_prints_the_registry_digest(self):
        result = self.push("v1.2.0-rc.1", f"ghcr.io/sannrox/mizuchi@{DIGEST}")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, f"digest={DIGEST}\n")
        pushes = [c for c in self.calls() if c.startswith("push ")]
        self.assertEqual(pushes, ["push ghcr.io/sannrox/mizuchi:staging-1.2.0-rc.1"])
        self.assertNotIn("latest", "\n".join(self.calls()))
        self.assertTrue((self.tmp / "dist" / "mizuchi-1.2.0-rc.1-image.tar.gz").stat().st_size > 0)

    def test_push_fails_without_a_registry_digest(self):
        result = self.push("v1.2.0", "")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("no registry digest", result.stderr)

    def test_refuses_an_invalid_tag_or_command_before_touching_docker(self):
        self.assertNotEqual(self.push("latest", f"x@{DIGEST}").returncode, 0)
        self.assertNotEqual(self.run_script("tag", "v1.2.0", DIGEST).returncode, 0)
        self.assertNotEqual(self.run_script("promote", "v1.2.0", "sha256:short").returncode, 0)
        self.assertEqual(self.log.read_text(), "")

    def test_promote_points_a_new_version_tag_at_the_digest(self):
        result = self.run_script("promote", "v1.2.0", DIGEST)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.creates(), [f"buildx imagetools create --tag {FINAL} ghcr.io/sannrox/mizuchi@{DIGEST}"])
        self.assertEqual(self.tags.read_text(), f"{FINAL} {DIGEST}\n")

    def test_promote_is_a_no_op_when_the_tag_already_names_the_digest(self):
        self.tags.write_text(f"{FINAL} {DIGEST}\n")
        result = self.run_script("promote", "v1.2.0", DIGEST)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.creates(), [])

    def test_promote_refuses_to_move_a_tag_that_names_another_digest(self):
        self.tags.write_text(f"{FINAL} {OTHER}\n")
        result = self.run_script("promote", "v1.2.0", DIGEST)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(f"already names {OTHER}", result.stderr)
        self.assertEqual(self.creates(), [])
        self.assertEqual(self.tags.read_text(), f"{FINAL} {OTHER}\n")

    def test_promote_fails_when_the_lookup_fails_for_another_reason(self):
        result = self.run_script("promote", "v1.2.0", DIGEST, FAKE_INSPECT_ERROR="ERROR: unauthorized")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unauthorized", result.stderr)
        self.assertEqual(self.creates(), [])

    def test_promote_fails_when_the_tag_does_not_name_the_digest_afterwards(self):
        result = self.run_script("promote", "v1.2.0", DIGEST, FAKE_CREATE_DIGEST=OTHER)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("after tagging", result.stderr)


if __name__ == "__main__":
    unittest.main()
