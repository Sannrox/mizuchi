"""verify-tag.sh against real annotated, signed, and lightweight tags in a
scratch repository whose origin is a local bare repository."""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

from tests.release import RELEASE, git, init_repo

SCRIPT = RELEASE / "verify-tag.sh"
UNKNOWN_KEY = "AEDA618886BF0331105B9CFCE4DC1F4A579505FB"


@unittest.skipUnless(shutil.which("gpg"), "gpg is required")
class VerifyTag(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Short base path: gpg-agent socket paths have a length limit.
        cls.tmp = pathlib.Path(tempfile.mkdtemp(prefix=f"vt{os.getpid()}-", dir="/tmp"))
        gnupg = cls.tmp / "g"
        gnupg.mkdir(mode=0o700)
        (gnupg / "gpg.conf").write_text("pinentry-mode loopback\n")
        (gnupg / "gpg-agent.conf").write_text("allow-loopback-pinentry\n")
        cls.env = {**os.environ, "GNUPGHOME": str(gnupg)}
        gpg = lambda *a: subprocess.run(["gpg", "--batch", *a], env=cls.env, check=True, capture_output=True)
        gpg("--pinentry-mode", "loopback", "--passphrase", "", "--quick-generate-key",
            "Release test <release-test@example.invalid>", "ed25519", "default", "never")
        listing = gpg("--list-secret-keys", "--with-colons").stdout.decode()
        cls.fpr = next(line.split(":")[9] for line in listing.splitlines() if line.startswith("fpr:"))
        key = cls.tmp / "key.asc"
        key.write_bytes(gpg("--armor", "--export", cls.fpr).stdout)
        cls.fingerprints = cls.tmp / "fingerprints"
        cls.work = cls.tmp / "w"
        cls.env.update(
            RELEASE_TAG_KEY=str(key),
            RELEASE_TAG_FINGERPRINTS=str(cls.fingerprints),
            RELEASE_ROOT=str(cls.work),
        )
        g = lambda *a: git(cls.work, *a, env=cls.env)
        init_repo(cls.work, env=cls.env)
        g("config", "user.signingkey", cls.fpr)
        g("config", "gpg.program", shutil.which("gpg"))
        cargo = cls.work / "Cargo.toml"
        cargo.write_text('[workspace.package]\nversion = "0.1.0"\n')
        g("add", "Cargo.toml")
        g("commit", "-q", "-m", "first")
        cls.first = g("rev-parse", "HEAD")
        g("tag", "v0.0.9", cls.first)
        g("tag", "-a", "v0.0.8", "-m", "unsigned", cls.first)
        g("tag", "-s", "v0.1.0", "-m", "signed", cls.first)
        g("tag", "-s", "v0.2.0", "-m", "wrong version", cls.first)
        # A signed tag object pushed under another ref name keeps its own name.
        g("tag", "-s", "v0.1.1-rc.1", "-m", "renamed", cls.first)
        g("update-ref", "refs/tags/v0.1.1", g("rev-parse", "v0.1.1-rc.1"))
        cargo.write_text('[workspace.package]\nversion = "0.2.0-rc.1"\n')
        g("commit", "-q", "-am", "rc")
        cls.rc = g("rev-parse", "HEAD")
        g("tag", "-s", "v0.2.0-rc.1", "-m", "rc", cls.rc)
        g("tag", "-s", "v0.2.0-beta.x", "-m", "bad suffix", cls.rc)
        # A signed tag on a commit that never reached main.
        g("checkout", "-q", "-b", "side")
        cargo.write_text('[workspace.package]\nversion = "0.3.0"\n')
        g("commit", "-q", "-am", "side")
        cls.side = g("rev-parse", "HEAD")
        g("tag", "-s", "v0.3.0", "-m", "off main", cls.side)
        origin = cls.tmp / "o.git"
        subprocess.run(["git", "init", "-q", "--bare", str(origin)], env=cls.env, check=True)
        g("remote", "add", "origin", str(origin))
        g("push", "-q", "origin", "main", "--tags")
        g("checkout", "-q", cls.first)

    @classmethod
    def tearDownClass(cls):
        subprocess.run(["gpgconf", "--kill", "gpg-agent"], env=cls.env, capture_output=True)
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def setUp(self):
        self.fingerprints.write_text(self.fpr + "\n")
        git(self.work, "checkout", "-q", self.first, env=self.env)

    def run_script(self, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(["bash", str(SCRIPT), *args], cwd=self.work, env=self.env, capture_output=True, text=True)

    def assertRefused(self, message: str, *args: str):
        result = self.run_script(*args)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn(message, result.stderr)

    def test_accepts_a_signed_tag_matching_the_workspace_version(self):
        result = self.run_script("verify", "v0.1.0", self.first)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_refuses_lightweight_unsigned_and_renamed_tags(self):
        for tag, message in (
            ("v0.0.9", "is lightweight"),
            ("v0.0.8", "unsigned"),
            ("v0.1.1", "tag object named v0.1.1-rc.1"),
        ):
            with self.subTest(tag=tag):
                self.assertRefused(message, "verify", tag, self.first)

    def test_refuses_an_unknown_key(self):
        self.fingerprints.write_text(UNKNOWN_KEY + "\n")
        self.assertRefused("unknown key", "verify", "v0.1.0", self.first)

    def test_refuses_a_tag_that_does_not_match_the_workspace_version(self):
        self.assertRefused("does not match workspace version 0.1.0", "verify", "v0.2.0", self.first)

    def test_release_candidate_tag_matches_an_rc_version_only(self):
        git(self.work, "checkout", "-q", self.rc, env=self.env)
        self.assertEqual(self.run_script("verify", "v0.2.0-rc.1", self.rc).returncode, 0)
        self.assertRefused("invalid release tag", "verify", "v0.2.0-beta.x", self.rc)

    def test_refuses_a_tag_for_another_commit(self):
        self.assertRefused(f"resolves to {self.first}, expected {self.rc}", "verify", "v0.1.0", self.rc)

    def test_release_accepts_a_signed_tag_on_main_at_head(self):
        result = self.run_script("release", "v0.1.0")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_release_refuses_a_tag_off_main(self):
        git(self.work, "checkout", "-q", self.side, env=self.env)
        self.assertRefused("is not on main", "release", "v0.3.0")

    def test_release_refuses_a_checkout_at_another_commit(self):
        git(self.work, "checkout", "-q", self.rc, env=self.env)
        self.assertRefused("checkout is not at tag v0.1.0", "release", "v0.1.0")


if __name__ == "__main__":
    unittest.main()
