"""changelog-draft.sh in a scratch clone whose origin is a local bare
repository, with a fake gh that reports no labelled pull requests."""

import os
import pathlib
import shutil
import subprocess
import tempfile
import unittest

from tests.release import ROOT, fake_bin, git, init_repo

SCRIPT = ROOT / "scripts" / "changelog-draft.sh"


class ChangelogDraft(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        tmp = tempfile.TemporaryDirectory(prefix=f"cld{os.getpid()}-")
        cls.addClassCleanup(tmp.cleanup)
        cls.tmp = pathlib.Path(tmp.name)
        origin, cls.repo = cls.tmp / "origin.git", cls.tmp / "repo"
        subprocess.run(["git", "init", "-q", "--bare", "-b", "main", str(origin)], check=True)
        init_repo(cls.repo)
        (cls.repo / "scripts").mkdir()
        shutil.copy(SCRIPT, cls.repo / "scripts")
        git(cls.repo, "add", ".")
        git(cls.repo, "commit", "-q", "-m", "chore: start (#1)")
        git(cls.repo, "commit", "-q", "--allow-empty", "-m", "feat: add x (#2)")
        git(cls.repo, "remote", "add", "origin", str(origin))
        git(cls.repo, "push", "-q", "origin", "main")
        cls.env = fake_bin(cls.tmp / "bin", "gh", "exit 0\n")

    def draft(self, since: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["bash", str(self.repo / "scripts" / "changelog-draft.sh"), "1.0.0", since],
            env=self.env, capture_output=True, text=True,
        )

    def test_drafts_sections_from_the_commits_in_range(self):
        result = self.draft(git(self.repo, "rev-parse", "HEAD~1"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("### Added\n\n- add x (#2)", result.stdout)

    def test_an_unreadable_range_fails_and_names_it(self):
        result = self.draft("v9.9.9")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("v9.9.9..origin/main", result.stderr)


if __name__ == "__main__":
    unittest.main()
