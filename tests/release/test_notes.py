import tomllib
import unittest

from tests.release import ROOT, load

notes = load("notes")

CHANGELOG = (
    "# Changelog\n\n## Unreleased\n\n- pending\n\n"
    "## [0.3.0-rc.1] - 2026-11-04\n\n- candidate\n\n"
    "## [0.2.0] - 2026-11-01\n\n### Added\n\n- manifest\n\n"
    "## [0.1.0] - 2026-10-30\n\nFirst release.\n"
)


class NotesFor(unittest.TestCase):
    def test_returns_the_heading_and_only_that_section(self):
        body = notes.notes_for(CHANGELOG, "0.2.0")
        self.assertTrue(body.startswith("## [0.2.0] - 2026-11-01\n"))
        self.assertIn("- manifest", body)
        self.assertNotIn("First release", body)

    def test_last_section_runs_to_the_end(self):
        self.assertTrue(notes.notes_for(CHANGELOG, "0.1.0").endswith("First release.\n"))

    def test_missing_or_empty_section_fails(self):
        with self.assertRaisesRegex(SystemExit, r"no ## \[0.2\] section"):
            notes.notes_for(CHANGELOG, "0.2")
        with self.assertRaisesRegex(SystemExit, "is empty"):
            notes.notes_for("## [0.3.0]\n\n## [0.2.0]\n\nx\n", "0.3.0")

    def test_final_release_never_reuses_its_release_candidate_notes(self):
        self.assertIn("- candidate", notes.notes_for(CHANGELOG, "0.3.0-rc.1"))
        with self.assertRaisesRegex(SystemExit, r"no ## \[0.3.0\] section"):
            notes.notes_for(CHANGELOG, "0.3.0")

    def test_changelog_has_notes_for_the_workspace_version(self):
        version = tomllib.loads((ROOT / "Cargo.toml").read_text())["workspace"]["package"]["version"]
        self.assertTrue(notes.notes_for((ROOT / "CHANGELOG.md").read_text(), version).strip())


if __name__ == "__main__":
    unittest.main()
