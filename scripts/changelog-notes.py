#!/usr/bin/env python3
"""Print the CHANGELOG.md section for one version: the GitHub Release body.

    scripts/changelog-notes.py VERSION   # heading plus body, from CHANGELOG.md
    scripts/changelog-notes.py --self-test
"""

from __future__ import annotations

import pathlib
import re
import sys


def notes_for(text: str, version: str) -> str:
    """Return `## [version] ...` through the line before the next `## ` heading."""
    headings = list(re.finditer(r"^## .*$", text, re.M))
    for i, heading in enumerate(headings):
        if heading.group(0).startswith(f"## [{version}]"):
            end = headings[i + 1].start() if i + 1 < len(headings) else len(text)
            body = text[heading.end() : end].strip()
            if not body:
                raise SystemExit(f"CHANGELOG.md section for {version} is empty")
            return f"{heading.group(0)}\n\n{body}\n"
    raise SystemExit(f"CHANGELOG.md has no ## [{version}] section")


def self_test() -> None:
    sample = (
        "# Changelog\n\n## Unreleased\n\n- pending\n\n"
        "## [0.2.0] - 2026-11-01\n\n### Added\n\n- manifest\n\n"
        "## [0.1.0] - 2026-10-30\n\nFirst release.\n"
    )
    notes = notes_for(sample, "0.2.0")
    assert notes.startswith("## [0.2.0] - 2026-11-01\n"), notes
    assert "- manifest" in notes and "First release" not in notes, notes
    assert notes_for(sample, "0.1.0").endswith("First release.\n")
    for text, version, message in [
        (sample, "0.2", "no ## [0.2] section"),
        ("## [0.3.0]\n\n## [0.2.0]\n\nx\n", "0.3.0", "is empty"),
    ]:
        try:
            notes_for(text, version)
        except SystemExit as exc:
            assert message in str(exc), exc
        else:
            raise AssertionError(f"expected failure: {message}")
    print("changelog-notes self-test passed")


def main(argv: list[str]) -> None:
    if argv == ["--self-test"]:
        self_test()
    elif len(argv) == 1:
        sys.stdout.write(notes_for(pathlib.Path("CHANGELOG.md").read_text(), argv[0]))
    else:
        raise SystemExit("usage: changelog-notes.py VERSION | --self-test")


if __name__ == "__main__":
    main(sys.argv[1:])
