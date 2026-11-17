#!/usr/bin/env python3
"""Print the CHANGELOG.md section for one version: the GitHub Release body.

    scripts/release/notes.py VERSION   # heading plus body, from CHANGELOG.md
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


def main(argv: list[str]) -> None:
    if len(argv) == 1:
        sys.stdout.write(notes_for(pathlib.Path("CHANGELOG.md").read_text(), argv[0]))
    else:
        raise SystemExit("usage: notes.py VERSION")


if __name__ == "__main__":
    main(sys.argv[1:])
