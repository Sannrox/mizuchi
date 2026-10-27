#!/usr/bin/env python3
"""Generate the decision record index in docs/decisions/README.md.

Each row comes from one ADR file: the number from its filename, the title
from its first heading, the status from its `Status:` line. The table sits
between the adr-index markers; prose outside them is kept.

    python3 scripts/adr-index.py           # rewrite the index
    python3 scripts/adr-index.py --check   # exit 1 with a diff when stale
"""

import difflib
import re
import sys
from pathlib import Path

DECISIONS = Path(__file__).resolve().parent.parent / "docs" / "decisions"
README = DECISIONS / "README.md"
START = "<!-- adr-index:start -->"
END = "<!-- adr-index:end -->"


def row(path: Path) -> str:
    number = path.name[:4]
    text = path.read_text(encoding="utf-8")
    heading = re.search(r"^# (?:ADR \d+: )?(.+)$", text, re.M)
    status = re.search(r"^- Status: (.+)$", text, re.M)
    if not heading or not status:
        sys.exit(f"{path.name}: needs a '# ' heading and a '- Status:' line")
    return f"| [{number}]({path.name}) | {heading[1].strip()} | {status[1].strip()} |"


def render(readme: str) -> str:
    before, sep, rest = readme.partition(START)
    _, sep2, after = rest.partition(END)
    if not sep or not sep2:
        sys.exit(f"{README.name}: missing {START} / {END} markers")
    rows = [row(p) for p in sorted(DECISIONS.glob("[0-9][0-9][0-9][0-9]-*.md"))]
    table = ["| ADR | Title | Status |", "| --- | --- | --- |", *rows]
    return f"{before}{START}\n" + "\n".join(table) + f"\n{END}{after}"


def main() -> None:
    current = README.read_text(encoding="utf-8")
    wanted = render(current)
    if "--check" in sys.argv[1:]:
        if current != wanted:
            sys.stdout.writelines(
                difflib.unified_diff(
                    current.splitlines(keepends=True),
                    wanted.splitlines(keepends=True),
                    "docs/decisions/README.md",
                    "generated",
                )
            )
            sys.exit("decision index is stale: run python3 scripts/adr-index.py")
    elif current != wanted:
        README.write_text(wanted, encoding="utf-8")


if __name__ == "__main__":
    main()
