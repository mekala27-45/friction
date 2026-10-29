"""Fail if any authored file contains an em dash or an en dash.

Ranges are written with "to", asides with commas or parentheses. The gate
refuses to report a pass when it found nothing to scan.
"""

from __future__ import annotations

import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._files import authored_files

BANNED = {chr(0x2014): "em dash", chr(0x2013): "en dash", chr(0x2015): "horizontal bar"}


def find_dashes(text: str) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        for char, name in BANNED.items():
            if char in line:
                hits.append((number, name))
    return hits


def check(root: Path, files: list[Path] | None = None) -> int:
    targets = files if files is not None else authored_files(root)
    if not targets:
        raise ValueError("the dash gate found no files to scan")
    problems: list[str] = []
    for path in targets:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for line, name in find_dashes(text):
            problems.append(f"{path.relative_to(root)}:{line}: {name}")
    if problems:
        raise ValueError("dash gate failed:\n" + "\n".join(problems[:50]))
    return len(targets)


if __name__ == "__main__":
    count = check(Path(__file__).resolve().parents[1])
    print(f"dash gate: {count} files, none contain an em or en dash")
