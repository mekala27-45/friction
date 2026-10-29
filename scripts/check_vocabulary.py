"""Keep the authored prose plain. Phrases on this list read as filler in an operations review."""

from __future__ import annotations

import re
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._files import authored_files

# Split so the gate does not trip over its own list.
PHRASES = (
    "lever" + "age",
    "del" + "ve",
    "seam" + "less",
    "cutting" + " edge",
    "cutting" + "-edge",
    "comprehen" + "sive",
    "state" + " of the art",
    "state" + "-of-the-art",
    "empo" + "wer",
    "unlo" + "ck",
    "ele" + "vate",
    "stream" + "line",
    "holi" + "stic",
    "syner" + "gy",
    "game" + " changing",
    "game" + "-changing",
    "next" + " generation",
    "best" + " in class",
    "util" + "ize",
    "facil" + "itate",
    "in" + " today's",
    "it is" + " worth noting",
    "tape" + "stry",
    "testa" + "ment to",
    "revolu" + "tionize",
    "super" + "charge",
    "world" + " class",
    "world" + "-class",
    "embark" + " on",
    "ever" + "-evolving",
    "fast" + "-paced",
    "rob" + "ust and scalable",
)


def _stem(phrase: str) -> str:
    escaped = re.escape(phrase)
    if phrase.endswith("e"):
        return escaped[:-1] + "(?:e|es|ed|ing)"
    return escaped + "(?:s|ed|ing)?"


PATTERN = re.compile(r"\b(?:" + "|".join(_stem(p) for p in PHRASES) + r")\b", re.I)


def find_phrases(text: str) -> list[str]:
    return [m.group(0) for m in PATTERN.finditer(text)]


def check(root: Path, files: list[Path] | None = None) -> int:
    targets = files if files is not None else authored_files(root)
    targets = [p for p in targets if p.name != Path(__file__).name]
    if not targets:
        raise ValueError("the vocabulary gate found no files to scan")
    problems: list[str] = []
    for path in targets:
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        found = find_phrases(text)
        if found:
            problems.append(f"{path.relative_to(root)}: {sorted(set(found))}")
    if problems:
        raise ValueError("vocabulary gate failed:\n" + "\n".join(problems[:50]))
    return len(targets)


if __name__ == "__main__":
    count = check(Path(__file__).resolve().parents[1])
    print(f"vocabulary gate: {count} files clean")
