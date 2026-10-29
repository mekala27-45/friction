"""The statement on every surface: the documents, every model card and every page of the built site.

API response bodies are covered by tests/api/test_statement.py, which walks the route table rather
than a hand kept list.
"""

from __future__ import annotations

import html
import re
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from friction_core.statements import contains_statement

DOCUMENTS = (
    "README.md",
    "RESULTS.md",
    "report/briefing.md",
    "data/PROVENANCE.md",
    "docs/lakehouse.md",
    "docs/governance.md",
    "docs/evaluation.md",
    "docs/serving.md",
)
GLOBS = ("docs/models/*.md", "web/out/**/*.html")


def surfaces(root: Path) -> list[Path]:
    found = [root / doc for doc in DOCUMENTS if (root / doc).exists()]
    for pattern in GLOBS:
        found.extend(sorted(p for p in root.glob(pattern) if "node_modules" not in p.parts))
    return found


def _visible_text(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    if path.suffix == ".html":
        text = re.sub(r"<script.*?</script>", " ", text, flags=re.S)
        text = re.sub(r"<style.*?</style>", " ", text, flags=re.S)
        text = re.sub(r"<[^>]+>", " ", text)
        text = html.unescape(text)
    return text


def check(root: Path, files: list[Path] | None = None) -> int:
    targets = list(dict.fromkeys(files if files is not None else surfaces(root)))
    if not targets:
        raise ValueError("the statement gate found no surfaces to check")
    missing = [str(p.relative_to(root)) for p in targets if not contains_statement(_visible_text(p))]
    if missing:
        raise ValueError("these surfaces are missing the statement:\n" + "\n".join(missing))
    return len(targets)


if __name__ == "__main__":
    count = check(Path(__file__).resolve().parents[1])
    print(f"statement gate: the statement is present on {count} surfaces")
