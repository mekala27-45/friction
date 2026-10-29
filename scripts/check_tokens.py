"""Fail if anything that looks like a workspace credential is in a tracked file.

GitHub Actions is the only machine that holds the workspace host and token. This gate reads every
tracked or trackable file, data and results included, for a Databricks personal access token, an
OAuth client secret, a Postgres URL with a password, a bearer header with a value, or a
.databrickscfg block. It refuses to pass over nothing.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts._files import is_text, repo_files

PATTERNS = {
    # Databricks personal access tokens: dapi followed by 32 hex, sometimes a -N suffix.
    "databricks token": re.compile(r"\bdapi[0-9a-f]{32}(?:-\d)?\b"),
    # Databricks OAuth secrets for a service principal start with dose.
    "databricks oauth secret": re.compile(r"\bdose[0-9a-zA-Z]{32,}\b"),
    "postgres url with a password": re.compile(r"postgres(?:ql)?://[^:\s/]+:[^@\s]{6,}@[^\s]+"),
    "bearer header with a value": re.compile(r"Authorization:\s*Bearer\s+[A-Za-z0-9._\-]{20,}"),
    "databrickscfg block": re.compile(r"^\s*token\s*=\s*\S{20,}", re.M),
    "github token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
}
SKIP_PARTS = {"node_modules", ".git", ".venv", "web/.next", "web/out"}


def find_tokens(text: str) -> list[str]:
    return [name for name, pattern in PATTERNS.items() if pattern.search(text)]


def check(root: Path, files: list[Path] | None = None) -> int:
    targets = (
        files
        if files is not None
        else [
            p for p in repo_files(root) if not any(part in SKIP_PARTS for part in p.relative_to(root).parts)
        ]
    )
    targets = [
        p for p in targets if p.name != Path(__file__).name and (is_text(p) or p.suffix in {".log", ""})
    ]
    if not targets:
        raise ValueError("the token gate found no files to scan")
    problems: list[str] = []
    for path in targets:
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for name in find_tokens(text):
            problems.append(f"{path.relative_to(root)}: {name}")
    if problems:
        raise ValueError("token gate failed:\n" + "\n".join(problems[:50]))
    return len(targets)


if __name__ == "__main__":
    count = check(Path(__file__).resolve().parents[1])
    print(f"token gate: {count} files, no workspace token, secret or password in any of them")
