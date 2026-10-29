"""Which files the text gates look at.

Authored files are everything tracked or trackable in the repository except data, third party
code, build output and the results promoted from the workspace. The identifier scan reads those.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

TEXT_SUFFIXES = {
    ".md",
    ".py",
    ".ts",
    ".tsx",
    ".js",
    ".mjs",
    ".cjs",
    ".css",
    ".json",
    ".yaml",
    ".yml",
    ".toml",
    ".sql",
    ".html",
    ".txt",
    ".ipynb",
    ".j2",
    ".cfg",
    ".ini",
    ".sh",
    ".example",
}
TEXT_NAMES = {"Makefile", "Dockerfile.render", "Dockerfile.vercel", "LICENSE", "NOTICE", ".gitignore"}
TEXT_SUFFIXES |= {".tf", ".ps1", ".hcl"}
SKIP_PARTS = {
    "node_modules",
    ".next",
    "out",
    ".git",
    ".venv",
    "__pycache__",
    "target",
    "dbt_packages",
    "dbt_internal_packages",
    "logs",
    ".cache",
    ".pytest_cache",
    ".mypy_cache",
    ".ruff_cache",
    "test-results",
    "playwright-report",
}
DATA_ROOTS = {"data", "results"}
DATA_PATHS: set[tuple[str, ...]] = set()


def repo_files(root: Path) -> list[Path]:
    try:
        tracked = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        candidates = [root / line for line in tracked]
    except (OSError, subprocess.CalledProcessError):
        candidates = [p for p in root.rglob("*") if p.is_file()]
    return sorted(p for p in candidates if p.is_file())


def is_text(path: Path) -> bool:
    return path.suffix in TEXT_SUFFIXES or path.name in TEXT_NAMES


def authored_files(root: Path, *, include_data: bool = False) -> list[Path]:
    out: list[Path] = []
    for path in repo_files(root):
        rel = path.relative_to(root)
        if any(part in SKIP_PARTS for part in rel.parts):
            continue
        if not include_data and rel.parts[0] in DATA_ROOTS:
            continue
        if tuple(rel.parts[:2]) in DATA_PATHS:
            continue
        if rel.parts[:3] == ("web", "public", "data"):
            continue
        if path.name in {"uv.lock", "package-lock.json", "LICENSE"}:
            continue
        if is_text(path):
            out.append(path)
    return out
