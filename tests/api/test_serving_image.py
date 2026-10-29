"""The serving image installs the versions the lock file resolved, and only the scoring path."""

from __future__ import annotations

import ast
import re
import tomllib

from tests.conftest import ROOT


def _locked() -> dict[str, str]:
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    return {p["name"]: p["version"] for p in lock["package"] if "version" in p}


def test_serving_pins_match_the_lock() -> None:
    locked = _locked()
    for line in (ROOT / "packages" / "api" / "requirements-serving.txt").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, version = re.split(r"==", line)
        name = re.sub(r"\[.*\]", "", name).lower()
        assert locked[name] == version, f"{name}: image pins {version}, lock has {locked[name]}"


def test_the_scoring_path_imports_nothing_heavy() -> None:
    heavy = {"pandas", "pyspark", "duckdb", "sklearn", "scipy", "pyarrow"}
    roots = {
        "friction_api": ROOT / "packages/api/src/friction_api",
        "friction_models": ROOT / "packages/models/src/friction_models",
    }
    files = [*roots["friction_api"].glob("*.py")]
    files += [roots["friction_models"] / f for f in ("floors.py", "freeman.py", "learners.py")]
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in tree.body:
            names = []
            if isinstance(node, ast.Import):
                names = [a.name.split(".")[0] for a in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module.split(".")[0]]
            assert not heavy & set(names), f"{path.name} imports {heavy & set(names)} at module level"


def test_dockerfile_copies_every_package_the_api_imports() -> None:
    text = (ROOT / "Dockerfile.vercel").read_text(encoding="utf-8")
    for pkg in ("friction_core", "friction_features", "friction_models", "friction_policy", "friction_reasons",
                "friction_api"):  # fmt: skip
        assert f"/app/src/{pkg}" in text
