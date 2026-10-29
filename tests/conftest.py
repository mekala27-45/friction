"""Shared test setup. Tests never read the full source files unless marked fullsize."""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("FRICTION_ALLOW_TEST_SALT", "1")


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    has_java = shutil.which("java") is not None
    try:
        import pyspark  # noqa: F401

        has_spark = has_java
    except ImportError:
        has_spark = False
    has_full = (ROOT / "data" / "raw" / "tabformer" / "staged").exists()
    skip_spark = pytest.mark.skip(reason="needs pyspark and a JVM")
    skip_full = pytest.mark.skip(reason="needs the staged source files under data/raw")
    for item in items:
        if "spark" in item.keywords and not has_spark:
            item.add_marker(skip_spark)
        if "fullsize" in item.keywords and not has_full:
            item.add_marker(skip_full)


@pytest.fixture
def root() -> Path:
    return ROOT
