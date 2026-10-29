"""Where things live. Every path is relative to the repository root, found once.

Source files and derived tables never enter git. They live under data/raw (the two sources as
fetched), data/landing (the replay drops), data/lake (the local lakehouse) and data/work (scratch),
and FRICTION_DATA can move all four to a bigger disk.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def root() -> Path:
    env = os.environ.get("FRICTION_ROOT")
    if env:
        return Path(env).resolve()
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "data" / "sources.lock.json").exists() and (parent / "pyproject.toml").exists():
            return parent
    return Path.cwd().resolve()


def data_home() -> Path:
    env = os.environ.get("FRICTION_DATA")
    return Path(env).resolve() if env else root() / "data"


def raw_dir() -> Path:
    return data_home() / "raw"


def landing_dir() -> Path:
    return data_home() / "landing"


def lake_dir() -> Path:
    return data_home() / "lake"


def work_dir() -> Path:
    return data_home() / "work"


def config_dir() -> Path:
    return root() / "config"


def contracts_dir() -> Path:
    return root() / "contracts"


def results_dir() -> Path:
    return root() / "results"


def evaluation_dir() -> Path:
    return root() / "evaluation"
