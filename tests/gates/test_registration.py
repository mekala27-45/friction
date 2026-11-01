"""The registration gate: the plan, its hypotheses and every addendum hash to what the lock holds."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from scripts import check_registration as reg

ROOT = Path(__file__).resolve().parents[2]


def _copy(tmp: Path) -> Path:
    (tmp / "evaluation").mkdir()
    for name in ("plan.yaml", "hypotheses.yaml", "plan_v2.yaml", "plan.lock"):
        shutil.copy(ROOT / "evaluation" / name, tmp / "evaluation" / name)
    shutil.copy(ROOT / "DECISIONS.md", tmp / "DECISIONS.md")
    return tmp


def test_the_repository_is_registered() -> None:
    assert reg.check(ROOT) == json.loads((ROOT / "evaluation" / "plan.lock").read_text())["sha256"]
    assert reg.addendum_hash(ROOT, Path("evaluation/plan_v2.yaml")) in (ROOT / "DECISIONS.md").read_text()


def test_an_edited_addendum_fails(tmp_path: Path) -> None:
    root = _copy(tmp_path)
    path = root / "evaluation" / "plan_v2.yaml"
    path.write_text(path.read_text() + "\nlate: true\n")
    with pytest.raises(ValueError, match=r"plan_v2\.yaml hashes to"):
        reg.check(root)


def test_an_addendum_the_lock_does_not_hold_fails(tmp_path: Path) -> None:
    root = _copy(tmp_path)
    lock = json.loads((root / "evaluation" / "plan.lock").read_text())
    lock.pop("addenda")
    (root / "evaluation" / "plan.lock").write_text(json.dumps(lock))
    with pytest.raises(ValueError, match="does not lock it"):
        reg.check(root)


def test_a_locked_addendum_with_no_dated_entry_fails(tmp_path: Path) -> None:
    root = _copy(tmp_path)
    locked = reg.addendum_hash(root, Path("evaluation/plan_v2.yaml"))
    text = (root / "DECISIONS.md").read_text().replace(locked, "removed")
    (root / "DECISIONS.md").write_text(text)
    with pytest.raises(ValueError, match=r"no DECISIONS\.md entry"):
        reg.check(root)
