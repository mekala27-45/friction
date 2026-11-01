"""The registration gate: the evaluation plan is what was registered, and the registration is dated.

    python scripts/check_registration.py            # check
    python scripts/check_registration.py --lock     # write evaluation/plan.lock (then add the entry)

Fails when evaluation/plan.yaml hashes to something other than evaluation/plan.lock, or when no
dated DECISIONS.md entry names the locked hash. The evaluate job checks the same lock before it
scores a test month, so a plan cannot change after the fact without a dated record of it. An
addendum (evaluation/plan_v2.yaml and later) is locked the same way, under addenda, and the
challengers it registers are graded only when its hash matches too.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import yaml
from friction_core.hashing import sha256_json

PLAN = Path("evaluation/plan.yaml")
HYPOTHESES = Path("evaluation/hypotheses.yaml")
LOCK = Path("evaluation/plan.lock")
ADDENDA = (Path("evaluation/plan_v2.yaml"),)


def digest(root: Path, path: Path = PLAN) -> str:
    return sha256_json(yaml.safe_load((root / path).read_text(encoding="utf-8")))


def plan_digest(root: Path) -> str:
    return digest(root, PLAN)


def addendum_hash(root: Path, path: Path) -> str:
    """The locked hash of one addendum, after the whole registration checks."""
    check(root)
    lock = json.loads((root / LOCK).read_text(encoding="utf-8"))
    return str(next(a["sha256"] for a in lock.get("addenda", []) if a["file"] == str(path)))


def check(root: Path) -> str:
    if not (root / LOCK).exists():
        raise ValueError("evaluation/plan.lock does not exist")
    lock = json.loads((root / LOCK).read_text(encoding="utf-8"))
    decisions = (root / "DECISIONS.md").read_text(encoding="utf-8")
    entries = [(PLAN, lock["sha256"])]
    if "hypotheses" in lock:
        entries.append((HYPOTHESES, lock["hypotheses"]["sha256"]))
    locked_addenda = {a["file"]: a["sha256"] for a in lock.get("addenda", [])}
    for path in ADDENDA:
        if (root / path).exists() and str(path) not in locked_addenda:
            raise ValueError(f"{path} exists but evaluation/plan.lock does not lock it")
    entries += [(Path(f), h) for f, h in sorted(locked_addenda.items())]
    for path, locked in entries:
        got = digest(root, path)
        if got != locked:
            raise ValueError(f"{path} hashes to {got[:12]}, the lock says {locked[:12]}")
        if got not in decisions:
            raise ValueError(f"no DECISIONS.md entry names the locked hash of {path}")
    return str(lock["sha256"])


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", action="store_true")
    args = parser.parse_args()
    if args.lock:
        lock = json.loads((ROOT / LOCK).read_text(encoding="utf-8")) if (ROOT / LOCK).exists() else {}
        # A registered plan keeps its hash; --lock only adds what was not registered yet.
        lock.setdefault("sha256", plan_digest(ROOT))
        lock.setdefault("file", str(PLAN))
        if (ROOT / HYPOTHESES).exists():
            lock.setdefault("hypotheses", {"sha256": digest(ROOT, HYPOTHESES), "file": str(HYPOTHESES)})
        addenda = lock.setdefault("addenda", [])
        for path in ADDENDA:
            if (ROOT / path).exists() and str(path) not in {a["file"] for a in addenda}:
                addenda.append({"sha256": digest(ROOT, path), "file": str(path)})
        (ROOT / LOCK).write_text(json.dumps(lock, indent=1) + "\n", encoding="utf-8")
        print(json.dumps(lock))
        return 0
    print(f"registration gate: the plan is registered as {check(ROOT)[:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
