"""The registration gate: the evaluation plan is what was registered, and the registration is dated.

    python scripts/check_registration.py            # check
    python scripts/check_registration.py --lock     # write evaluation/plan.lock (then add the entry)

Fails when evaluation/plan.yaml hashes to something other than evaluation/plan.lock, or when no
dated DECISIONS.md entry names the locked hash. The evaluate job checks the same lock before it
scores a test month, so a plan cannot change after the fact without a dated record of it.
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
LOCK = Path("evaluation/plan.lock")


def plan_digest(root: Path) -> str:
    return sha256_json(yaml.safe_load((root / PLAN).read_text(encoding="utf-8")))


def check(root: Path) -> str:
    if not (root / LOCK).exists():
        raise ValueError("evaluation/plan.lock does not exist")
    lock = json.loads((root / LOCK).read_text(encoding="utf-8"))
    digest = plan_digest(root)
    if lock["sha256"] != digest:
        raise ValueError(f"evaluation/plan.yaml hashes to {digest[:12]}, the lock says {lock['sha256'][:12]}")
    decisions = (root / "DECISIONS.md").read_text(encoding="utf-8")
    if digest not in decisions:
        raise ValueError("no DECISIONS.md entry names the locked plan hash")
    return digest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", action="store_true")
    args = parser.parse_args()
    if args.lock:
        digest = plan_digest(ROOT)
        (ROOT / LOCK).write_text(json.dumps({"sha256": digest, "file": str(PLAN)}, indent=1) + "\n", encoding="utf-8")
        print(digest)
        return 0
    print(f"registration gate: the plan is registered as {check(ROOT)[:12]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
