"""Compare the two engines over the same drops: the workspace export against the open source run.

    python scripts/reconcile.py --workspace <export.json> --local <summary.json>

Both sides ran with the workspace salt (the open source side on the Actions runner, request kind
portable), so salted tables are compared by digest, exactly. The reconcile statistics are compared
at the desk, month, day and user bucket grains: counts and sums of integers exactly, sums of
doubles to a relative 1e-9, since the engines add in different orders. Exits non zero on any drift.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
REL = 1e-9


def _same(a: Any, b: Any) -> bool:
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_same(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_same(x, y) for x, y in zip(a, b, strict=True))
    if isinstance(a, float) or isinstance(b, float):
        if a is None or b is None:
            return a is b
        return math.isclose(float(a), float(b), rel_tol=REL, abs_tol=1e-12)
    return bool(a == b)


def _first_difference(a: Any, b: Any, path: str = "") -> str | None:
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a or k not in b:
                return f"{path}/{k}: present on one side only"
            found = _first_difference(a[k], b[k], f"{path}/{k}")
            if found:
                return found
        return None
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return f"{path}: {len(a)} rows against {len(b)}"
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            found = _first_difference(x, y, f"{path}[{i}]")
            if found:
                return found
        return None
    return None if _same(a, b) else f"{path}: {a!r} against {b!r}"


def compare(workspace: dict[str, Any], local: dict[str, Any]) -> dict[str, Any]:
    tables = {}
    for name, w in sorted(workspace["digests"].items()):
        mine = local["tables"].get(name)
        tables[name] = {
            "rows_workspace": w["rows"],
            "rows_local": None if mine is None else mine["rows"],
            "digest_equal": mine is not None and mine["digest"] == w["digest"],
        }
    stats = {}
    for desk in ("payments", "logins"):
        for grain in ("desk", "month", "day", "user_bucket"):
            a, b = workspace["reconcile"][desk][grain], local["reconcile"][desk][grain]
            stats[f"{desk}/{grain}"] = {"equal": _same(a, b), "first_difference": _first_difference(a, b)}
    for part in ("feature_sums", "histories", "quarantine"):
        a, b = workspace["reconcile"].get(part), local["reconcile"].get(part)
        stats[part] = {"equal": _same(a, b), "first_difference": _first_difference(a, b)}
    drift = not all(t["digest_equal"] for t in tables.values()) or not all(s["equal"] for s in stats.values())
    return {"tables": tables, "statistics": stats, "drift": drift}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--local", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "reconcile_first_month.json")
    args = parser.parse_args()
    local = json.loads(args.local.read_text())
    # summarize_lake.py files each engine's summary under its engine name.
    local = local.get("local", local) if "tables" not in local else local
    result = compare(json.loads(args.workspace.read_text()), local)
    result["inputs"] = {"workspace": str(args.workspace), "local": str(args.local)}
    args.out.write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    equal = sum(t["digest_equal"] for t in result["tables"].values())
    print(f"{equal} of {len(result['tables'])} tables equal by digest; drift: {result['drift']}")
    for key, s in result["statistics"].items():
        if not s["equal"]:
            print(f"  {key}: {s['first_difference']}")
    return 1 if result["drift"] else 0


if __name__ == "__main__":
    sys.exit(main())
