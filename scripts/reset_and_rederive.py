"""Reset and rederive in a clean worktree, and compare with what main holds.

    python scripts/reset_and_rederive.py

1. A fresh git worktree of HEAD under data/work/rederive, with nothing from the build machine but
   the lock files and the staged sources.
2. The replay from the lock: scripts/replay.py over the staged sources must reproduce
   data/replay.lock.json byte for byte.
3. The fixture slice through the portable pipeline: its table digests must equal the fixture
   summary in results/reconcile.json.
4. The evaluation from the scored sets: scripts/grade.py must reproduce results/evaluation.json's digest.
   The full scored sets live only on the Actions runner, so the rerun there (a portable request with
   models) is read back with --evaluation and --model: its grade and its served model file must
   equal main's.
5. One landed month rerun in the workspace (--month-workspace, the integration job's month test)
   against the open source engine's run of the same month (--month-open-source): every table equal
   by digest, and the row counts equal to the first month published in results/reconcile_first_month.json.

Writes results/rederive.json and exits non zero on any drift.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


TOLERANCE = 1e-9


def differences(a: Any, b: Any, path: str = "") -> tuple[float, list[str]]:
    """The largest relative difference between two graded files' numbers, and every other difference.
    Wall seconds and the digest are left out: they are not graded numbers."""
    if isinstance(a, dict) and isinstance(b, dict):
        worst, other = 0.0, []
        for k in sorted(set(a) | set(b)):
            if not path and k in ("seconds", "digest"):
                continue
            w, o = differences(a.get(k), b.get(k), f"{path}/{k}")
            worst, other = max(worst, w), other + o
        return worst, other
    if isinstance(a, list) and isinstance(b, list) and len(a) == len(b):
        worst, other = 0.0, []
        for i, (x, y) in enumerate(zip(a, b, strict=True)):
            w, o = differences(x, y, f"{path}[{i}]")
            worst, other = max(worst, w), other + o
        return worst, other
    if isinstance(a, float) and isinstance(b, float):
        return abs(a - b) / max(abs(a), abs(b), 1e-300), []
    return (0.0, []) if a == b else (0.0, [path])


def sh(*cmd: str, cwd: Path, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(cmd), cwd=cwd, capture_output=True, text=True, env={**os.environ, **(env or {})})


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scored", type=Path, default=ROOT / "data" / "work" / "models" / "full" / "scored")
    parser.add_argument("--skip-replay", action="store_true")
    parser.add_argument(
        "--evaluation",
        type=Path,
        action="append",
        default=[],
        help="the evaluation.json of a runner's rerun; may be given more than once",
    )
    parser.add_argument("--model", type=Path, help="the served payments model.txt of the runner's rerun")
    parser.add_argument("--month-workspace", type=Path, help="integration.json of the workspace month rerun")
    parser.add_argument("--month-open-source", type=Path, help="summary.json of the open source month rerun")
    args = parser.parse_args()
    started = time.monotonic()
    tree = ROOT / "data" / "work" / "rederive"
    if tree.exists():
        sh("git", "worktree", "remove", "--force", str(tree), cwd=ROOT)
        shutil.rmtree(tree, ignore_errors=True)
    head = sh("git", "rev-parse", "HEAD", cwd=ROOT).stdout.strip()  # the commit the worktree is made from
    r = sh("git", "worktree", "add", "--detach", str(tree), head, cwd=ROOT)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    (tree / "data" / "raw").mkdir(parents=True, exist_ok=True)
    for src in ("tabformer", "rba"):
        (tree / "data" / "raw" / src).symlink_to(ROOT / "data" / "raw" / src, target_is_directory=True)
    env = {"FRICTION_ALLOW_TEST_SALT": "1", "PYTHONHASHSEED": "0", "UV_PROJECT_ENVIRONMENT": str(ROOT / ".venv")}
    checks: dict[str, object] = {}
    if not args.skip_replay:
        r = sh("uv", "run", "--no-sync", "python", "scripts/replay.py", "--all", cwd=tree, env=env)
        diff = sh("git", "diff", "--exit-code", "--stat", "data/replay.lock.json", cwd=tree)
        checks["replay"] = {"exit": r.returncode, "lock_unchanged": diff.returncode == 0}
    r = sh("uv", "run", "--no-sync", "python", "scripts/run_portable.py", "--landing", "data/fixtures/landing",
           "--out", "data/lake/fixture", cwd=tree, env=env)  # fmt: skip
    r2 = sh("uv", "run", "--no-sync", "python", "scripts/run_features.py", "--lake", "data/lake/fixture", cwd=tree,
            env=env)  # fmt: skip
    r3 = sh("uv", "run", "--no-sync", "python", "scripts/summarize_lake.py", "--lake", "data/lake/fixture",
            "--engine", "fixture", "--out", "data/work/rederive_reconcile.json", cwd=tree, env=env)  # fmt: skip
    mine = json.loads((tree / "data" / "work" / "rederive_reconcile.json").read_text()) if r3.returncode == 0 else {}
    ref = json.loads((ROOT / "results" / "reconcile.json").read_text()).get("fixture", {})
    same = {
        k: mine.get("fixture", {}).get("tables", {}).get(k, {}).get("digest") == v["digest"]
        for k, v in ref.get("tables", {}).items()
    }
    checks["fixture_pipeline"] = {"exit": [r.returncode, r2.returncode, r3.returncode], "tables_equal": same,
                                  "all_equal": bool(same) and all(same.values())}  # fmt: skip
    ref_eval = json.loads((ROOT / "results" / "evaluation.json").read_text())
    if args.evaluation:
        runs = []
        for path in args.evaluation:
            rerun = json.loads(path.read_text())
            worst, other = differences(rerun, ref_eval)
            runs.append({"file": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
                         "digest_equal": rerun.get("digest") == ref_eval.get("digest"),
                         "worst_relative_difference": worst, "other_differences": other[:10]})  # fmt: skip
        # Every graded number within the parity tolerance and everything else identical: thresholds,
        # verdicts, champions, gates. An equal digest is the stronger statement and is recorded too.
        checks["evaluation"] = {"where": "Actions runner, every drop again", "runs": runs,
                                "all_equal": all(r["worst_relative_difference"] <= TOLERANCE and not
                                                    r["other_differences"] for r in runs)}  # fmt: skip
    elif args.scored.exists():
        out = tree / "results" / "evaluation.json"
        r = sh("uv", "run", "--no-sync", "python", "scripts/grade.py", "--scored", str(args.scored), "--source",
               "local", "--out", str(out), cwd=tree, env=env)  # fmt: skip
        new_eval = json.loads(out.read_text()) if r.returncode == 0 else {}
        checks["evaluation"] = {"where": "build machine", "exit": r.returncode,
                                "digest_equal": new_eval.get("digest") == ref_eval.get("digest")}  # fmt: skip
    if args.model:
        served = ROOT / "results" / "models" / "payments" / "model.txt"
        checks["served_model"] = {"digest_equal": args.model.read_bytes() == served.read_bytes()}
    if args.month_workspace and args.month_open_source:
        ws = json.loads(args.month_workspace.read_text())["tests"]["month_rerun"]["digests"]
        local = json.loads(args.month_open_source.read_text())
        local = local if "tables" in local else local.get("local", {})  # filed under its engine name
        tables = {k: {"rows": v["rows"], "digest_equal": local["tables"].get(k, {}).get("digest") == v["digest"]}
                  for k, v in sorted(ws.items())}  # fmt: skip
        published = json.loads((ROOT / "results" / "reconcile_first_month.json").read_text())["tables"]
        rows = {k: v["rows"] == published.get(k, {}).get("rows_workspace") for k, v in ws.items() if k in published}
        same = all(t["digest_equal"] for t in tables.values()) and all(rows.values())
        checks["month"] = {"tables": tables, "all_equal": same, "rows_equal_to_published": rows}
    ok = all(
        v.get("lock_unchanged", True) and v.get("all_equal", True) and v.get("digest_equal", True)
        for v in checks.values()
        if isinstance(v, dict)
    )
    result = {
        "at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "head": head,
        "checks": checks,
        "drift": not ok,
        "complete": {"replay", "fixture_pipeline", "evaluation", "served_model", "month"} <= set(checks),
        "seconds": round(time.monotonic() - started, 1),
    }
    (ROOT / "results" / "rederive.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n")
    sh("git", "worktree", "remove", "--force", str(tree), cwd=ROOT)
    print(json.dumps(result, indent=1))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
