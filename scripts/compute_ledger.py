"""The compute ledger: what each step of the build cost, where it ran.

    python scripts/compute_ledger.py

Reads the records the workspace workflow left on the results branch (every request's done.json,
with its wall seconds), the model step's timings from the runner's open source run, and the
workspace's billing usage when a request has read system.billing.usage (results/workspace/
billing.json). Writes results/compute_ledger.json.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BRANCH = "origin/results"


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def records() -> list[dict[str, Any]]:
    names = [n for n in _git("ls-tree", "--name-only", "-r", BRANCH, "runs").splitlines() if n.endswith("/done.json")]
    out = []
    for name in sorted(names):
        d = json.loads(_git("show", f"{BRANCH}:{name}"))
        result = d.get("result") or {}
        if d.get("status") != "succeeded" or "wall_seconds" not in result:
            continue
        req = d["request"]
        out.append(
            {
                "request": req["id"],
                "kind": req["kind"],
                "job": req.get("job"),
                "where": "Actions runner, open source Spark" if req["kind"] == "portable" else "Databricks workspace",
                "seconds": float(result["wall_seconds"]),
                "github_run": d.get("github_run_id"),
            }
        )
    return out


def _json(rel: str) -> Any:
    p = ROOT / rel
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def main() -> int:
    steps = records()
    ledger: dict[str, Any] = {
        "steps": steps,
        "workspace_seconds": round(sum(s["seconds"] for s in steps if s["where"] == "Databricks workspace"), 1),
        "runner_seconds": round(sum(s["seconds"] for s in steps if s["where"].startswith("Actions")), 1),
        "runner_model_steps_seconds": _json("results/workspace/models_timings_runner.json"),
        "billing": _json("results/workspace/billing.json"),
        "machines": {
            "Actions runner": "4 cores, 16 GB (GitHub hosted ubuntu-latest)",
            "Databricks workspace": "Free Edition, serverless",
        },
    }
    (ROOT / "results" / "compute_ledger.json").write_text(json.dumps(ledger, indent=1) + "\n", encoding="utf-8")
    print(json.dumps({k: ledger[k] for k in ("workspace_seconds", "runner_seconds")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
