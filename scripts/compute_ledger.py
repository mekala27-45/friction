"""The compute ledger: what each step of the build cost in machine time.

    python scripts/compute_ledger.py

Reads the timings the steps leave behind (the pipeline's run log, the model step's timings, the
summaries of the lake, the grade, the export and the experiments) and writes results/compute_ledger.json.
On the build machine the cost is wall clock seconds on a stated number of cores; the workspace's
ledger adds DBUs from system.billing.usage through the export job once the workspace runs.
"""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
FLOW = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d): Flow spark_catalog\.default\.(\w+) is (RUNNING|COMPLETED)")


def pipeline_flows(log: Path) -> dict[str, float]:
    starts: dict[str, datetime] = {}
    out: dict[str, float] = {}
    for line in log.read_text(encoding="utf-8", errors="replace").splitlines():
        m = FLOW.match(line)
        if not m:
            continue
        ts = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
        if m.group(3) == "RUNNING":
            starts[m.group(2)] = ts
        elif m.group(2) in starts:
            out[m.group(2)] = (ts - starts[m.group(2)]).total_seconds()
    return out


def _json(rel: str) -> Any:
    p = ROOT / rel
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def main() -> int:
    ledger: dict[str, Any] = {"machine": {"cores": os.cpu_count()}, "steps": []}
    log = ROOT / "data" / "lake" / "full" / "run.log"
    if log.exists():
        flows = pipeline_flows(log)
        ledger["pipeline_flows_seconds"] = dict(sorted(flows.items(), key=lambda kv: -kv[1]))
        run = (ROOT / "data" / "lake" / "full_run.out").read_text(encoding="utf-8", errors="replace")
        m = re.search(r"portable pipeline exited (\d+) after ([\d.]+) s", run)
        if m:
            ledger["steps"].append({"step": "portable pipeline, full", "seconds": float(m.group(2))})
    timings = _json("data/work/models/full/timings.json")
    for name, secs in (timings or {}).items():
        ledger["steps"].append({"step": f"models: {name}", "seconds": secs})
    for rel, name in (("results/evaluation.json", "grade"), ("results/monitor.json", "monitor")):
        doc = _json(rel)
        if doc and "seconds" in doc:
            ledger["steps"].append({"step": name, "seconds": doc["seconds"]})
    rec = _json("results/reconcile.json")
    if rec and "local" in rec:
        ledger["steps"].append({"step": "summarize the lake", "seconds": rec["local"]["seconds"]})
    ledger["total_seconds"] = round(sum(s["seconds"] for s in ledger["steps"]), 1)
    ledger["workspace"] = _json("results/workspace/compute.json")
    (ROOT / "results" / "compute_ledger.json").write_text(json.dumps(ledger, indent=1) + "\n", encoding="utf-8")
    print(json.dumps({k: ledger[k] for k in ("steps", "total_seconds")}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
