"""Promote a reviewed file from the results branch into results/, with its provenance.

The workspace never writes to main. Its runs land on the results branch under runs/<request id>/,
each with a done.json naming the request, the Actions run, the commit and the digest of every file.
This script copies a file the build machine has reviewed into results/, checks its digest against
done.json, and records the promotion in results/provenance.json.

    python scripts/promote_results.py <request id> <file in the run> <destination under results/>
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_core.hashing import sha256_bytes

PROVENANCE = ROOT / "results" / "provenance.json"
BRANCH = "origin/results"


def _show(path: str) -> bytes:
    return subprocess.run(["git", "show", f"{BRANCH}:{path}"], cwd=ROOT, check=True, capture_output=True).stdout


def promote(request_id: str, name: str, dest: str, failed_because: str | None = None) -> dict[str, str]:
    """failed_because promotes evidence from a request whose failure was its expected outcome (the
    monitor fails its task on a breach); the reason is kept in the provenance entry."""
    done = json.loads(_show(f"runs/{request_id}/done.json"))
    if done.get("status") != "succeeded" and not failed_because:
        raise ValueError(f"{request_id} did not succeed; nothing from it is promoted")
    expected = done["files"].get(name)
    if expected is None:
        raise KeyError(f"{name} is not a file of {request_id}")
    data = _show(f"runs/{request_id}/{name}")
    if sha256_bytes(data) != expected:
        raise ValueError(f"{name} does not match the digest its run recorded")
    target = ROOT / "results" / dest
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    entry = {
        "file": f"results/{dest}",
        "sha256": expected,
        "request": request_id,
        "github_run_id": str(done.get("github_run_id")),
        "commit": str(done.get("commit")),
        "ran_at": str(done.get("finished_at")),
        "promoted_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if done.get("status") != "succeeded":
        entry["request_status"] = str(done.get("status"))
        entry["failed_because"] = str(failed_because)
    record = json.loads(PROVENANCE.read_text(encoding="utf-8")) if PROVENANCE.exists() else {"promoted": []}
    record["promoted"] = [e for e in record["promoted"] if e["file"] != entry["file"]] + [entry]
    record["promoted"].sort(key=lambda e: e["file"])
    PROVENANCE.write_text(json.dumps(record, indent=1) + "\n", encoding="utf-8")
    return entry


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("request_id")
    parser.add_argument("name")
    parser.add_argument("dest")
    parser.add_argument("--failed-because", help="promote from a request whose failure was the expected outcome")
    args = parser.parse_args()
    entry = promote(args.request_id, args.name, args.dest, args.failed_because)
    print(f"promoted {entry['file']} from {args.request_id} (run {entry['github_run_id']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
