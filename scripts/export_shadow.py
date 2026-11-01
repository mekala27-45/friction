"""Put a registered challenger behind the API in shadow: results/models/<desk>/shadow/.

    python scripts/export_shadow.py --desk payments --model logistic \
        --request 2026-10-30-18a-train-register --file export/payments_logistic.json

The model file is promoted from the results branch (scripts/promote_results.py records where it came
from) and must hash to the sha256 its Unity Catalog version was registered with, which the train
request's summary holds. The thresholds are the ones results/evaluation.json chose for that model
on validation, so the shadow decides as the graded challenger would; the reason map is the
champion's, since both read the same columns. The API loads it at start and never lets it decide.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_core.hashing import sha256_bytes

from scripts.promote_results import promote

FILES = {"logistic": "model.json", "lightgbm": "model.txt"}


def registered(request: str, desk: str, model: str) -> dict[str, str]:
    summary = json.loads(subprocess.run(["git", "show", f"origin/results:runs/{request}/export/summary.json"],
                                        cwd=ROOT, check=True, capture_output=True).stdout)  # fmt: skip
    name = f"workspace.friction_dev_ml.{desk}_{model}"
    entry = summary["registered"][name]
    return {"name": name, "version": str(entry["version"]), "sha256": str(entry["sha256"])}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--desk", required=True)
    parser.add_argument("--model", required=True, choices=sorted(FILES))
    parser.add_argument("--request", required=True)
    parser.add_argument("--file", required=True)
    args = parser.parse_args()
    folder = ROOT / "results" / "models" / args.desk / "shadow"
    folder.mkdir(parents=True, exist_ok=True)
    target = FILES[args.model]
    promote(args.request, args.file, f"models/{args.desk}/shadow/{target}")
    data = (folder / target).read_bytes()
    uc = registered(args.request, args.desk, args.model)
    if sha256_bytes(data) != uc["sha256"]:
        raise SystemExit(f"{target} does not hash to the registered version {uc['name']} {uc['version']}")
    evaluation = json.loads((ROOT / "results" / "evaluation.json").read_text(encoding="utf-8"))
    champion = json.loads((ROOT / "results" / "models" / args.desk / "bundle.json").read_text(encoding="utf-8"))
    meta = {
        "desk": args.desk,
        "model": args.model,
        "status": "shadow",
        "thresholds": evaluation["desks"][args.desk]["models"][args.model]["thresholds"],
        "files": {target: sha256_bytes(data)},
        "reason_map": champion["reason_map"],
        "registered": uc,
        "evaluation_digest": evaluation["digest"],
    }
    (folder / "bundle.json").write_text(json.dumps(meta, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"shadow {args.desk}: {uc['name']} version {uc['version']}, thresholds {meta['thresholds']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
