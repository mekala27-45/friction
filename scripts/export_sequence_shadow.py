"""Put the promoted sequence model behind the API in shadow: results/models/payments/shadow/.

    python scripts/export_sequence_shadow.py

evaluation/plan_v3.yaml registered that a promoted sequence model runs in shadow first, because the
API kept no event history. The network is the file promoted from the run that graded it
(results/models/v3/payments_gru.json, its provenance in results/provenance.json), and the thresholds
are the ones results/evaluation_v3.json chose for it on validation, so the shadow decides as the
graded model would. It is not in Unity Catalog: it was trained on the runner, and its alias moves only
after exact parity and latency, as for every champion. The logistic shadow it replaces stays on
record in results/parity_shadow.json.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_core.hashing import sha256_bytes
from friction_reasons.reasons import feature_to_code, load_table

MODEL = ROOT / "results" / "models" / "v3" / "payments_gru.json"
EVALUATION = ROOT / "results" / "evaluation_v3.json"
FOLDER = ROOT / "results" / "models" / "payments" / "shadow"


def main() -> int:
    evaluation = json.loads(EVALUATION.read_text(encoding="utf-8"))
    desk = evaluation["desks"]["payments"]
    if desk["promotion"]["champion_after"] != "gru":
        raise SystemExit("the sequence model was not promoted on the grade; nothing goes in shadow")
    data = MODEL.read_bytes()
    provenance = json.loads((ROOT / "results" / "provenance.json").read_text(encoding="utf-8"))["promoted"]
    source = next(p for p in reversed(provenance) if p["file"] == "results/models/v3/payments_gru.json")
    if source["sha256"] != sha256_bytes(data):
        raise SystemExit("results/models/v3/payments_gru.json is not the file that was promoted")
    meta = {
        "desk": "payments",
        "model": "gru",
        "status": "shadow",
        "thresholds": desk["models"]["gru"]["thresholds"],
        "files": {"payments_gru.json": sha256_bytes(data)},
        "reason_map": feature_to_code(load_table(ROOT / "evaluation" / "reason_codes.yaml"), "payments"),
        "trained": {"request": source["request"], "github_run_id": source["github_run_id"]},
        "evaluation_digest": evaluation["digest"],
    }
    if FOLDER.exists():
        shutil.rmtree(FOLDER)
    FOLDER.mkdir(parents=True)
    (FOLDER / "payments_gru.json").write_bytes(data)
    (FOLDER / "bundle.json").write_text(json.dumps(meta, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"shadow payments: gru from {source['request']}, thresholds {meta['thresholds']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
