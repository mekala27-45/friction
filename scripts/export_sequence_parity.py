"""The sequence shadow's parity set: each card's earlier events, and the lake's score of every event.

    python scripts/export_sequence_parity.py --lake data/lake/<id> --out <run>/parity_shadow_payments.json

Run on a lake the open source build wrote with the workspace salt. The events are the ones in the
committed results/parity/payments.json, so this set and the champion's describe the same replay. For
each card the replay touches, the history is the lag columns of its first event in the window: its
last events since 1 November 2015, oldest first, as the online store should hold them before the
replay starts. The expected score of each event is the network in results/models/payments/shadow
run over the same lag columns in a batch, as the lakehouse scores (sequence.steps), so a difference
in the replay is a difference in the online history or in one row against a batch, never a
different model. The verdict and the codes follow the shadow's thresholds and reason map.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import numpy as np
from friction_core.hashing import sha256_bytes
from friction_models import sequence
from friction_policy.policy import Thresholds, one
from friction_reasons.reasons import top_codes
from friction_transforms import session

SHADOW = ROOT / "results" / "models" / "payments" / "shadow"


def build(read: Any, spark: Any, parity: dict[str, Any], shadow: Path) -> dict[str, Any]:
    meta = json.loads((shadow / "bundle.json").read_text(encoding="utf-8"))
    if meta["model"] != "gru":
        raise SystemExit(f"the payments shadow is {meta['model']}, not the sequence model")
    network, means = sequence.from_json((shadow / "payments_gru.json").read_text(encoding="utf-8"))
    thresholds = Thresholds(**meta["thresholds"])
    codes_of = sequence.code_map(meta["reason_map"])
    events = parity["events"]
    order = {int(e["event_ref"]): i for i, e in enumerate(events)}
    accounts = spark.createDataFrame([(a,) for a in sorted({e["account_key"] for e in events})], "account_key string")
    refs = spark.createDataFrame([(r,) for r in sorted(order)], "txn_ref long")
    # Only the cards in the window: a card's lags read its own rows and nothing else.
    silver = read("payments_silver").join(accounts, "account_key")
    lags = sequence.lagged(read("payments_features"), silver).join(refs, "txn_ref")
    now = read("payments_features").select("txn_ref", *sequence.STEP).join(refs, "txn_ref")
    pdf = now.join(lags, "txn_ref").toPandas()
    if len(pdf) != len(order):
        raise SystemExit(f"{len(order) - len(pdf)} replay events have no row in the lake")
    pdf["_i"] = pdf["txn_ref"].map(order)
    pdf = pdf.sort_values("_i").reset_index(drop=True)
    seq = sequence.steps(pdf)
    scores = sequence.predict(network, seq)
    history: dict[str, dict[str, list[list[float]]]] = {}
    expected = []
    for i, e in enumerate(events):
        acc = e["account_key"]
        if acc not in history:
            row = pdf.iloc[i]
            steps = []
            for k in range(sequence.LAGS, 0, -1):
                values = [row[f"{c}__{k}"] for c in sequence.STEP]
                if not np.isnan(values[0]):
                    steps.append([float(v) for v in values])
            history[acc] = {"steps": steps}
        score = float(scores[i])
        verdict = one(score, int(e["channel"]), thresholds)
        codes: list[str] = []
        if verdict != "approve":
            contrib = sequence.contributions(network, means, seq[i : i + 1])[0]
            codes = top_codes(contrib, list(sequence.ATTRIBUTES), codes_of)
        expected.append({"event_ref": int(e["event_ref"]), "score": score, "verdict": verdict, "codes": codes})
    held = [len(h["steps"]) for h in history.values()]
    return {
        "desk": "payments",
        "model": "gru",
        "bundle_digest": sha256_bytes(json.dumps(meta, sort_keys=True).encode()),
        "parity_events_sha256": sha256_bytes(json.dumps(events, sort_keys=True).encode()),
        "snapshot": {"card_steps": dict(sorted(history.items()))},
        "expected": expected,
        "cards": len(history),
        "cards_with_full_history": sum(1 for n in held if n == sequence.LAGS),
        "flagged": sum(1 for x in expected if x["verdict"] != "approve"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--parity", type=Path, default=ROOT / "results" / "parity" / "payments.json")
    parser.add_argument("--shadow", type=Path, default=SHADOW)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=16)
    read = lambda name: spark.read.parquet(str(wh / name))  # noqa: E731
    parity = json.loads(args.parity.read_text(encoding="utf-8"))
    out = build(read, spark, parity, args.shadow)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(out, sort_keys=True) + "\n", encoding="utf-8")
    print(f"sequence parity: {len(out['expected'])} events, {out['cards']} cards, "
          f"{out['cards_with_full_history']} with a full history, {out['flagged']} flagged")  # fmt: skip
    return 0


if __name__ == "__main__":
    sys.exit(main())
