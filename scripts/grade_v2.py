"""Grade the challengers of an addendum (evaluation/plan_v2.yaml, plan_v3.yaml) against the models they challenge.

    python scripts/grade_v2.py --scored data/work/models/full/scored --scored-v2 data/work/models/full/scored_v2 \
        --models data/work/models/full/models/v2/summary.json --source local --out results/evaluation_v2.json
    python scripts/grade_v2.py --addendum evaluation/plan_v3.yaml --scored-v2 data/work/models/full/scored_v3 ...

Joins each challenger scored set to the v1 set of the same split on the event key (every row must
match, one to one), grades the desk family the addendum registers with the same code that graded
the first plan, and applies the addendum's promotion rule. Refuses to run unless the whole
registration, the addendum included, matches evaluation/plan.lock. results/evaluation.json is read
for the current champions and never written.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import yaml
from friction_core.hashing import sha256_json
from friction_evaluation import bootstrap, grade

from scripts.check_registration import addendum_hash
from scripts.grade import GATE_BOUNDS, clean, read

ADDENDUM = Path("evaluation/plan_v2.yaml")


def logistic_output(addendum: dict[str, Any]) -> set[str]:
    """The addendum's own model when its registration ends the network in a logistic output.

    Fitted on weighted binary cross entropy like LightGBM, its score is a probability in the same
    sense, so the calibration gate applies to it. Before this, probabilities were known by name only,
    which left the sequence model's calibration gate with no value (DECISIONS.md, 2026-11-01)."""
    model = addendum.get("model") or {}
    return {str(model["name"])} if "logistic output" in str(model.get("network", "")) else set()


def desks_v2(plan: dict[str, Any], addendum: dict[str, Any]) -> dict[str, grade.Desk]:
    out = {}
    logistic = logistic_output(addendum)
    for name, desk in grade.desks(plan).items():
        if name not in addendum["desks"]:  # an addendum may challenge one desk only
            continue
        a = addendum["desks"][name]
        models, family = tuple(a["models"]), tuple(a["family"])
        out[name] = dataclasses.replace(
            desk,
            models=models,
            family=family,
            probabilities=frozenset(m for m in models if m.startswith("lightgbm") or m == "logistic" or m in logistic),
            extra=tuple(m for m in models if m not in family),
        )
    return out


def joined(scored: Path, scored_v2: Path, desk: grade.Desk, split: str) -> pd.DataFrame:
    """The v1 set with the challenger columns; refuses a row the other side does not have."""
    base = read(scored / f"{desk.name}_{split}")
    extra = read(scored_v2 / f"{desk.name}_{split}")
    keep = [c for c in base.columns if not c.startswith(("score_", "codes_"))]
    keep += [c for m in desk.models if f"score_{m}" in base.columns for c in (f"score_{m}", f"codes_{m}")]
    out = base[[c for c in keep if c in base.columns]].merge(extra, on=desk.key, how="outer", validate="one_to_one",
                                                             indicator=True)  # fmt: skip
    unmatched = int((out["_merge"] != "both").sum())
    if unmatched:
        raise ValueError(f"{desk.name} {split}: {unmatched} rows are in one scored set and not the other")
    return out.drop(columns="_merge")


def promotion(
    desk: grade.Desk, graded: dict[str, Any], gates: dict[str, dict[str, Any]], champion: str | None,
    challengers: list[str], ablations: list[str],
) -> dict[str, Any]:  # fmt: skip
    """The addendum's rule: every gate, and an interval against the champion clear of zero above it."""
    by_pair = {(c["a"], c["b"]): c for c in graded["comparisons"]}
    out: dict[str, Any] = {"champion_before": champion, "challengers": {}}
    for m in challengers:
        failed = [k for k, v in gates[m].items() if not v["passed"]]
        entry: dict[str, Any] = {"failed_gates": failed, "ablation": m in ablations}
        pair = by_pair.get((m, champion)) if champion else None
        if champion and pair is None and (champion, m) in by_pair:  # the family lists the champion later
            flip = by_pair[(champion, m)]
            d = flip["difference"]
            pair = {**flip, "difference": {"estimate": -d["estimate"], "low": -d["high"], "high": -d["low"]}}
        if pair is not None:
            entry["against_champion"] = {"difference": pair["difference"], "p_adjusted": pair["p_adjusted"],
                                         "rejected": pair["rejected"]}  # fmt: skip
        # Outside the family (an ablation) there is no corrected comparison, and no promotion.
        clear = pair is not None and pair["difference"]["low"] > 0 if champion else True
        entry["promote"] = bool(not failed and clear and m not in ablations)
        out["challengers"][m] = entry
    winners = [m for m, e in out["challengers"].items() if e["promote"]]
    out["champion_after"] = (
        max(winners, key=lambda m: graded["models"][m]["validation"]["capture_usd"]) if winners else champion
    )
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scored", type=Path, required=True)
    parser.add_argument("--scored-v2", type=Path, required=True)
    parser.add_argument("--models", type=Path, help="the challengers' training summary (models/v2/summary.json)")
    parser.add_argument("--source", choices=["workspace", "local", "fixture"], required=True)
    parser.add_argument("--champions", type=Path, default=ROOT / "results" / "evaluation.json")
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "evaluation_v2.json")
    parser.add_argument("--resamples", type=int, default=bootstrap.RESAMPLES)
    parser.add_argument("--addendum", type=Path, default=ADDENDUM)
    args = parser.parse_args()
    started = time.monotonic()
    registered = addendum_hash(ROOT, args.addendum)
    plan = yaml.safe_load((ROOT / "evaluation" / "plan.yaml").read_text(encoding="utf-8"))
    addendum = yaml.safe_load((ROOT / args.addendum).read_text(encoding="utf-8"))
    first = json.loads(args.champions.read_text(encoding="utf-8"))
    result: dict[str, Any] = {
        "source": args.source,
        "addendum": str(args.addendum),
        "addendum_sha256": registered,
        "plan_sha256": first.get("plan_sha256"),
        "first_evaluation_digest": first.get("digest"),
        "gate_bounds": GATE_BOUNDS,
        "resamples": args.resamples,
        "seed": bootstrap.SEED,
        "desks": {},
    }
    for name, desk in desks_v2(plan, addendum).items():
        val = joined(args.scored, args.scored_v2, desk, "validation")
        tst = joined(args.scored, args.scored_v2, desk, "test")
        graded = grade.grade_desk(
            desk, val, tst, resamples=args.resamples, as_of_label="label_as_of_end" if name == "payments" else None
        )
        gates = grade.gates(desk, graded, GATE_BOUNDS)
        challengers = list(addendum["desks"][name]["challengers"])
        ablations = [m for m in challengers if m not in desk.family]
        entry = dict(graded)
        entry["gates"] = gates
        entry["promotion"] = promotion(desk, graded, gates, first["desks"][name]["champion"]["model"], challengers,
                                       ablations)  # fmt: skip
        if name == "logins":
            entry["attackers"] = grade.per_attacker(graded, desk)
        if args.models and args.models.exists():
            trained = json.loads(args.models.read_text(encoding="utf-8"))
            trained = trained.get(name, trained)
            entry["training"] = {"rows": trained["rows"], "positives": trained["positives"]}
            if "models" in trained:
                entry["training"]["gain_share"] = {m: e["gain_share"] for m, e in trained["models"].items()}
        result["desks"][name] = entry
    result["seconds"] = round(time.monotonic() - started, 1)
    payload = clean(result)
    payload["digest"] = sha256_json({k: v for k, v in payload.items() if k != "seconds"})
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    for name, d in payload["desks"].items():
        caps = {m: round(e["test"]["capture_usd"]["estimate"], 4) for m, e in d["models"].items()}
        print(name, caps, "champion after", d["promotion"]["champion_after"])
    print(f"graded in {payload['seconds']} s; wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
