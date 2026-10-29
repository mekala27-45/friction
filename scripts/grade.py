"""Grade both desks in the repository from the exported scored sets.

    python scripts/grade.py --scored data/work/models/full/scored --source local --out results/evaluation.json

Refuses to grade when evaluation/plan.yaml or evaluation/hypotheses.yaml differ from the hashes in
evaluation/plan.lock. Writes one JSON document that the manifest builder reads; every number on a
published page that comes from grading comes from this file.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import yaml
from friction_core.hashing import sha256_json
from friction_evaluation import bootstrap, grade, interior

from scripts.check_registration import check as check_registration

# The bounds of the promotion gates, as evaluation/plan.yaml words them (tests/evaluation checks
# that these equal the numbers in the plan's text).
GATE_BOUNDS = {"ece": 0.05, "psi": 0.2, "friction_factor": 1.25}
VARIANTS = {"lag_setting": ("lag_fast", "lag_slow"), "training_months": ("months_12", "months_18")}


def read(folder: Path) -> pd.DataFrame:
    return pq.read_table(folder).to_pandas()


def clean(value: Any) -> Any:
    """JSON safe: no NaN, numpy scalars as Python numbers, private keys dropped."""
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items() if not str(k).startswith("_")}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if isinstance(value, (np.floating, float)):
        f = float(value)
        return None if math.isnan(f) or math.isinf(f) else f
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def seed_interior(desk: grade.Desk, graded: dict[str, Any], model: str) -> dict[str, Any]:
    g = graded["_graded"]
    out = {}
    for seed in (bootstrap.SEED, 1, 2):
        st = grade.Stats(g)
        for k, v in grade.event_columns(g, graded["_decisions"][model]).items():
            st.add(f"{model}:{k}", v)
        st.build(bootstrap.RESAMPLES, seed)
        out[str(seed)] = st.ratio(f"{model}:captured_usd", "fraud_usd")
    return out


def variant_interior(desk: grade.Desk, scored: Path, registered: dict[str, float]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    models = [m for m in desk.family if m != "floor"]
    for parameter, tags in VARIANTS.items():
        by_setting: dict[str, dict[str, float]] = {"registered": {m: registered[m] for m in models}}
        missing = []
        for tag in tags:
            folder = scored / "variants" / tag
            if not (folder / f"{desk.name}_test").exists():
                missing.append(tag)
                continue
            val = grade.prepare(desk, read(folder / f"{desk.name}_validation"))
            tst = grade.prepare(desk, read(folder / f"{desk.name}_test"))
            row = {}
            for m in models:
                d = grade.decide(desk, tst, m, grade.choose(desk, val, m))
                cols = grade.event_columns(tst, d)
                fusd = float(np.where(tst.fraud, tst.usd, 0.0).sum())
                row[m] = float(cols["captured_usd"].sum() / fusd) if fusd else float("nan")
            by_setting[tag] = row
        out[parameter] = {
            "by_setting": by_setting,
            "champion": {k: interior.champion(v) for k, v in by_setting.items()},
            "verdict": interior.verdict(by_setting, "registered", list(tags)) if not missing else None,
            "missing": missing,
        }
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scored", type=Path, required=True)
    parser.add_argument("--source", choices=["workspace", "local", "fixture"], required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "evaluation.json")
    parser.add_argument("--resamples", type=int, default=bootstrap.RESAMPLES)
    args = parser.parse_args()
    started = time.monotonic()
    plan_hash = check_registration(ROOT)
    plan = yaml.safe_load((ROOT / "evaluation" / "plan.yaml").read_text(encoding="utf-8"))
    registered = yaml.safe_load((ROOT / "evaluation" / "hypotheses.yaml").read_text(encoding="utf-8"))
    published = yaml.safe_load((ROOT / "evaluation" / "rba_published.yaml").read_text(encoding="utf-8"))
    costs = yaml.safe_load((ROOT / "config" / "costs.yaml").read_text(encoding="utf-8"))["assumptions"]
    desks = grade.desks(plan)
    result: dict[str, Any] = {
        "source": args.source,
        "plan_sha256": plan_hash,
        "gate_bounds": GATE_BOUNDS,
        "resamples": args.resamples,
        "seed": bootstrap.SEED,
        "desks": {},
    }
    validation_sets: dict[str, pd.DataFrame] = {}
    for name, desk in desks.items():
        val = read(args.scored / f"{name}_validation")
        tst = read(args.scored / f"{name}_test")
        validation_sets[name] = val
        graded = grade.grade_desk(
            desk, val, tst, resamples=args.resamples, as_of_label="label_as_of_end" if name == "payments" else None
        )
        gates = grade.gates(desk, graded, GATE_BOUNDS)
        champ = grade.champion(desk, graded, {m: g for m, g in gates.items() if m in desk.family})
        entry = dict(graded)
        entry["gates"] = gates
        entry["champion"] = champ
        entry["cost"] = grade.expected_cost(desk, graded, costs)
        family_models = [m for m in desk.family if m != "floor"]
        budget_rows = grade.interior_budget(desk, graded, family_models)
        entry["interior"] = {
            "budget": {
                "by_setting": budget_rows,
                "champion": {k: interior.champion(v) for k, v in budget_rows.items()},
                "verdict": interior.verdict(budget_rows, "registered", ["half", "double"]),
            }
        }
        lead = champ["model"] or max(
            sorted(family_models), key=lambda m: graded["models"][m]["validation"]["capture_usd"]
        )
        entry["interior"]["bootstrap_seed"] = {"model": lead, "intervals": seed_interior(desk, graded, lead)}
        if name == "payments":
            entry["interior"].update(variant_interior(desk, args.scored, budget_rows["registered"]))
        if name == "logins":
            entry["attackers"] = grade.per_attacker(graded, desk)
            entry["rba"] = grade.rba_comparison(graded, published)
        result["desks"][name] = entry
    result["hypotheses"] = grade.hypotheses(registered, validation_sets, desks, args.resamples)
    result["seconds"] = round(time.monotonic() - started, 1)
    payload = clean(result)
    payload["digest"] = sha256_json({k: v for k, v in payload.items() if k != "seconds"})
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    for name, d in payload["desks"].items():
        champ = d["champion"]["model"]
        print(
            name, "champion", champ, {m: round(e["test"]["capture_usd"]["estimate"], 4) for m, e in d["models"].items()}
        )
    print(f"graded in {payload['seconds']} s; wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
