"""The monitor task on the build machine, over a local lake.

    python scripts/monitor.py --lake data/lake/full --out results/monitor.json

The workspace runs the same checks as jobs/monitor.py after every scoring run. Exits 1 when any
alert fires, which is what fails the task and opens the issue in the workspace workflow.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import yaml
from friction_features import logins as login_features
from friction_features import payments as payment_features
from friction_transforms import monitor, session


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, required=True)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "monitor.json")
    parser.add_argument("--no-fail", action="store_true")
    args = parser.parse_args()
    started = time.monotonic()
    cfg = yaml.safe_load((ROOT / "config" / "monitor.yaml").read_text(encoding="utf-8"))
    _active = (yaml.safe_load((ROOT / "contracts" / "approved_changes.yaml").read_text(encoding="utf-8")) or {}).get(
        "active", {}
    )
    rejected = {desk: tuple((v or {}).get("rejected_files", [])) for desk, v in (_active or {}).items()}
    plan = yaml.safe_load((ROOT / "evaluation" / "plan.yaml").read_text(encoding="utf-8"))
    wh = (args.lake / "spark-warehouse").resolve()
    spark = session.get(shuffle_partitions=32)
    read = lambda name: spark.read.parquet(str(wh / name))  # noqa: E731
    report: dict[str, object] = {"desks": {}}
    scored: set[str] = set()
    for desk, key, names, feats in (
        ("payments", "txn_ref", list(payment_features.FEATURES), "payments_features"),
        ("logins", "login_ref", [*login_features.FEATURES], "logins_features"),
    ):
        p = plan["desks"][desk]["splits"]
        train = (p["train"]["start"], p["train"]["end"])
        for split in ("validation", "test"):
            scored |= set(monitor.months(str(p[split]["start"]), str(p[split]["end"])))
        silver, bronze = read(f"{desk}_silver"), read(f"{desk}_bronze")
        quarantine = read("quarantine")
        d = {
            "feature_drift": monitor.feature_drift(read(feats), silver, key, names, train),
            "expectations": monitor.expectation_rates(quarantine, bronze, desk, rejected.get(desk, ())),
            "completeness": monitor.completeness(
                silver, bronze, key, "txn_ref" if desk == "payments" else "login_index"
            ),
            "clock": monitor.clock(
                quarantine, bronze, desk, "P_FUTURE" if desk == "payments" else "L_FUTURE", rejected.get(desk, ())
            ),
        }
        report["desks"][desk] = d  # type: ignore[index]
    report["label_maturity"] = monitor.label_maturity(
        read("labels_payments_history"), cfg["label_maturity"]["checkpoints_days"]
    )
    rows = monitor.alerts(report, cfg, scored)
    report["alerts"] = rows
    report["seconds"] = round(time.monotonic() - started, 1)
    args.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    print(f"monitor: {len(rows)} alerts in {report['seconds']} s")
    for r in rows[:20]:
        print(" ", r["severity"], r["check"], r["scope"], round(r["value"], 4))
    return 1 if rows and not args.no_fail else 0


if __name__ == "__main__":
    sys.exit(main())
