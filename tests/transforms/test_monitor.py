"""Each monitor check fires on a planted breach and stays quiet on a clean report."""

from __future__ import annotations

import yaml
from friction_transforms import monitor

from tests.conftest import ROOT

CFG = yaml.safe_load((ROOT / "config" / "monitor.yaml").read_text(encoding="utf-8"))


def _clean() -> dict:  # type: ignore[type-arg]
    return {
        "desks": {
            "payments": {
                "feature_drift": [{"feature": "amount_z", "month": "2019-08", "psi": 0.02}],
                "expectations": [{"drop_id": "0386_2020-01-06", "rule": "P_LATE", "rows": 1, "share": 0.0001}],
                "completeness": [{"drop_id": "0386_2020-01-06", "delivered": 8000, "kept": 8000, "ratio": 1.0}],
                "clock": [],
            }
        }
    }


def test_clean_report_raises_nothing() -> None:
    assert monitor.alerts(_clean(), CFG, {"2019-08"}) == []


def test_each_check_fires_on_its_planted_breach() -> None:
    r = _clean()
    d = r["desks"]["payments"]
    d["feature_drift"].append({"feature": "hour", "month": "2019-09", "psi": 0.4})
    d["expectations"].append({"drop_id": "0387_2020-01-07", "rule": "P_RESCUED", "rows": 4000, "share": 0.5})
    d["completeness"].append({"drop_id": "0387_2020-01-07", "delivered": 8000, "kept": 4000, "ratio": 0.5})
    d["clock"].append({"drop_id": "0387_2020-01-07", "file": "part2.csv.gz", "rows": 900, "share": 0.25})
    got = {a["check"] for a in monitor.alerts(r, CFG, {"2019-08", "2019-09"})}
    assert got == {"feature_psi", "expectation_rate", "completeness", "clock"}


def test_drift_outside_the_scored_months_does_not_page() -> None:
    r = _clean()
    r["desks"]["payments"]["feature_drift"].append({"feature": "hour", "month": "2005-01", "psi": 0.9})
    assert monitor.alerts(r, CFG, {"2019-08"}) == []


def test_psi_from_counts() -> None:
    assert monitor.psi_from_counts([10, 10, 10], [10, 10, 10]) == 0.0
    assert monitor.psi_from_counts([10, 10, 10], [1, 1, 28]) > 0.25
