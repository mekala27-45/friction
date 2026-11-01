"""The addendum's promotion rule and the join of challenger sets to the v1 sets."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import yaml

from scripts import grade_v2

ROOT = Path(__file__).resolve().parents[2]


def _desks() -> dict[str, Any]:
    plan = yaml.safe_load((ROOT / "evaluation" / "plan.yaml").read_text())
    addendum = yaml.safe_load((ROOT / "evaluation" / "plan_v2.yaml").read_text())
    return grade_v2.desks_v2(plan, addendum)


def _graded(diff: tuple[float, float, float], flip: bool = False) -> dict[str, Any]:
    est, low, high = diff
    pair = {"a": "lightgbm_graph", "b": "lightgbm"}
    d = {"estimate": est, "low": low, "high": high}
    if flip:
        pair = {"a": "lightgbm", "b": "lightgbm_graph"}
        d = {"estimate": -est, "low": -high, "high": -low}
    return {
        "comparisons": [{**pair, "difference": d, "p_adjusted": 0.01, "rejected": True}],
        "models": {m: {"validation": {"capture_usd": v}} for m, v in
                   (("lightgbm", 0.5), ("lightgbm_graph", 0.6), ("lightgbm_structure", 0.7))},
    }  # fmt: skip


PASS = {"a": {"passed": True}}
FAIL = {"a": {"passed": True}, "calibration_error": {"passed": False}}


def test_the_addendum_names_the_desks_it_grades() -> None:
    desks = _desks()
    assert desks["payments"].models == ("floor", "lightgbm", "lightgbm_graph", "lightgbm_structure")
    assert desks["payments"].extra == ("lightgbm_structure",)
    assert "lightgbm_graph" in desks["logins"].probabilities
    assert "freeman" not in desks["logins"].probabilities


def test_a_registered_logistic_output_is_graded_for_calibration() -> None:
    plan = yaml.safe_load((ROOT / "evaluation" / "plan.yaml").read_text())
    addendum = yaml.safe_load((ROOT / "evaluation" / "plan_v3.yaml").read_text())
    desk = grade_v2.desks_v2(plan, addendum)["payments"]
    assert desk.models == ("floor", "lightgbm", "gru")
    assert desk.probabilities == frozenset({"lightgbm", "gru"})
    assert grade_v2.logistic_output({"model": {"name": "x", "network": "a tree ensemble"}}) == set()


@pytest.mark.parametrize("flip", [False, True])
def test_a_challenger_needs_every_gate_and_an_interval_above_the_champion(flip: bool) -> None:
    desk = _desks()["payments"]
    gates = {"lightgbm_graph": PASS, "lightgbm_structure": PASS}
    chall = ["lightgbm_graph", "lightgbm_structure"]
    won = grade_v2.promotion(desk, _graded((0.05, 0.01, 0.09), flip), gates, "lightgbm", chall, ["lightgbm_structure"])
    assert won["challengers"]["lightgbm_graph"]["promote"]
    assert won["challengers"]["lightgbm_graph"]["against_champion"]["difference"]["low"] == pytest.approx(0.01)
    assert not won["challengers"]["lightgbm_structure"]["promote"]  # an ablation, never promoted
    assert won["champion_after"] == "lightgbm_graph"
    close = grade_v2.promotion(desk, _graded((0.03, -0.01, 0.07), flip), gates, "lightgbm", ["lightgbm_graph"], [])
    assert not close["challengers"]["lightgbm_graph"]["promote"]
    assert close["champion_after"] == "lightgbm"
    failed = grade_v2.promotion(desk, _graded((0.05, 0.01, 0.09)), {"lightgbm_graph": FAIL}, "lightgbm",
                                ["lightgbm_graph"], [])  # fmt: skip
    assert failed["challengers"]["lightgbm_graph"]["failed_gates"] == ["calibration_error"]
    assert failed["champion_after"] == "lightgbm"


def test_a_desk_with_no_champion_takes_a_challenger_that_clears_every_gate() -> None:
    desk = _desks()["logins"]
    out = grade_v2.promotion(desk, _graded((0, 0, 0)), {"lightgbm_graph": PASS}, None, ["lightgbm_graph"], [])
    assert out["champion_before"] is None and out["champion_after"] == "lightgbm_graph"


def test_the_join_refuses_a_row_on_one_side_only(tmp_path: Path) -> None:
    desk = _desks()["payments"]
    v1, v2 = tmp_path / "v1", tmp_path / "v2"
    base = pd.DataFrame({"txn_ref": [1, 2, 3], "label": [False, True, False], "score_lightgbm": [0.1, 0.9, 0.2],
                         "codes_lightgbm": ["", "P01", ""], "score_logistic": [0.0, 0.0, 0.0]})  # fmt: skip
    extra = pd.DataFrame({"txn_ref": [1, 2, 3], "score_lightgbm_graph": [0.2, 0.8, 0.1]})
    for folder, frame in ((v1, base), (v2, extra)):
        (folder / "payments_test").mkdir(parents=True)
        frame.to_parquet(folder / "payments_test" / "part-0.parquet")
    out = grade_v2.joined(v1, v2, desk, "test")
    assert "score_logistic" not in out.columns and list(out["score_lightgbm_graph"]) == [0.2, 0.8, 0.1]
    pd.DataFrame({"txn_ref": [1, 2], "score_lightgbm_graph": [0.2, 0.8]}).to_parquet(
        v2 / "payments_test" / "part-0.parquet"
    )
    with pytest.raises(ValueError, match="1 rows are in one scored set"):
        grade_v2.joined(v1, v2, desk, "test")
