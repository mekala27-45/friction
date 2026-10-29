"""Evaluation on planted data with known answers."""

from __future__ import annotations

import itertools

import numpy as np
import pandas as pd
import pytest
import yaml
from friction_evaluation import bootstrap, calibration, frontier, grade, interior, metrics, multiple
from friction_evaluation.frontier import Budget
from friction_policy import policy
from friction_policy.policy import APPROVE, DECLINE, STEP_UP, Thresholds
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.conftest import ROOT


def _payments(n: int = 4000, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    fraud = rng.random(n) < 0.02
    signal = rng.normal(0, 1, n) + 2.5 * fraud
    df = pd.DataFrame(
        {
            "txn_ref": np.arange(n),
            "account_key": [f"a{int(i)}" for i in rng.integers(0, 300, n)],
            "event_ts": pd.Timestamp("2019-07-01"),
            "amount_cents": rng.integers(100, 50_000, n),
            "channel": rng.integers(0, 3, n),
            "label": fraud,
            "labelled": fraud,
            "label_as_of_end": fraud & (rng.random(n) < 0.7),
            "hour": rng.integers(0, 24, n).astype(float),
            "merchant_new": (rng.random(n) < 0.2).astype(float),
            "foreign": (rng.random(n) < 0.05).astype(float),
        }
    )
    df["score_floor"] = np.round(signal) + 1e-6 * df["amount_cents"] / 50_000
    df["score_logistic"] = 1 / (1 + np.exp(-(signal - 4)))
    df["score_lightgbm"] = 1 / (1 + np.exp(-(1.3 * signal - 5)))
    for m in ("floor", "logistic", "lightgbm"):
        df[f"codes_{m}"] = np.where(df[f"score_{m}"] > df[f"score_{m}"].median(), "P03,P01", "")
    return df


def _plan() -> dict:  # type: ignore[type-arg]
    return yaml.safe_load((ROOT / "evaluation" / "plan.yaml").read_text(encoding="utf-8"))


def test_metrics_on_a_hand_counted_fixture() -> None:
    d = np.array([APPROVE, STEP_UP, DECLINE, APPROVE, DECLINE], dtype=np.int8)
    y = np.array([True, True, False, False, True])
    usd = np.array([10.0, 20.0, 5.0, 7.0, 30.0])
    o = metrics.outcome(d, y, usd)
    assert (o.frauds, o.captured, o.goods, o.goods_flagged, o.goods_declined) == (3, 2, 2, 1, 1)
    assert o.capture_usd == pytest.approx(50 / 60) and o.friction == pytest.approx(0.5)
    assert o.precision_all == pytest.approx(2 / 3)


def test_policy_never_steps_up_a_terminal_purchase() -> None:
    s = np.array([0.5, 0.5, 0.5, 0.95])
    ch = np.array([0, 1, 2, 1])
    t = Thresholds(step_up=0.4, decline=0.9)
    assert policy.payments(s, ch, t).tolist() == [APPROVE, APPROVE, STEP_UP, DECLINE]
    assert policy.logins(np.array([0.1, 0.5, 0.95]), t).tolist() == [APPROVE, STEP_UP, DECLINE]
    with pytest.raises(ValueError):
        Thresholds(step_up=0.9, decline=0.4)


@settings(max_examples=60, deadline=None)
@given(st.lists(st.floats(0, 1), min_size=5, max_size=60), st.integers(0, 2))
def test_policy_property_channel(scores: list[float], channel: int) -> None:
    s = np.array(scores)
    d = policy.payments(s, np.full(len(s), channel), Thresholds(step_up=0.3, decline=0.8))
    if channel != policy.ONLINE:
        assert not (d == STEP_UP).any()


@settings(max_examples=40, deadline=None)
@given(st.integers(0, 10_000))
def test_capture_and_friction_never_fall_as_the_threshold_falls(seed: int) -> None:
    rng = np.random.default_rng(seed)
    s = rng.random(300)
    y = rng.random(300) < 0.2
    usd = rng.random(300) * 100
    prev = None
    for t in np.linspace(1.0, 0.0, 15):
        o = metrics.outcome(policy.logins(s, Thresholds(step_up=t, decline=1.1)), y, usd)
        if prev is not None:
            assert o.captured_usd >= prev.captured_usd and o.goods_flagged >= prev.goods_flagged
        prev = o


def test_operating_point_rule_on_a_planted_validation_set() -> None:
    # 1,000 goods scored 0..999 and ten frauds at the top. A budget of 10 step ups and 1 decline
    # per 1,000 goods declines the top good only and steps up the next ten.
    goods = np.arange(1000, dtype=float)
    s = np.r_[goods, np.full(10, 5000.0)]
    y = np.r_[np.zeros(1000, bool), np.ones(10, bool)]
    t = frontier.choose(s, y, None, Budget(10, 1))
    d = policy.logins(s, t)
    assert int(((d == DECLINE) & ~y).sum()) == 1 and int(((d == STEP_UP) & ~y).sum()) == 10
    assert (d[y] == DECLINE).all()


def test_operating_point_steps_up_online_only() -> None:
    rng = np.random.default_rng(3)
    s = rng.random(5000)
    y = np.zeros(5000, bool)
    ch = rng.integers(0, 3, 5000)
    t = frontier.choose(s, y, ch, Budget(10, 1))
    d = policy.payments(s, ch, t)
    assert int((d == STEP_UP).sum()) <= 50 and int((d == DECLINE).sum()) <= 5
    assert not ((d == STEP_UP) & (ch != 2)).any()


def test_bootstrap_contains_the_estimate_and_is_reproducible() -> None:
    rng = np.random.default_rng(1)
    stats = np.column_stack([rng.integers(0, 5, 2500), rng.integers(5, 10, 2500)]).astype(float)
    a = bootstrap.resampled_sums(stats, 400, 11, chunk=600)
    b = bootstrap.resampled_sums(stats, 400, 11, chunk=600)
    assert np.array_equal(a, b)
    iv = bootstrap.ratio(stats, a, 0, 1)
    assert iv.contains(iv.estimate) and iv.low < iv.high
    # Each resample draws exactly as many users as there are.
    w = bootstrap.resampled_sums(np.ones((2500, 1)), 50, 3, chunk=700)
    assert np.all(w[:, 0] == 2500)


def test_chunked_bootstrap_matches_one_multinomial_in_distribution() -> None:
    stats = np.column_stack([np.r_[np.ones(100), np.zeros(900)], np.ones(1000)])
    one = bootstrap.resampled_sums(stats, 3000, 5, chunk=10_000)
    two = bootstrap.resampled_sums(stats, 3000, 5, chunk=97)
    r1, r2 = one[:, 0] / one[:, 1], two[:, 0] / two[:, 1]
    assert abs(r1.std() - r2.std()) < 0.002 and abs(r1.mean() - r2.mean()) < 0.002


def test_benjamini_hochberg_on_a_fixture() -> None:
    p = [0.01, 0.04, 0.03, 0.2]
    assert multiple.bh(p, 0.05) == [True, False, False, False]
    assert multiple.bh([0.001, 0.008, 0.039, 0.041, 0.042], 0.05) == [True, True, True, True, True]
    adj = multiple.adjusted(p)
    assert adj[0] == pytest.approx(0.04) and max(adj) <= 1.0


def test_threshold_free_reading() -> None:
    y = np.array([False, False, True, True])
    assert grade.roc_auc(np.array([0.1, 0.4, 0.35, 0.8]), y) == pytest.approx(0.75)
    assert grade.average_precision(np.array([0.1, 0.4, 0.35, 0.8]), y) == pytest.approx((1 + 2 / 3) / 2)


def test_psi_is_zero_on_the_same_scores_and_large_on_a_shift() -> None:
    rng = np.random.default_rng(2)
    x = rng.random(5000)
    assert grade.psi(x, x) == pytest.approx(0.0, abs=1e-9)
    assert grade.psi(x, x + 0.5) > 0.2


def test_calibration_of_a_calibrated_score() -> None:
    rng = np.random.default_rng(4)
    p = rng.random(50_000)
    y = rng.random(50_000) < p
    assert calibration.ece(p, y) < 0.02
    assert calibration.ece(np.clip(p * 0.2, 0, 1), y) > 0.2


def test_interior_verdict() -> None:
    by = {"registered": {"a": 0.6, "b": 0.5}, "half": {"a": 0.55, "b": 0.5}, "double": {"a": 0.4, "b": 0.7}}
    assert interior.verdict(by, "registered", ["half", "double"]) == "degenerate"
    assert interior.verdict(by, "registered", ["half"]) == "stable"


def test_grade_desk_end_to_end_and_the_gates() -> None:
    desk = grade.desks(_plan())["payments"]
    val, tst = _payments(seed=7), _payments(seed=8)
    g = grade.grade_desk(desk, val, tst, resamples=200, as_of_label="label_as_of_end")
    lg = g["models"]["lightgbm"]
    for k in ("capture_usd", "friction_per_1000"):
        iv = lg["test"][k]
        assert iv["low"] <= iv["estimate"] <= iv["high"]
    caps = [r["capture_usd"]["estimate"] for r in lg["frontier"]]
    fr = [r["friction_per_1000"] for r in lg["frontier"]]
    assert all(b >= a - 1e-12 for a, b in itertools.pairwise(caps))
    assert all(b >= a - 1e-12 for a, b in itertools.pairwise(fr))
    assert len(g["comparisons"]) == 3 and all("p_adjusted" in c for c in g["comparisons"])
    assert g["as_of"]["frauds_known"] < g["test"]["frauds"]
    gates = grade.gates(desk, g, {"ece": 0.05, "psi": 0.2, "friction_factor": 1.25})
    assert set(gates) == {"logistic", "lightgbm"}
    # The codes in the planted set cover only scores above the median, so coverage fails when
    # the operating point flags anything below it; it must never pass a model that flags uncoded.
    for m, gate in gates.items():
        cov = g["models"][m]["reasons"]["coverage"]
        assert gate["reason_code_coverage"]["passed"] == (cov >= 1.0)


def test_each_gate_fails_on_a_planted_breach() -> None:
    desk = grade.desks(_plan())["payments"]
    g = grade.grade_desk(desk, _payments(seed=1), _payments(seed=2), resamples=100)
    tight = grade.gates(desk, g, {"ece": 0.0, "psi": 0.0, "friction_factor": 0.0})
    for m in ("logistic", "lightgbm"):
        assert not tight[m]["calibration_error"]["passed"]
        assert not tight[m]["score_stability"]["passed"]
        assert not tight[m]["friction_within_budget"]["passed"]
    swapped = dict(g)
    swapped["comparisons"] = [
        {**c, "difference": {"estimate": -1.0, "low": -1.5, "high": -0.5}} for c in g["comparisons"]
    ]
    breach = grade.gates(desk, swapped, {"ece": 1, "psi": 9, "friction_factor": 9})
    assert not any(breach[m]["capture_above_floor"]["passed"] for m in ("logistic", "lightgbm"))
    champ = grade.champion(desk, swapped, breach)
    assert champ["model"] is None and "capture_above_floor" in champ["failed"]["lightgbm"]


def test_gate_bounds_match_the_plan() -> None:
    from scripts.grade import GATE_BOUNDS

    text = (ROOT / "evaluation" / "plan.yaml").read_text(encoding="utf-8")
    assert f"below {GATE_BOUNDS['ece']}" in text
    assert f"below {GATE_BOUNDS['psi']}" in text
    assert f"times {GATE_BOUNDS['friction_factor']}" in text


def test_hypotheses_run_once_each_with_a_verdict() -> None:
    desk = grade.desks(_plan())
    frame = _payments(seed=5)
    reg = {
        "payments": {
            "H2": {"question": "q", "a": "merchant_new == 1", "b": "merchant_new == 0", "measure": "count"},
            "H1": {"question": "q", "a": "channel == 2 and hour < 6", "b": "channel == 1", "measure": "dollars"},
        }
    }
    rows = grade.hypotheses(reg, {"payments": frame}, desk, resamples=200)
    assert [r["id"] for r in rows] == ["H2", "H1"]
    assert all(r["verdict"] in {"supported", "not supported"} and 0 < r["p"] <= 1 for r in rows)


def test_expected_cost_sensitivity_names_the_cheapest() -> None:
    desk = grade.desks(_plan())["payments"]
    g = grade.grade_desk(desk, _payments(seed=3), _payments(seed=4), resamples=50)
    costs = yaml.safe_load((ROOT / "config" / "costs.yaml").read_text(encoding="utf-8"))["assumptions"]
    c = grade.expected_cost(desk, g, costs)
    assert c["cheapest"] in desk.models and len(c["sensitivity"]) == 12
