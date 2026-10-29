"""The floors, the learners, the Freeman model and the reason codes."""

from __future__ import annotations

import math

import numpy as np
import pytest
from friction_features import logins, payments
from friction_models import floors
from friction_models.freeman import MIN_COUNT, Freeman, smoothed
from friction_models.learners import Gbm, Logistic
from friction_reasons.reasons import feature_to_code, load_table, top_codes

from tests.conftest import ROOT

PAY = {n: i for i, n in enumerate(payments.FEATURES)}
LOG = {n: i for i, n in enumerate(logins.FEATURES)}


def _pay_row(**over: float) -> np.ndarray:
    row = np.zeros(len(payments.FEATURES))
    row[PAY["days_since_issue"]] = 100
    row[PAY["hour"]] = 12
    for k, v in over.items():
        row[PAY[k]] = v
    return row


def test_floor_rules_fire_and_name_their_codes() -> None:
    row = _pay_row(cnt_1h=3, log_amount_to_median=math.log(6), log_n_prior=2.0)
    assert floors.payment_fired(row) == ["P03", "P01"]
    night = _pay_row(merchant_new=1, channel=2, hour=3)
    assert floors.payment_fired(night) == ["P07"]
    assert floors.payment_fired(_pay_row()) == []
    s = floors.payment_scores(np.vstack([row, night, _pay_row()]))
    assert s[0] == pytest.approx(4.0, abs=1e-5) and s[1] == pytest.approx(2.0, abs=1e-5) and s[2] < 1e-5


def test_floor_never_scores_a_first_login() -> None:
    row = np.zeros(len(logins.FEATURES))
    assert floors.login_fired(row) == []
    row[LOG["n_success_prior"]] = 5
    assert floors.login_fired(row) == ["L02", "L04", "L03"]
    for f in ("country_count", "device_count", "browser_count", "os_count", "asn_count"):
        row[LOG[f]] = 3
    assert floors.login_fired(row) == []


def _data(n: int = 3000, seed: int = 0) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    x = rng.normal(size=(n, 4))
    y = (rng.random(n) < 1 / (1 + np.exp(-(2 * x[:, 0] - x[:, 1] - 3)))).astype(int)
    return x, y


def test_learners_are_deterministic_and_contributions_add_up() -> None:
    x, y = _data()
    names = ["a", "b", "c", "d"]
    g1, g2 = Gbm.fit(x, y, names), Gbm.fit(x, y, names)
    assert g1.to_text() == g2.to_text()
    raw = g1.booster.predict(x[:50], raw_score=True)
    full = g1.booster.predict(x[:50], pred_contrib=True)
    assert np.allclose(full.sum(axis=1), raw)
    lr = Logistic.fit(x, y)
    again = Logistic.from_dict(lr.to_dict())
    assert np.allclose(again.predict(x), lr.predict(x))
    assert np.allclose(lr.contributions(x).sum(axis=1) + lr.intercept, lr.margin(x))


def test_sampling_weights_keep_the_probability_level() -> None:
    x, y = _data(20_000, 1)
    keep = (y == 1) | (np.random.default_rng(2).random(len(y)) < 0.25)
    w = np.where(y[keep] == 1, 1.0, 4.0)
    lr = Logistic.fit(x[keep], y[keep], weight=w)
    assert abs(lr.predict(x).mean() - y.mean()) < 0.01


def test_reason_codes_cover_stay_put_and_stay_distinct() -> None:
    table = load_table(ROOT / "evaluation" / "reason_codes.yaml")
    for desk in ("payments", "logins"):
        assert len(table[desk]) == 12
    mapping = feature_to_code(table, "payments")
    assert set(mapping) == set(payments.FEATURES)
    contrib = np.zeros(len(payments.FEATURES))
    contrib[PAY["cnt_1h"]] = 0.9
    contrib[PAY["sum_1h_usd"]] = 0.8
    contrib[PAY["amount_z"]] = 0.5
    contrib[PAY["hour"]] = -2.0
    assert top_codes(contrib, payments.FEATURES, mapping) == ["P03", "P01"]
    x, y = _data()
    g = Gbm.fit(x, y, ["cnt_1h", "amount_z", "hour", "foreign"])
    small = {"cnt_1h": "P03", "amount_z": "P01", "hour": "P11", "foreign": "P08"}
    names = ["cnt_1h", "amount_z", "hour", "foreign"]
    a = [top_codes(r, names, small) for r in g.contributions(x[:200])]
    b = [top_codes(r, names, small) for r in g.contributions(x[:200] + 1e-12)]
    assert a == b


def test_every_login_feature_has_a_code() -> None:
    table = load_table(ROOT / "evaluation" / "reason_codes.yaml")
    mapping = feature_to_code(table, "logins")
    assert set(logins.FEATURES) <= set(mapping)


def _freeman() -> Freeman:
    rows = []
    for i in range(400):
        values = (f"ip{i % 40}", "asn1" if i % 4 else "asn2", "NO", "", "", f"ua{i % 7}", "Chrome", "Android", "mobile")
        rows.append((values, True))
    rows.append((("ip_once", "asn9", "US", "", "", "ua_once", "Firefox", "Linux", "desktop"), True))
    return Freeman.fit(rows, users=50)


def test_freeman_smoothing_and_pruning() -> None:
    assert smoothed(0, 100, 10) == pytest.approx(1 / 110)
    assert smoothed(10, 100, 10) == pytest.approx(0.1 * (1 - 10 / 110))
    f = _freeman()
    assert "ip_once" not in f.counts["ip"] and f.distinct["ip"] == 41
    assert all(v >= MIN_COUNT for v in f.counts["ip"].values())
    assert "US" in f.counts["country"]


def test_freeman_scores_a_new_place_above_a_usual_one() -> None:
    # Freeman et al. approximate the attacker's distribution of each feature by the global one, so
    # the attack here uses values common across users and new to this one. A value unseen
    # everywhere scores low under that reading, which the login page reports.
    f = _freeman()
    feats = np.zeros(len(logins.FEATURES))
    feats[LOG["n_success_prior"]] = 20
    for lvl in logins.LEVELS:
        feats[LOG[f"{lvl}_count"]] = 20
        feats[LOG[f"{lvl}_distinct"]] = 1
    usual = ("ip1", "asn1", "NO", "", "", "ua1", "Chrome", "Android", "mobile")
    stranger = feats.copy()
    for lvl in logins.LEVELS:
        if lvl not in ("country", "browser", "os", "device"):
            stranger[LOG[f"{lvl}_count"]] = 0
    attack = ("ip5", "asn2", "NO", "", "", "ua3", "Chrome", "Android", "mobile")
    assert f.log_score(attack, stranger) > f.log_score(usual, feats)
    contrib = f.contributions(attack, stranger)
    assert len(contrib) == 7 and max(contrib) > 0
    again = Freeman.from_json(f.to_json())
    assert again.log_score(attack, stranger) == f.log_score(attack, stranger)
