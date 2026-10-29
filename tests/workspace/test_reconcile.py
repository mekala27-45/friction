"""The two engine reconcile: exact on digests and integers, close on sums of doubles."""

from __future__ import annotations

import copy

from scripts.reconcile import compare


def _side() -> dict:  # type: ignore[type-arg]
    grains = {g: [{"rows": 2, "keys": 2, "amount_cents": 10}] for g in ("desk", "month", "day", "user_bucket")}
    return {
        "digests": {"payments_silver": {"rows": 2, "digest": "2:ab:cd"}},
        "tables": {"payments_silver": {"rows": 2, "digest": "2:ab:cd"}},
        "reconcile": {
            "payments": grains,
            "logins": copy.deepcopy(grains),
            "feature_sums": {"payments_features": {"rows": 2.0, "amount_z": 0.30000000000000004}},
            "histories": {},
            "quarantine": [{"desk": "payments", "rule": "P_LATE", "count": 1}],
        },
    }


def test_equal_sides_have_no_drift_and_float_sums_may_differ_in_the_last_place() -> None:
    a, b = _side(), _side()
    b["reconcile"]["feature_sums"]["payments_features"]["amount_z"] = 0.3
    assert compare(a, b)["drift"] is False


def test_a_digest_or_a_count_that_differs_is_drift() -> None:
    a, b = _side(), _side()
    b["tables"]["payments_silver"]["digest"] = "2:ab:ce"
    assert compare(a, b)["tables"]["payments_silver"]["digest_equal"] is False
    c = _side()
    c["reconcile"]["logins"]["day"][0]["rows"] = 3
    got = compare(a, c)
    assert got["drift"] and got["statistics"]["logins/day"]["first_difference"] == "[0]/rows: 2 against 3"
