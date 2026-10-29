"""A bundle refuses a file whose digest changed; a desk without a champion serves its floor."""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.api.conftest import write_bundle


def test_a_changed_file_refuses_to_load(tmp_path: Path) -> None:
    from friction_api.bundle import Bundle, BundleError

    folder = write_bundle(tmp_path / "logins", "logins", "floor", {"unknown_accounts.json": b"[]"},
                          {"step_up": 1.0, "decline": 4.0})  # fmt: skip
    assert Bundle.load(folder).model == "floor"
    (folder / "unknown_accounts.json").write_bytes(b'["u_x"]')
    with pytest.raises(BundleError):
        Bundle.load(folder)


def test_a_desk_without_a_champion_serves_the_floor() -> None:
    from friction_models.export import served_model

    assert served_model({"champion": {"model": None}})[0] == "floor"
    assert served_model({"champion": {"model": "lightgbm"}}) == ("lightgbm", "champion")


def test_floor_bundle_scores_and_names_the_rule(tmp_path: Path) -> None:
    import numpy as np
    from friction_api.bundle import Bundle
    from friction_features import payments

    folder = write_bundle(tmp_path / "payments", "payments", "floor", {"mcc_rates.json": b"{}"},
                          {"step_up": 1.0, "decline": 4.0})  # fmt: skip
    b = Bundle.load(folder)
    row = np.zeros(len(payments.FEATURES))
    row[payments.FEATURES.index("cnt_1h")] = 4
    row[payments.FEATURES.index("days_since_issue")] = 50
    score, codes = b.score(list(row))
    assert codes == ["P03"] and score >= 2.0
    assert b.decide(score, 2) == "step_up" and b.decide(score, 1) == "approve"
