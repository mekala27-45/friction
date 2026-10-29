"""The integration job's choice of drops and its comparison of digests."""

from __future__ import annotations

import pytest
from friction_transforms import integration as it

NAMES = ["0001_1991-01", "0002_1991-02", "0348_2019-11-29", "0349_2019-11-30", "0350_2019-12-01"]


def test_replay_days_count_only_daily_drops() -> None:
    assert it.daily_drops(NAMES) == ["0348_2019-11-29", "0349_2019-11-30", "0350_2019-12-01"]
    assert it.day_drop(NAMES, 2) == "0349_2019-11-30"
    with pytest.raises(ValueError):
        it.day_drop(NAMES, 4)


def test_feeds_follow_their_desk_by_date() -> None:
    labels = ["0054_1995-06", "0349_2019-11-30", "0351_2019-12-02"]
    assert it.same_date(labels, "0349_2019-11-30") == ["0349_2019-11-30"]
    assert it.same_date(labels, "0350_2019-12-01") == []


def test_differences_name_changed_and_missing_tables() -> None:
    a = {"x": {"digest": "1:a:b"}, "y": {"digest": "2:c:d"}}
    b = {"x": {"digest": "1:a:b"}, "y": {"digest": "2:c:e"}, "z": {"digest": "0:0:0"}}
    assert it.differences(a, a) == []
    assert it.differences(a, b) == ["y", "z"]


def test_the_guard_refuses_any_environment_but_integration() -> None:
    it.guard("friction_it")
    for prefix in ("friction_dev", "friction_prod"):
        with pytest.raises(SystemExit):
            it.guard(prefix)
