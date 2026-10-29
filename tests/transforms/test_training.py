"""Training sets read labels as known at the cut: the two clocks test."""

from __future__ import annotations

import pandas as pd
import pytest

pytestmark = pytest.mark.spark


def _history(spark):  # type: ignore[no-untyped-def]
    t = pd.Timestamp
    rows = [
        # Known before the cut: in the training set as fraud.
        (1, True, {"reported_at": t("2018-10-20").to_pydatetime(), "seq": 1}, None),
        # Known one day after the cut: absent from a training set cut on 1 November.
        (2, True, {"reported_at": t("2018-11-02").to_pydatetime(), "seq": 2}, None),
        # Reported as fraud, reversed before the cut: good at the cut.
        (3, True, {"reported_at": t("2018-09-01").to_pydatetime(), "seq": 3},
         {"reported_at": t("2018-10-01").to_pydatetime(), "seq": 4}),
        (3, False, {"reported_at": t("2018-10-01").to_pydatetime(), "seq": 4}, None),
    ]  # fmt: skip
    return spark.createDataFrame(
        rows,
        "event_ref long, is_fraud boolean, __START_AT struct<reported_at:timestamp,seq:bigint>, "
        "__END_AT struct<reported_at:timestamp,seq:bigint>",
    )


def test_a_label_known_after_the_cut_is_absent(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import training

    hist = _history(spark)
    at_cut = {r["event_ref"]: r["is_fraud"] for r in training.label_as_of(hist, "2018-11-01T00:00:00").collect()}
    assert at_cut == {1: True, 3: False}
    latest = {r["event_ref"]: r["is_fraud"] for r in training.label_as_of(hist, None).collect()}
    assert latest == {1: True, 2: True, 3: False}


def test_the_reversal_shows_on_the_system_clock(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import training

    hist = _history(spark)
    mid = {r["event_ref"]: r["is_fraud"] for r in training.label_as_of(hist, "2018-09-15T00:00:00").collect()}
    assert mid == {3: True}
