"""Approve, step up or decline, with the channel deciding what is possible.

A chip or swipe purchase is decided at the terminal: it can be approved or declined, never
challenged. An online purchase can be stepped up (a one time code). A login can be allowed,
stepped up (re-authentication) or blocked. Thresholds are on the model's score; a decline or block
threshold is never below the step up threshold.
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import numpy.typing as npt
from friction_core.model import StrictModel

Verdict = Literal["approve", "step_up", "decline"]
APPROVE, STEP_UP, DECLINE = 0, 1, 2
VERDICTS: tuple[Verdict, ...] = ("approve", "step_up", "decline")
ONLINE = 2


class Thresholds(StrictModel):
    step_up: float
    decline: float

    def model_post_init(self, _: object) -> None:
        if self.decline < self.step_up:
            raise ValueError("the decline threshold is below the step up threshold")


def payments(scores: npt.NDArray[np.float64], channels: npt.NDArray[np.int_], t: Thresholds) -> npt.NDArray[np.int8]:
    out = np.full(len(scores), APPROVE, dtype=np.int8)
    out[(scores >= t.step_up) & (channels == ONLINE)] = STEP_UP
    out[scores >= t.decline] = DECLINE
    return out


def logins(scores: npt.NDArray[np.float64], t: Thresholds) -> npt.NDArray[np.int8]:
    out = np.full(len(scores), APPROVE, dtype=np.int8)
    out[scores >= t.step_up] = STEP_UP
    out[scores >= t.decline] = DECLINE
    return out


def one(score: float, channel: int | None, t: Thresholds) -> Verdict:
    if score >= t.decline:
        return "decline"
    if score >= t.step_up and (channel is None or channel == ONLINE):
        return "step_up"
    return "approve"
