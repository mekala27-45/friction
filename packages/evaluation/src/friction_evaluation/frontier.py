"""The friction frontier: capture against friction across thresholds, with the policy applied.

For a set of scores the frontier is traced by one step up threshold sweeping the score's quantiles,
with the decline threshold held where declines of good customers use their own budget. The
operating point rule, registered in evaluation/plan.yaml before the test months were scored:
on validation, take the lowest decline threshold whose good declines stay within the decline
budget, then the lowest step up threshold whose good step ups stay within the step up budget;
those two thresholds go to test unchanged.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
from friction_policy.policy import DECLINE, STEP_UP, Thresholds

from friction_evaluation.metrics import Outcome, outcome

Decide = Callable[[npt.NDArray[np.float64], Thresholds], npt.NDArray[np.int8]]


@dataclass(frozen=True)
class Budget:
    step_up_per_1000: float
    decline_per_1000: float


def _lowest_within(
    scores: npt.NDArray[np.float64], good: npt.NDArray[np.bool_], eligible: npt.NDArray[np.bool_], per_1000: float
) -> float:
    """The lowest threshold t such that good eligible events with score >= t are within the budget."""
    allowed = int(np.floor(per_1000 / 1000.0 * good.sum()))
    candidates = np.sort(scores[good & eligible])[::-1]
    if allowed >= len(candidates):
        return float(np.min(scores)) if len(scores) else 0.0
    # Strictly above the (allowed + 1)th highest good score; ties go to approve.
    return float(np.nextafter(candidates[allowed], np.inf))


def choose(
    scores: npt.NDArray[np.float64],
    is_fraud: npt.NDArray[np.bool_],
    channels: npt.NDArray[np.int_] | None,
    budget: Budget,
) -> Thresholds:
    good = ~is_fraud
    everyone = np.ones(len(scores), dtype=bool)
    decline = _lowest_within(scores, good, everyone, budget.decline_per_1000)
    steppable = everyone if channels is None else channels == 2
    # Step ups are counted on events below the decline threshold: a declined event is not stepped up.
    below = scores < decline
    step = _lowest_within(scores, good, steppable & below, budget.step_up_per_1000)
    return Thresholds(step_up=min(step, decline), decline=decline)


def trace(
    scores: npt.NDArray[np.float64],
    is_fraud: npt.NDArray[np.bool_],
    amount_usd: npt.NDArray[np.float64],
    decide: Decide,
    decline: float,
    points: int = 60,
) -> list[tuple[float, Outcome]]:
    """(step up threshold, outcome) pairs from strict to lenient, decline threshold fixed."""
    qs = np.unique(np.quantile(scores, np.linspace(0.5, 1.0, points)))[::-1]
    out = []
    for t in qs:
        step = float(min(t, decline))
        dec = decide(scores, Thresholds(step_up=step, decline=decline))
        out.append((step, outcome(dec, is_fraud, amount_usd)))
    return out


def counts_by_verdict(decisions: npt.NDArray[np.int8]) -> dict[str, int]:
    return {
        "approve": int((decisions == 0).sum()),
        "step_up": int((decisions == STEP_UP).sum()),
        "decline": int((decisions == DECLINE).sum()),
    }
