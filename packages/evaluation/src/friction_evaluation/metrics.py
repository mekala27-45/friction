"""The metrics the business uses, defined once.

capture: frauds declined or stepped up over all frauds, by count and by dollars.
friction: good events declined or stepped up per good event (printed per 1,000), and declines alone.
false positive rate: good events flagged over good events (the same quantity as friction, named
the way a model risk reviewer expects it).
alert precision: of the flagged events, the share that were fraud; computed over labelled
decisions only (the ledger rule: an event whose label is not known yet is not a false alarm) and
over all decisions, and both are printed.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import numpy.typing as npt
from friction_policy.policy import APPROVE, DECLINE


@dataclass(frozen=True)
class Outcome:
    frauds: int
    fraud_usd: float
    captured: int
    captured_usd: float
    goods: int
    goods_flagged: int
    goods_declined: int
    flagged: int
    flagged_labelled: int
    flagged_fraud: int

    @property
    def capture(self) -> float:
        return self.captured / self.frauds if self.frauds else float("nan")

    @property
    def capture_usd(self) -> float:
        return self.captured_usd / self.fraud_usd if self.fraud_usd else float("nan")

    @property
    def friction(self) -> float:
        return self.goods_flagged / self.goods if self.goods else float("nan")

    @property
    def decline_friction(self) -> float:
        return self.goods_declined / self.goods if self.goods else float("nan")

    @property
    def precision_labelled(self) -> float:
        return self.flagged_fraud / self.flagged_labelled if self.flagged_labelled else float("nan")

    @property
    def precision_all(self) -> float:
        return self.flagged_fraud / self.flagged if self.flagged else float("nan")


def outcome(
    decisions: npt.NDArray[np.int8],
    is_fraud: npt.NDArray[np.bool_],
    amount_usd: npt.NDArray[np.float64],
    labelled: npt.NDArray[np.bool_] | None = None,
) -> Outcome:
    flagged = decisions != APPROVE
    good = ~is_fraud
    lab = np.ones_like(is_fraud) if labelled is None else labelled
    usd = np.clip(amount_usd, 0, None)
    return Outcome(
        frauds=int(is_fraud.sum()),
        fraud_usd=float(usd[is_fraud].sum()),
        captured=int((flagged & is_fraud).sum()),
        captured_usd=float(usd[flagged & is_fraud].sum()),
        goods=int(good.sum()),
        goods_flagged=int((flagged & good).sum()),
        goods_declined=int(((decisions == DECLINE) & good).sum()),
        flagged=int(flagged.sum()),
        flagged_labelled=int((flagged & lab).sum()),
        flagged_fraud=int((flagged & is_fraud).sum()),
    )
