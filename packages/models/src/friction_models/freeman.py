"""The Freeman et al. (2016) risk based authentication model, written from the paper.

    score(x, u) = prod_k [ p(x_k) / p(x_k | u, legit) ] * p(u | attack) / p(u | legit)

(Freeman et al., NDSS 2016, Eq. 7, p. 3, without the reputation term p(A | x_k), which the
evaluated configuration of Wiefling et al. 2022 also leaves out.) Each probability is the
proportion of observed values with the smoothing of Eq. 8 (p. 4): c(x)/N * (1 - M/(N+M)) when x
was seen, 1/(N+M) when not, with M the number of distinct values seen (the choice the paper
settles on, p. 9). Each feature is a hierarchy combined by linear interpolation (Eq. 11, p. 5)
with the weights of Wiefling et al. (2022, Section 4): IP address 0.6, ASN 0.3, country 0.1; user
agent string 0.53, browser 0.27, operating system 0.19, device type 0.01. p(u | attack) is
uniform over users; p(u | legit) is the user's share of legitimate logins.

Global proportions come from the successful logins of the training months only and are frozen in
the model; the user's own counts come from the login state machine, as of the login. A user with
no successful login yet has no history to compare with, and the user terms are then left equal to
the global ones (a ratio of one).

One engineering choice the paper does not make, recorded in DECISIONS.md: the global tables for the
two levels with millions of values (IP address and the full browser string) keep only values seen
at least MIN_COUNT times; a rarer value is treated as unseen, 1/(N+M), with N and M still the
full totals. The lakehouse and the API score with the same pruned model, so their decisions agree.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import numpy.typing as npt
from friction_features import logins

IP_GROUP = (("ip", 0.6), ("asn", 0.3), ("country", 0.1))
UA_GROUP = (("ua", 0.53), ("browser", 0.27), ("os", 0.19), ("device", 0.01))
LEVEL = {name: i for i, name in enumerate(logins.LEVELS)}
F = {name: i for i, name in enumerate(logins.FEATURES)}


def smoothed(count: float, total: float, distinct: float) -> float:
    if total + distinct <= 0:
        return 1.0
    if count > 0:
        return count / total * (1.0 - distinct / (total + distinct))
    return 1.0 / (total + distinct)


MIN_COUNT = 3
PRUNED = ("ip", "ua")
# The feature each level's contribution is named by, so reason_codes.yaml maps it (L01, L03 ...).
CODE_FEATURES = [f"{lvl}_count" for lvl, _ in (*IP_GROUP, *UA_GROUP)]


@dataclass
class Freeman:
    """Global tables from the training months, and the score from a login's feature vector."""

    counts: dict[str, dict[str, int]] = field(default_factory=dict)
    totals: dict[str, int] = field(default_factory=dict)
    distinct: dict[str, int] = field(default_factory=dict)
    users: int = 0
    legit_logins: int = 0

    @classmethod
    def from_counts(
        cls, counts: dict[str, dict[str, int]], distinct: dict[str, int], legit_logins: int, users: int
    ) -> Freeman:
        """counts per level (pruned levels already pruned), with the full distinct counts and total."""
        return cls(
            counts=counts,
            totals=dict.fromkeys(counts, legit_logins),
            distinct=distinct,
            users=users,
            legit_logins=legit_logins,
        )

    @classmethod
    def fit(cls, rows: Iterable[tuple[tuple[str, ...], bool]], users: int) -> Freeman:
        """rows: (values per logins.LEVELS, success) for every login of the training months."""
        counters = {lvl: Counter[str]() for lvl, _ in (*IP_GROUP, *UA_GROUP)}
        n = 0
        for values, ok in rows:
            if not ok:
                continue
            n += 1
            for lvl in counters:
                counters[lvl][values[LEVEL[lvl]]] += 1
        distinct = {lvl: len(c) for lvl, c in counters.items()}
        kept = {lvl: {k: v for k, v in c.items() if lvl not in PRUNED or v >= MIN_COUNT} for lvl, c in counters.items()}
        return cls.from_counts(kept, distinct, n, users)

    def _global(self, lvl: str, value: str) -> float:
        table = self.counts.get(lvl, {})
        return smoothed(table.get(value, 0), self.totals.get(lvl, 0), self.distinct.get(lvl, len(table)))

    def log_score(self, values: tuple[str, ...], feats: npt.NDArray[np.float64] | list[float]) -> float:
        n_user = float(feats[F["n_success_prior"]])
        total = 0.0
        for group in (IP_GROUP, UA_GROUP):
            p_global = sum(w * self._global(lvl, values[LEVEL[lvl]]) for lvl, w in group)
            if n_user > 0:
                p_user = sum(
                    w * smoothed(float(feats[F[f"{lvl}_count"]]), n_user, float(feats[F[f"{lvl}_distinct"]]))
                    for lvl, w in group
                )
            else:
                p_user = p_global
            total += math.log(p_global) - math.log(p_user)
        p_attack = 1.0 / max(self.users, 1)
        p_legit = max(n_user, 1.0) / max(self.legit_logins, 1)
        return total + math.log(p_attack) - math.log(p_legit)

    def contributions(self, values: tuple[str, ...], feats: npt.NDArray[np.float64] | list[float]) -> list[float]:
        """One term per level, in CODE_FEATURES order: the level's weight times the log of its global
        over its user proportion. Positive where the value is rarer for this user than for everyone;
        the reason codes come from the largest of them."""
        n_user = float(feats[F["n_success_prior"]])
        out = []
        for group in (IP_GROUP, UA_GROUP):
            for lvl, w in group:
                g = self._global(lvl, values[LEVEL[lvl]])
                if n_user > 0:
                    u = smoothed(float(feats[F[f"{lvl}_count"]]), n_user, float(feats[F[f"{lvl}_distinct"]]))
                else:
                    u = g
                out.append(w * (math.log(g) - math.log(u)))
        return out

    def scores(self, values: list[tuple[str, ...]], feats: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return np.array([self.log_score(v, f) for v, f in zip(values, feats, strict=True)], dtype=np.float64)

    def to_json(self) -> str:
        return json.dumps(
            {
                "counts": self.counts,
                "totals": self.totals,
                "distinct": self.distinct,
                "users": self.users,
                "legit_logins": self.legit_logins,
            },
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, text: str) -> Freeman:
        d: Mapping[str, Any] = json.loads(text)
        return cls(
            counts=d["counts"],
            totals=d["totals"],
            distinct=d.get("distinct", {}),
            users=int(d["users"]),
            legit_logins=int(d["legit_logins"]),
        )
