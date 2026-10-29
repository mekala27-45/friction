"""The exported champion of each desk, loaded once, and the scoring of one event.

results/models/<desk>/bundle.json names the model, its files with their digests, the operating
point thresholds chosen on validation, and the reference tables the features need (merchant
category fraud rates by month on the payments desk; the unknown account keys on the login desk).
A file whose digest differs from the bundle refuses to load. The same functions score the parity
replay offline and the live requests online, so a difference between them is a difference in
state, never in code.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
from friction_core.hashing import sha256_bytes
from friction_features import logins as login_features
from friction_features import payments as payment_features
from friction_models import floors
from friction_models.freeman import CODE_FEATURES, Freeman
from friction_models.learners import Gbm, Logistic
from friction_policy import policy
from friction_policy.policy import Thresholds
from friction_reasons.reasons import top_codes

PAY_FEATURES = list(payment_features.FEATURES)
LOGIN_FEATURES = [*login_features.FEATURES, *login_features.IP_FEATURES]
LOGIN_FEATURES_REP = [*LOGIN_FEATURES, "is_attack_ip"]


class BundleError(RuntimeError):
    pass


@dataclass
class Bundle:
    desk: str
    model: str
    status: str
    thresholds: Thresholds
    files: dict[str, str]
    mapping: dict[str, str]
    digest: str
    learner: Any = None
    freeman: Freeman | None = None
    mcc_rates: dict[str, dict[str, float]] = field(default_factory=dict)
    unknown_accounts: frozenset[str] = frozenset()

    @classmethod
    def load(cls, folder: Path) -> Bundle:
        meta: dict[str, Any] = json.loads((folder / "bundle.json").read_text(encoding="utf-8"))
        for name, digest in meta["files"].items():
            got = sha256_bytes((folder / name).read_bytes())
            if got != digest:
                raise BundleError(f"{folder.name}/{name} has digest {got[:12]}, the bundle says {digest[:12]}")
        b = cls(
            desk=meta["desk"],
            model=meta["model"],
            status=meta["status"],
            thresholds=Thresholds(**meta["thresholds"]),
            files=meta["files"],
            mapping=meta["reason_map"],
            digest=sha256_bytes(json.dumps(meta, sort_keys=True).encode()),
        )
        kind = meta["model"]
        if kind in ("lightgbm", "lightgbm_rep"):
            b.learner = Gbm.from_text((folder / "model.txt").read_text(encoding="utf-8"))
        elif kind == "logistic":
            b.learner = Logistic.from_dict(json.loads((folder / "model.json").read_text(encoding="utf-8")))
        elif kind == "freeman":
            b.freeman = Freeman.from_json((folder / "model.json").read_text(encoding="utf-8"))
        elif kind != "floor":
            raise BundleError(f"unknown model {kind!r}")
        if "mcc_rates.json" in meta["files"]:
            b.mcc_rates = json.loads((folder / "mcc_rates.json").read_text(encoding="utf-8"))
        if "unknown_accounts.json" in meta["files"]:
            b.unknown_accounts = frozenset(json.loads((folder / "unknown_accounts.json").read_text(encoding="utf-8")))
        return b

    def mcc_rate(self, month: str, mcc: int) -> float:
        return float(self.mcc_rates.get(month, {}).get(str(mcc), 0.0))

    def score(self, x: list[float], values: tuple[str, ...] | None = None) -> tuple[float, list[str]]:
        """The model's score and its reason codes (computed for every event; carried when flagged)."""
        row = np.asarray([x], dtype=np.float64)
        if self.model == "floor":
            if self.desk == "payments":
                return float(floors.payment_scores(row)[0]), floors.payment_fired(row[0])
            return float(floors.login_scores(row)[0]), floors.login_fired(row[0])
        if self.model == "freeman":
            assert self.freeman is not None and values is not None
            feats = row[0][: len(login_features.FEATURES)]
            contrib = np.asarray(self.freeman.contributions(values, feats), dtype=np.float64)
            return float(self.freeman.log_score(values, feats)), top_codes(contrib, CODE_FEATURES, self.mapping)
        names = self.names()
        contrib = self.learner.contributions(row)[0]
        return float(self.learner.predict(row)[0]), top_codes(contrib, names, self.mapping)

    def names(self) -> list[str]:
        if self.desk == "payments":
            return PAY_FEATURES
        return LOGIN_FEATURES_REP if self.model == "lightgbm_rep" else LOGIN_FEATURES

    def decide(self, score: float, channel: int | None) -> str:
        return policy.one(score, channel if self.desk == "payments" else None, self.thresholds)
