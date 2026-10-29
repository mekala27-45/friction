"""The learned models: logistic regression on standardized features, and LightGBM.

Class weights on the training rows only, recorded in the model card; validation and test are never
resampled or reweighted. LightGBM runs single threaded and deterministic so the same training set
gives the same model on any machine, which the rederive checks.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import numpy.typing as npt

LGBM_PARAMS: dict[str, Any] = {
    "objective": "binary",
    "learning_rate": 0.05,
    "num_leaves": 31,
    "min_data_in_leaf": 200,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq": 1,
    "lambda_l2": 1.0,
    "seed": 20261029,
    "deterministic": True,
    "force_row_wise": True,
    "num_threads": 1,
    "verbose": -1,
}
ROUNDS = 400


def positive_weight(y: npt.NDArray[np.int_]) -> float:
    pos = float(y.sum())
    return float((len(y) - pos) / pos) if pos else 1.0


@dataclass
class Logistic:
    mean: npt.NDArray[np.float64]
    scale: npt.NDArray[np.float64]
    coef: npt.NDArray[np.float64]
    intercept: float

    @classmethod
    def fit(cls, x: npt.NDArray[np.float64], y: npt.NDArray[np.int_], c: float = 1.0) -> Logistic:
        from sklearn.linear_model import LogisticRegression

        mean = x.mean(axis=0)
        scale = x.std(axis=0)
        scale[scale == 0] = 1.0
        z = (x - mean) / scale
        model = LogisticRegression(C=c, class_weight="balanced", max_iter=500, solver="lbfgs")
        model.fit(z, y)
        return cls(mean=mean, scale=scale, coef=model.coef_[0].astype(np.float64), intercept=float(model.intercept_[0]))

    def margin(self, x: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return np.asarray(((x - self.mean) / self.scale) @ self.coef + self.intercept, dtype=np.float64)

    def predict(self, x: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return np.asarray(1.0 / (1.0 + np.exp(-self.margin(x))), dtype=np.float64)

    def contributions(self, x: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """Per feature contribution to the margin, the linear analogue of SHAP values."""
        return np.asarray(((x - self.mean) / self.scale) * self.coef, dtype=np.float64)

    def to_dict(self) -> dict[str, Any]:
        return {
            "mean": self.mean.tolist(),
            "scale": self.scale.tolist(),
            "coef": self.coef.tolist(),
            "intercept": self.intercept,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Logistic:
        return cls(np.array(d["mean"]), np.array(d["scale"]), np.array(d["coef"]), float(d["intercept"]))


@dataclass
class Gbm:
    booster: Any

    @classmethod
    def fit(
        cls,
        x: npt.NDArray[np.float64],
        y: npt.NDArray[np.int_],
        names: list[str],
        weight: npt.NDArray[np.float64] | None = None,
    ) -> Gbm:
        import lightgbm as lgb

        params = {**LGBM_PARAMS, "scale_pos_weight": positive_weight(y) if weight is None else 1.0}
        data = lgb.Dataset(x, label=y, weight=weight, feature_name=names, free_raw_data=True)
        return cls(lgb.train(params, data, num_boost_round=ROUNDS))

    def predict(self, x: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        return np.asarray(self.booster.predict(x), dtype=np.float64)

    def contributions(self, x: npt.NDArray[np.float64]) -> npt.NDArray[np.float64]:
        """TreeSHAP contributions from LightGBM itself; the last column is the expected value."""
        return np.asarray(self.booster.predict(x, pred_contrib=True), dtype=np.float64)[:, :-1]

    def to_text(self) -> str:
        return str(self.booster.model_to_string())

    @classmethod
    def from_text(cls, text: str) -> Gbm:
        import lightgbm as lgb

        return cls(lgb.Booster(model_str=text))
