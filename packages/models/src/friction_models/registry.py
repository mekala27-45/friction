"""Registration in Unity Catalog: every model of both desks, the floors included.

Unity Catalog takes only MLflow models that carry a signature, so each model file is wrapped in a
small pyfunc whose predict scores with the same code the lakehouse runs (score() below). The
packages travel with the model as code paths, so a registered version loads and scores on its own.
The floors are rules in code; they are registered as a JSON copy of their rule table with the
digest of the module that applies it. The API still serves the export (results/models); the
sha256 tag on each version is what ties a served file to its registered version.
"""

from __future__ import annotations

import hashlib
import inspect
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import pandas as pd
from friction_features import logins as login_features
from friction_features import payments as payment_features

from friction_models import floors
from friction_models.freeman import Freeman
from friction_models.learners import Gbm, Logistic

PAY = tuple(payment_features.FEATURES)
LOGIN = (*login_features.FEATURES, *login_features.IP_FEATURES)
VALUES = tuple(f"v_{lvl}" for lvl in login_features.LEVELS)
PIP = ["lightgbm==4.7.0", "scikit-learn==1.9.1", "numpy>=2.0", "pandas>=2.2,<3"]


@dataclass(frozen=True)
class Spec:
    name: str  # the registered model's name in the ml schema
    desk: str
    family: str  # floor, logistic, lightgbm or freeman
    file: str  # under models/<tag>/; the floors' files are written by write_floors()
    doubles: tuple[str, ...]
    strings: tuple[str, ...] = ()


SPECS: tuple[Spec, ...] = (
    Spec("payments_floor", "payments", "floor", "payments_floor.json", PAY),
    Spec("payments_logistic", "payments", "logistic", "payments_logistic.json", PAY),
    Spec("payments_lightgbm", "payments", "lightgbm", "payments_lightgbm.txt", PAY),
    Spec("logins_floor", "logins", "floor", "logins_floor.json", tuple(login_features.FEATURES)),
    Spec("logins_freeman", "logins", "freeman", "logins_freeman.json", tuple(login_features.FEATURES), VALUES),
    Spec("logins_lightgbm", "logins", "lightgbm", "logins_lightgbm.txt", LOGIN),
)
BY_NAME = {s.name: s for s in SPECS}


def plan_hash(root: Path) -> str:
    """The registered plan's digest, as evaluation/plan.lock holds it; every version is tagged with it."""
    return str(json.loads((root / "evaluation" / "plan.lock").read_text(encoding="utf-8"))["sha256"])


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def floor_document(desk: str) -> dict[str, Any]:
    """The floor's rule table as data, with the digest of the module that applies it."""
    rules = floors.PAYMENT_RULES if desk == "payments" else floors.LOGIN_RULES
    source = inspect.getsource(floors).encode("utf-8")
    return {
        "desk": desk,
        "rules": [{"code": c, "points": p, "description": d} for c, p, d in rules],
        "module": "friction_models.floors",
        "module_sha256": hashlib.sha256(source).hexdigest(),
    }


def write_floors(folder: Path) -> None:
    for desk in ("payments", "logins"):
        doc = json.dumps(floor_document(desk), indent=1, sort_keys=True) + "\n"
        (folder / f"{desk}_floor.json").write_text(doc, encoding="utf-8")


def score(spec: Spec, path: Path, frame: pd.DataFrame) -> npt.NDArray[np.float64]:
    """The model's score for each row, as the score job computes it."""
    x = frame[list(spec.doubles)].astype(float).to_numpy(np.float64)
    text = path.read_text(encoding="utf-8")
    if spec.family == "floor":
        return floors.payment_scores(x) if spec.desk == "payments" else floors.login_scores(x)
    if spec.family == "logistic":
        return Logistic.from_dict(json.loads(text)).predict(x)
    if spec.family == "lightgbm":
        return Gbm.from_text(text).predict(x)
    values = list(zip(*[frame[c].astype(str) for c in spec.strings], strict=True))
    return Freeman.from_json(text).scores(values, x)


def code_paths(root: Path) -> list[str]:
    """The packages score() imports, copied into each registered model."""
    return [str(root / "packages" / p / "src" / f"friction_{p}") for p in ("models", "features")]


def register(
    folder: Path, catalog: str, schema: str, experiment: str, root: Path, tags: dict[str, str]
) -> dict[str, dict[str, Any]]:
    """Log and register every model in folder; the learned ones take the alias challenger, the floors
    the alias floor. Promotion to champion is a separate request (promote-a-model skill)."""
    import mlflow
    from mlflow.models import ModelSignature
    from mlflow.types import ColSpec, Schema

    class Wrapped(mlflow.pyfunc.PythonModel):  # type: ignore[misc]
        def __init__(self, name: str) -> None:
            self.name = name

        def load_context(self, context: Any) -> None:
            self.path = Path(context.artifacts["model"])

        def predict(self, context: Any, model_input: pd.DataFrame, params: Any = None) -> Any:
            return score(BY_NAME[self.name], self.path, model_input)

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment)
    client = mlflow.MlflowClient()
    out: dict[str, dict[str, Any]] = {}
    for spec in SPECS:
        path = folder / spec.file
        inputs = [ColSpec("double", c) for c in spec.doubles] + [ColSpec("string", c) for c in spec.strings]
        signature = ModelSignature(inputs=Schema(inputs), outputs=Schema([ColSpec("double", "score")]))
        version_tags = {**tags, "desk": spec.desk, "family": spec.family, "sha256": sha256_file(path)}
        with mlflow.start_run(run_name=f"friction-{spec.name}"):
            mlflow.set_tags(version_tags)
            info = mlflow.pyfunc.log_model(
                name="model", python_model=Wrapped(spec.name), artifacts={"model": str(path)},
                code_paths=code_paths(root), signature=signature, pip_requirements=PIP,
            )  # fmt: skip
        full = f"{catalog}.{schema}.{spec.name}"
        mv = mlflow.register_model(info.model_uri, full, tags=version_tags)
        alias = "floor" if spec.family == "floor" else "challenger"
        client.set_registered_model_alias(full, alias, mv.version)
        out[full] = {"version": str(mv.version), "alias": alias, "sha256": version_tags["sha256"]}
    return out
