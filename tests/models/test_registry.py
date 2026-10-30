"""Registration: every model of both desks has a spec, a signature and the score job's score."""

from __future__ import annotations

import json
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml
from friction_features import logins
from friction_models import floors, registry
from friction_models.freeman import Freeman
from friction_models.learners import Gbm, Logistic

from tests.conftest import ROOT


def close(a: np.ndarray, b: np.ndarray) -> None:
    """Equal to the last bits a matrix product's memory layout can move; parity allows 1e-9."""
    np.testing.assert_allclose(a, b, rtol=0, atol=1e-12)


def test_every_planned_model_of_both_desks_is_registered() -> None:
    plan = yaml.safe_load((ROOT / "evaluation" / "plan.yaml").read_text(encoding="utf-8"))
    for desk in ("payments", "logins"):
        planned = set(plan["desks"][desk]["models"])
        assert {s.family for s in registry.SPECS if s.desk == desk} == planned


def test_floor_document_holds_the_rules_and_the_module_digest(tmp_path: Path) -> None:
    registry.write_floors(tmp_path)
    doc = json.loads((tmp_path / "payments_floor.json").read_text(encoding="utf-8"))
    assert [r["code"] for r in doc["rules"]] == [c for c, _, _ in floors.PAYMENT_RULES]
    assert len(doc["module_sha256"]) == 64
    assert json.loads((tmp_path / "logins_floor.json").read_text(encoding="utf-8"))["desk"] == "logins"


def test_registered_scores_equal_the_score_job(tmp_path: Path) -> None:
    rng = np.random.default_rng(7)
    n = 400
    pay = pd.DataFrame(rng.normal(size=(n, len(registry.PAY))), columns=list(registry.PAY))
    y = (pay.iloc[:, 0] + rng.normal(scale=0.5, size=n) > 1).astype(int).to_numpy()
    x = pay.to_numpy(np.float64)
    logistic = Logistic.fit(x, y)
    (tmp_path / "payments_logistic.json").write_text(json.dumps(logistic.to_dict()), encoding="utf-8")
    gbm = Gbm.fit(x, y, list(registry.PAY))
    (tmp_path / "payments_lightgbm.txt").write_text(gbm.to_text(), encoding="utf-8")
    registry.write_floors(tmp_path)
    by = registry.BY_NAME
    close(registry.score(by["payments_logistic"], tmp_path / "payments_logistic.json", pay),
                          Logistic.from_dict(logistic.to_dict()).predict(x))  # fmt: skip
    close(registry.score(by["payments_lightgbm"], tmp_path / "payments_lightgbm.txt", pay),
                          Gbm.from_text(gbm.to_text()).predict(x))  # fmt: skip
    close(registry.score(by["payments_floor"], tmp_path / "payments_floor.json", pay),
                          floors.payment_scores(x))  # fmt: skip

    feats = pd.DataFrame(
        rng.integers(0, 3, size=(n, len(logins.FEATURES))).astype(float), columns=list(logins.FEATURES)
    )
    values = pd.DataFrame({c: rng.choice(["a", "b", "c"], size=n) for c in registry.VALUES})
    frame = pd.concat([feats, values], axis=1)
    counts = {lvl: {"a": 5, "b": 3} for lvl in logins.LEVELS}
    freeman = Freeman.from_counts(counts, {lvl: 3 for lvl in logins.LEVELS}, 100, 10)
    (tmp_path / "logins_freeman.json").write_text(freeman.to_json(), encoding="utf-8")
    want = freeman.scores(list(zip(*[values[c] for c in registry.VALUES], strict=True)), feats.to_numpy(np.float64))
    close(registry.score(by["logins_freeman"], tmp_path / "logins_freeman.json", frame), want)
    close(registry.score(by["logins_floor"], tmp_path / "logins_floor.json", frame),
                          floors.login_scores(feats.to_numpy(np.float64)))  # fmt: skip


def test_code_paths_point_at_the_packages_score_imports() -> None:
    for p in registry.code_paths(ROOT):
        assert (Path(p) / "__init__.py").exists()
    assert registry.plan_hash(ROOT) == json.loads((ROOT / "evaluation" / "plan.lock").read_text())["sha256"]


def test_register_logs_each_model_with_a_signature_and_sets_the_aliases(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Against a stand in for MLflow: the calls Unity Catalog needs, in order, for every spec."""
    calls: dict[str, list[Any]] = {"log": [], "register": [], "alias": []}

    class Run:
        def __enter__(self) -> Run:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

    class Client:
        def set_registered_model_alias(self, name: str, alias: str, version: str) -> None:
            calls["alias"].append((name, alias, version))

    def log_model(**kw: Any) -> SimpleNamespace:
        calls["log"].append(kw)
        return SimpleNamespace(model_uri=f"models:/m-{len(calls['log'])}")

    def register_model(uri: str, name: str, tags: dict[str, str]) -> SimpleNamespace:
        calls["register"].append((uri, name, tags))
        return SimpleNamespace(version=str(len(calls["register"])))

    fake = types.ModuleType("mlflow")
    fake.pyfunc = SimpleNamespace(PythonModel=object, log_model=log_model)  # type: ignore[attr-defined]
    fake.set_registry_uri = lambda uri: None  # type: ignore[attr-defined]
    fake.set_experiment = lambda name: None  # type: ignore[attr-defined]
    fake.start_run = lambda run_name: Run()  # type: ignore[attr-defined]
    fake.set_tags = lambda tags: None  # type: ignore[attr-defined]
    fake.MlflowClient = Client  # type: ignore[attr-defined]
    fake.register_model = register_model  # type: ignore[attr-defined]
    models = types.ModuleType("mlflow.models")
    models.ModelSignature = lambda inputs, outputs: (inputs, outputs)  # type: ignore[attr-defined]
    mtypes = types.ModuleType("mlflow.types")
    mtypes.ColSpec = lambda t, name: (t, name)  # type: ignore[attr-defined]
    mtypes.Schema = list  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "mlflow", fake)
    monkeypatch.setitem(sys.modules, "mlflow.models", models)
    monkeypatch.setitem(sys.modules, "mlflow.types", mtypes)

    for spec in registry.SPECS:
        (tmp_path / spec.file).write_text("{}", encoding="utf-8")
    out = registry.register(tmp_path, "workspace", "friction_dev_ml", "/Users/x/exp", ROOT, {"plan_sha256": "p"})
    assert len(out) == len(registry.SPECS) == len(calls["log"])
    aliases = {name.rsplit(".", 1)[1]: alias for name, alias, _ in calls["alias"]}
    assert aliases == {s.name: ("floor" if s.family == "floor" else "challenger") for s in registry.SPECS}
    freeman = next(kw for kw in calls["log"] if kw["python_model"].name == "logins_freeman")
    inputs, outputs = freeman["signature"]
    assert ("string", "v_country") in inputs and outputs == [("double", "score")]
    assert all(tags["plan_sha256"] == "p" and len(tags["sha256"]) == 64 for _, _, tags in calls["register"])
