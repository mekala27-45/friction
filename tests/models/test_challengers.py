"""The graph challengers: their codes, their scoring, and training on the champion's own rows."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from friction_features import graph
from friction_models import challengers, steps
from friction_models.learners import Gbm

ROOT = Path(__file__).resolve().parents[2]
LAKE = ROOT / "data" / "lake" / "fixture" / "spark-warehouse"


def test_graph_codes_extend_the_table_without_touching_a_v1_code() -> None:
    maps = challengers.mappings(ROOT)
    for desk, cols in (("payments", graph.PAYMENT_GRAPH), ("logins", graph.LOGIN_GRAPH)):
        assert set(cols) <= set(maps[desk])
        assert {maps[desk][c] for c in cols}.isdisjoint({maps[desk][c] for c in maps[desk] if c not in cols})
    for desk, models in challengers.MODELS.items():
        for cols in models.values():
            assert set(cols) <= set(maps[desk]), desk
            assert len(cols) == len(set(cols))
    assert set(challengers.MODELS["payments"]["lightgbm_structure"]).isdisjoint(graph.PAYMENT_KNOWN_FRAUD)


def _fitted(tmp: Path, desk: str, n: int = 4000) -> pd.DataFrame:
    """Small models on random rows where the label follows the last graph feature."""
    rng = np.random.default_rng(7)
    cols = sorted({c for m in challengers.MODELS[desk].values() for c in m})
    frame = pd.DataFrame(rng.normal(size=(n, len(cols))), columns=cols)
    frame[challengers.KEY[desk]] = np.arange(n, dtype=np.int64)
    driver = graph.PAYMENT_STRUCTURE[-1] if desk == "payments" else graph.LOGIN_GRAPH[-1]
    y = (frame[driver] + 0.1 * rng.normal(size=n) > 1.2).astype(int).to_numpy()
    for name, features in challengers.MODELS[desk].items():
        model = Gbm.fit(frame[features].to_numpy(np.float64), y, features)
        (tmp / challengers.file_name(desk, name)).write_text(model.to_text(), encoding="utf-8")
    return frame


@pytest.mark.parametrize("desk", ["payments", "logins"])
def test_scores_and_codes_come_from_each_challengers_own_columns(tmp_path: Path, desk: str) -> None:
    frame = _fitted(tmp_path, desk)
    scored = challengers.score_frame(frame, tmp_path, desk)
    assert list(scored.columns) == [challengers.KEY[desk], *[f"score_{m}" for m in challengers.MODELS[desk]]]
    both = frame.merge(scored, on=challengers.KEY[desk])
    cut = {m: float(np.quantile(scored[f"score_{m}"], 0.9)) for m in challengers.MODELS[desk]}
    coded = challengers.code_frame(both, tmp_path, desk, challengers.mappings(ROOT)[desk], cut)
    for m in challengers.MODELS[desk]:
        above = both[f"score_{m}"].to_numpy() >= cut[m]
        codes = coded[f"codes_{m}"].to_numpy()
        assert all(codes[above]) and not any(codes[~above])
        top = pd.Series([c.split(",")[0] for c in codes[above]]).value_counts().index[0]
        assert top == ("P14" if desk == "payments" else "L14"), m


@pytest.mark.spark
def test_challengers_train_on_the_champions_rows_and_score_every_event(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    if not (LAKE / "payments_graph_features").exists():
        pytest.skip("needs the fixture lake: scripts/run_portable.py then scripts/run_features.py")
    wh = tmp_path / "wh"
    wh.mkdir()

    def read(name: str):  # type: ignore[no-untyped-def]
        local = wh / name
        return spark.read.parquet(str(local if local.exists() else LAKE / name))

    def write(name: str, df) -> None:  # type: ignore[no-untyped-def]
        df.write.mode("overwrite").parquet(str(wh / name))

    from friction_transforms import gold

    plan = steps.load_plan(ROOT)
    write("login_attacks", spark.createDataFrame(steps.attacks_step(spark, read, plan)))
    gold.features_step(spark, read, write, attacks=read("login_attacks"), source=read)
    v1 = steps.train_step(spark, read, plan, tmp_path / "models")
    v2 = challengers.train_step(spark, read, plan, tmp_path / "models")
    for desk in ("payments", "logins"):
        assert v2[desk]["rows"] == v1[desk]["rows"]
        assert v2[desk]["positives"] == v1[desk]["positives"]
        shares = v2[desk]["models"]["lightgbm_graph"]["gain_share"]
        assert sum(shares.values()) == pytest.approx(1.0, abs=1e-5)
    steps.score_step(spark, read, plan, tmp_path / "models", tmp_path / "scored", ROOT)
    challengers.score_step(spark, read, plan, tmp_path / "models", tmp_path / "scored_v2", ROOT)
    from scripts import grade_v2

    addendum = __import__("yaml").safe_load((ROOT / "evaluation" / "plan_v2.yaml").read_text(encoding="utf-8"))
    for name, desk in grade_v2.desks_v2(plan, addendum).items():
        for split in ("validation", "test"):
            joined = grade_v2.joined(tmp_path / "scored", tmp_path / "scored_v2", desk, split)
            assert joined[[f"score_{m}" for m in desk.models]].notna().all().all(), (name, split)
    summary = json.loads((tmp_path / "models" / "v2" / "summary.json").read_text(encoding="utf-8"))
    assert summary["tag"] == "v2"
    shutil.rmtree(wh)
