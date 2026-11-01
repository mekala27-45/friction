"""The serving check against a fake workspace: deploy, parity on the parity events, timing."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Any

import pytest
from friction_features import payments
from friction_workspace import serving


class FakeClient:
    """Enough of the SDK for serving.run: one alias, one warehouse, an endpoint that scores."""

    def __init__(self, vectors: dict[int, list[float]], off_by: float = 0.0, exists: bool = False) -> None:
        self.vectors, self.off_by, self.calls = vectors, off_by, []
        self.model_versions = NS(get_by_alias=lambda full_name, alias: NS(version=3))
        self.warehouses = NS(list=lambda: [NS(id="wh")])
        ready = NS(state=NS(ready=NS(value="READY"), config_update=NS(value="NOT_UPDATING")))
        self.serving_endpoints = NS(
            list=lambda: [NS(name="friction-payments")] if exists else [],
            create=lambda **kw: self.calls.append(("create", kw)),
            update_config=lambda **kw: self.calls.append(("update", kw)),
            get=lambda name: ready,
            query=self._query,
            delete=lambda name: self.calls.append(("delete", name)),
        )
        self.statement_execution = NS(execute_statement=self._execute)

    @staticmethod
    def score(v: dict[str, float]) -> float:
        return sum(v.values()) / 1000.0

    def _query(self, name: str, dataframe_records: list[dict[str, float]]) -> Any:
        return NS(predictions=[self.score(r) + self.off_by for r in dataframe_records])

    def _execute(self, statement: str, warehouse_id: str, wait_timeout: str) -> Any:
        assert "`foreign`" in statement and warehouse_id == "wh"
        from databricks.sdk.service.sql import StatementState

        cols = [NS(name="txn_ref"), *[NS(name=f) for f in payments.FEATURES]]
        rows = [[str(k), *map(str, v)] for k, v in sorted(self.vectors.items())]
        return NS(status=NS(state=StatementState.SUCCEEDED), statement_id="s", manifest=NS(schema=NS(columns=cols)),
                  result=NS(data_array=rows, next_chunk_index=None))  # fmt: skip


def _setup(tmp: Path) -> tuple[dict[int, list[float]], dict[str, Any]]:
    vectors = {r: [float(r % 7 + i) for i in range(len(payments.FEATURES))] for r in range(10, 40)}
    expected = [{"event_ref": r, "score": FakeClient.score(dict(zip(payments.FEATURES, v, strict=True)))}
                for r, v in vectors.items()]  # fmt: skip
    (tmp / "results").mkdir()
    (tmp / "results" / "parity.json").write_text(json.dumps({"expected": expected}))
    (tmp / "results" / "latency.json").write_text(json.dumps({"at": "x", "payments": {"p99_ms": 193}}))
    params = {"model": "workspace.friction_dev_ml.payments_lightgbm", "endpoint": "friction-payments",
              "parity": "results/parity.json", "features_table": "workspace.friction_dev_gold.payments_features",
              "queries": 5, "batch": 8}  # fmt: skip
    return vectors, params


def test_the_endpoint_serves_the_alias_and_agrees_on_every_parity_event(tmp_path: Path) -> None:
    vectors, params = _setup(tmp_path)
    client = FakeClient(vectors)
    result = serving.run(client, params, tmp_path, tmp_path)
    record = json.loads((tmp_path / "serving.json").read_text())
    assert result["parity_equal"] == 30 and record["parity"]["differ"] == 0 and record["version"] == "3"
    assert record["deploy"]["action"] == "created" and record["latency_single_row"]["n"] == 5
    assert record["vercel_payments"]["p99_ms"] == 193
    assert client.calls[0][0] == "create" and not any(c[0] == "delete" for c in client.calls)


def test_a_score_off_in_the_ninth_digit_fails_and_an_existing_endpoint_is_updated(tmp_path: Path) -> None:
    vectors, params = _setup(tmp_path)
    client = FakeClient(vectors, off_by=1e-6, exists=True)
    with pytest.raises(RuntimeError, match="serving parity"):
        serving.run(client, {**params, "keep": False}, tmp_path, tmp_path)
    assert client.calls[0][0] == "update" and ("delete", "friction-payments") in client.calls


def test_percentiles_and_the_sql_refuse_what_they_should() -> None:
    p = serving.percentiles([float(i) for i in range(1, 101)])
    assert (p["p50_ms"], p["p99_ms"], p["n"]) == (50.0, 99.0, 100)
    assert serving.percentiles([]) == {"n": 0}
    with pytest.raises(ValueError):
        serving.feature_sql("t", "txn_ref", ["a"], ["1; DROP TABLE t"])  # type: ignore[list-item]
