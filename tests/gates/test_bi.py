"""The BI files: CSV with headers, a semantic model and a Tableau source carrying every measure, and the
measures computed from the extract equal to the graded point estimates when the rows are the graded rows."""

from __future__ import annotations

import json
import random
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest
import yaml

from scripts import build_bi

ROOT = Path(__file__).resolve().parents[2]


def _raw(tmp: Path) -> Path:
    rng = random.Random(5)
    raw = tmp / "raw"
    raw.mkdir()
    rows = []
    for day in ("2019-06-30", "2019-07-01", "2019-08-15", "2019-10-31", "2019-11-01"):
        for channel in (0, 2):
            for verdict in ("approve", "step_up", "decline"):
                n = rng.randint(5, 900)
                frauds = rng.randint(0, n // 4)
                rows.append([day, str(channel), verdict, str(n), str(n * 3100), str(frauds), str(frauds * 9000)])
    docs = {
        "decisions_daily": rows,
        "codes_daily": [["2019-07-01", "decline", "P01", "3"]],
        "reason_codes": [["payments", "P01", "the amount is far above", "amount_usd", "reason_codes.yaml"]],
        "quarantine": [["payments", "P_KEY_CONFLICT", "12"]],
    }
    for name, data in docs.items():
        cols = [c for c, _ in build_bi.TABLES[name]]
        (raw / f"bi_{name}.json").write_text(json.dumps({"columns": cols, "rows": data}))
    return raw


def _graded(rows: list[dict[str, object]]) -> dict[str, object]:
    s = build_bi.sums(rows, "2019-07-01", "2019-11-01")  # type: ignore[arg-type]
    test = {"capture_usd": {"estimate": s["fraud_cents__flagged"] / s["fraud_cents"]},
            "step_ups_per_1000": {"estimate": 1000 * s["goods__step_up"] / s["goods"]},
            "declines_per_1000": {"estimate": 1000 * s["goods__decline"] / s["goods"]}}  # fmt: skip
    return {"desks": {"payments": {"models": {"lightgbm": {"test": test}}}}}


def test_the_files_carry_every_measure_and_the_checks_hold(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    spec = yaml.safe_load((ROOT / "dashboards" / "measures.yaml").read_text())
    data = build_bi.load(raw)
    out = tmp_path / "bi"
    build_bi.powerbi(out, spec)
    tds = build_bi.tableau(out, spec)
    model = (out / "powerbi" / "friction.SemanticModel" / "definition" / "tables" / "decisions_daily.tmdl").read_text()
    root = ET.parse(tds).getroot()
    captions = {c.get("caption") for c in root.iter("column") if c.get("role") == "measure"}
    for m in spec["measures"]:
        assert f"measure '{m['name']}' = {m['dax']}" in model
        assert m["name"] in captions
    assert len(root.findall(".//named-connection")) == len(build_bi.TABLES)
    assert json.loads((out / "powerbi" / "friction.pbip").read_text())["artifacts"][0]["report"]["path"] == (
        "friction.Report"
    )
    result = build_bi.checks(data["decisions_daily"], spec, _graded(data["decisions_daily"]))
    assert result["all_equal"] and set(result["measures"]) == set(spec["checks"]["against"])
    off = _graded(data["decisions_daily"])
    off["desks"]["payments"]["models"]["lightgbm"]["test"]["capture_usd"]["estimate"] += 1e-6  # type: ignore[index]
    assert not build_bi.checks(data["decisions_daily"], spec, off)["all_equal"]
    header = build_bi.to_csv(data["quarantine"], build_bi.TABLES["quarantine"]).splitlines()[0]
    assert header == "desk,rule,rows_refused"
    assert {r["channel"] for r in data["decisions_daily"]} == {"swipe", "online"}


def test_a_channel_code_with_no_name_is_refused(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    doc = json.loads((raw / "bi_decisions_daily.json").read_text())
    doc["rows"][0][1] = "7"
    (raw / "bi_decisions_daily.json").write_text(json.dumps(doc))
    with pytest.raises(ValueError, match="has no name"):
        build_bi.load(raw)


def test_an_extract_with_other_columns_is_refused(tmp_path: Path) -> None:
    raw = _raw(tmp_path)
    (raw / "bi_quarantine.json").write_text(json.dumps({"columns": ["desk", "rows"], "rows": []}))
    with pytest.raises(ValueError, match="bi_quarantine"):
        build_bi.load(raw)


def test_the_python_of_each_measure_reads_only_the_sums() -> None:
    spec = yaml.safe_load((ROOT / "dashboards" / "measures.yaml").read_text())
    values = {k: 2.0 for k in ("decisions", "fraud_cents", "goods", "fraud_cents__flagged", "goods__step_up",
                               "goods__decline")}  # fmt: skip
    for m in spec["measures"]:
        assert build_bi.evaluate(m["python"], values) > 0


def test_the_sql_of_each_measure_equals_its_python_over_the_extract(tmp_path: Path) -> None:
    import duckdb

    raw = _raw(tmp_path)
    spec = yaml.safe_load((ROOT / "dashboards" / "measures.yaml").read_text())
    data = build_bi.load(raw)
    path = tmp_path / "decisions_daily.csv"
    path.write_text(build_bi.to_csv(data["decisions_daily"], build_bi.TABLES["decisions_daily"]))
    values = build_bi.sums(data["decisions_daily"], "0000", "9999")
    con = duckdb.connect()
    con.execute(f"create view decisions_daily as select * from read_csv('{path}', header = true)")
    for m in spec["measures"]:
        got = con.execute(f"select {m['sql']} from decisions_daily").fetchone()[0]
        assert float(got) == pytest.approx(build_bi.evaluate(m["python"], values), rel=1e-12), m["name"]
    words, metrics = build_bi.dictionary(spec)
    assert len(words.splitlines()) == 1 + sum(len(c) for c in build_bi.TABLES.values())
    assert metrics.splitlines()[0] == "Metric,Over the events,Over the mart"
