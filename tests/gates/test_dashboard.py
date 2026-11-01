"""The dashboard definition: current, every widget reading columns its dataset returns, known tables only."""

from __future__ import annotations

import json
import re
from pathlib import Path

from scripts import build_dashboard

ROOT = Path(__file__).resolve().parents[2]
WRITTEN = {"payments_decisions", "reason_codes", "run_ledger"}  # tables the jobs create outside the contracts


def _columns(sql: str) -> set[str]:
    """The names a query's outer SELECT returns."""
    body = re.split(r"\bFROM\b", sql.split("SELECT", 1)[1], maxsplit=1)[0]
    items, depth, cur = [], 0, ""
    for ch in body:
        depth += ch == "("
        depth -= ch == ")"
        if ch == "," and depth == 0:
            items.append(cur)
            cur = ""
        else:
            cur += ch
    items.append(cur)
    out = set()
    for item in items:
        m = re.search(r"\bAS\s+(\w+)\s*$", item.strip(), re.S)
        out.add(m.group(1) if m else item.strip().split(".")[-1])
    return out


def test_the_committed_definition_is_current() -> None:
    assert (ROOT / "dashboards" / "friction.lvdash.json").read_text() == json.dumps(build_dashboard.build(),
                                                                                   indent=1) + "\n"  # fmt: skip


def test_every_widget_reads_what_its_dataset_returns() -> None:
    doc = build_dashboard.build()
    returns = {d["name"]: _columns("".join(d["queryLines"])) for d in doc["datasets"]}
    widgets = 0
    for page in doc["pages"]:
        for item in page["layout"]:
            pos = item["position"]
            assert pos["x"] + pos["width"] <= 6
            for q in item["widget"].get("queries", []):
                widgets += 1
                cols = returns[q["query"]["datasetName"]]
                for f in q["query"]["fields"]:
                    used = set(re.findall(r"`(\w+)`", f["expression"]))
                    assert used <= cols, (q["query"]["datasetName"], used - cols)
                enc = item["widget"]["spec"]["encodings"]
                names = {f["name"] for f in q["query"]["fields"]}
                assert {e["fieldName"] for e in enc.values()} <= names
    assert widgets >= 8


def test_every_table_is_a_contract_or_one_a_job_writes() -> None:
    contracts = {p.stem for p in (ROOT / "contracts").glob("*.yaml")}
    gold = {"payments_features", "logins_features", "mcc_fraud_rates"}
    for _, sql in build_dashboard.DATASETS.values():
        for name in re.findall(r"workspace\.friction_dev_\w+\.(\w+)", sql):
            assert name in contracts | WRITTEN | gold, name
