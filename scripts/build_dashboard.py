"""Write the AI/BI dashboard definition the bundle deploys: dashboards/friction.lvdash.json.

    python scripts/build_dashboard.py           # write
    python scripts/build_dashboard.py --check   # fail if the committed file differs

Every dataset is one SQL query over the dev schemas; every widget names a dataset and the columns
it reads, and tests/gates/test_dashboard.py checks that each column is one the query returns. The
numbers on the dashboard are the workspace's own, read live; none of them is typed here.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "dashboards" / "friction.lvdash.json"
P = "workspace.friction_dev"

# Labels as eventually reported: the latest version of each event's label.
LABELS = f"(SELECT event_ref, is_fraud FROM {P}_cdc.labels_payments_history WHERE __END_AT IS NULL)"

DATASETS: dict[str, tuple[str, str]] = {
    "totals": (
        "Decision totals",
        f"""
SELECT COUNT(*) AS decisions,
       SUM(CASE WHEN d.verdict = 'decline' THEN 1 ELSE 0 END) AS declines,
       SUM(CASE WHEN d.verdict = 'step_up' THEN 1 ELSE 0 END) AS step_ups,
       SUM(CASE WHEN l.is_fraud THEN GREATEST(d.amount_cents, 0) ELSE 0 END) / 100.0 AS fraud_usd,
       SUM(CASE WHEN l.is_fraud AND d.verdict <> 'approve' THEN GREATEST(d.amount_cents, 0) ELSE 0 END) / 100.0
         AS captured_usd,
       1000.0 * SUM(CASE WHEN NOT COALESCE(l.is_fraud, false) AND d.verdict <> 'approve' THEN 1 ELSE 0 END)
         / SUM(CASE WHEN NOT COALESCE(l.is_fraud, false) THEN 1 ELSE 0 END) AS friction_per_1000_good
FROM {P}_gold.payments_decisions d LEFT JOIN {LABELS} l ON l.event_ref = d.txn_ref
WHERE d.event_date < DATE'2019-11-01'""",
    ),
    "daily": (
        "Decisions by day",
        f"""
SELECT event_date AS day, verdict, COUNT(*) AS decisions
FROM {P}_gold.payments_decisions WHERE verdict <> 'approve' GROUP BY ALL""",
    ),
    "weekly_capture": (
        "Fraud dollars stopped by week",
        f"""
SELECT DATE_TRUNC('WEEK', d.event_date) AS week,
       SUM(CASE WHEN l.is_fraud THEN GREATEST(d.amount_cents, 0) ELSE 0 END) / 100.0 AS fraud_usd,
       SUM(CASE WHEN l.is_fraud AND d.verdict <> 'approve' THEN GREATEST(d.amount_cents, 0) ELSE 0 END) / 100.0
         AS captured_usd
FROM {P}_gold.payments_decisions d LEFT JOIN {LABELS} l ON l.event_ref = d.txn_ref
WHERE d.event_date < DATE'2019-11-01' GROUP BY ALL""",
    ),
    "codes": (
        "Reason codes on step ups and declines",
        f"""
SELECT c.code, r.says, COUNT(*) AS decisions
FROM {P}_gold.payments_decisions d LATERAL VIEW EXPLODE(SPLIT(d.codes, ',')) c AS code
JOIN {P}_gold.reason_codes r ON r.code = c.code AND r.desk = 'payments'
WHERE d.verdict <> 'approve' AND d.codes <> '' GROUP BY ALL""",
    ),
    "quarantine": (
        "Rows refused by rule",
        f"""
SELECT desk, rule, COUNT(*) AS rows_refused FROM {P}_silver.quarantine GROUP BY ALL""",
    ),
    "ledger": (
        "Compute by job",
        f"""
SELECT job, task, COUNT(*) AS runs, SUM(compute_seconds) / 60.0 AS compute_minutes,
       SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed_runs
FROM {P}_ops.run_ledger GROUP BY ALL""",
    ),
    "accounts": (
        "Sign in attempts by account kind",
        f"""
SELECT CASE WHEN unknown_account THEN 'unknown account' ELSE 'known account' END AS account,
       SUM(attempts) AS attempts, SUM(successes) AS successes
FROM {P}_silver.login_account_stats GROUP BY ALL""",
    ),
}


def field(name: str, expression: str | None = None) -> dict[str, str]:
    return {"name": name, "expression": expression or f"`{name}`"}


def query(dataset: str, fields: list[dict[str, str]], disaggregated: bool = False) -> list[dict[str, Any]]:
    return [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields, "disaggregated": disaggregated}}]


def counter(name: str, dataset: str, column: str, title: str) -> dict[str, Any]:
    agg = f"sum({column})"
    return {
        "name": name,
        "queries": query(dataset, [field(agg, f"SUM(`{column}`)")]),
        "spec": {
            "version": 2,
            "widgetType": "counter",
            "encodings": {"value": {"fieldName": agg, "displayName": title}},
            "frame": {"showTitle": True, "title": title},
        },
    }


def chart(
    name: str,
    kind: str,
    dataset: str,
    x: tuple[str, str, str],
    y: tuple[str, str],
    title: str,
    color: tuple[str, str] | None = None,
) -> dict[str, Any]:
    """x is (column, scale, label), y is (column, label) summed, color is (column, label)."""
    agg = f"sum({y[0]})"
    fields = [field(x[0]), field(agg, f"SUM(`{y[0]}`)")]
    enc: dict[str, Any] = {
        "x": {"fieldName": x[0], "scale": {"type": x[1]}, "displayName": x[2]},
        "y": {"fieldName": agg, "scale": {"type": "quantitative"}, "displayName": y[1]},
    }
    if color:
        fields.append(field(color[0]))
        enc["color"] = {"fieldName": color[0], "scale": {"type": "categorical"}, "displayName": color[1]}
    return {
        "name": name,
        "queries": query(dataset, fields),
        "spec": {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"showTitle": True, "title": title}},
    }


def text(name: str, lines: list[str]) -> dict[str, Any]:
    return {"name": name, "multilineTextboxSpec": {"lines": lines}}


def at(widget: dict[str, Any], x: int, y: int, w: int, h: int) -> dict[str, Any]:
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


def build() -> dict[str, Any]:
    datasets = [
        {"name": k, "displayName": label, "queryLines": [line + "\n" for line in sql.strip().splitlines()]}
        for k, (label, sql) in DATASETS.items()
    ]
    payments = [
        at(
            text(
                "intro",
                [
                    "## Payments desk, decided as each drop lands\n",
                    "Every event since 1 July 2019, scored by the version the champion alias names. "
                    "Fraud dollars use labels as eventually reported; TabFormer marks no fraud after "
                    "October 2019, so those totals stop there. Synthetic data.\n",
                ],
            ),
            0,
            0,
            6,
            2,
        ),
        at(counter("c_decisions", "totals", "decisions", "Decisions to 31 Oct 2019"), 0, 2, 1, 3),
        at(counter("c_declines", "totals", "declines", "Declines"), 1, 2, 1, 3),
        at(counter("c_steps", "totals", "step_ups", "Step ups"), 2, 2, 1, 3),
        at(counter("c_fraud", "totals", "fraud_usd", "Fraud dollars"), 3, 2, 1, 3),
        at(counter("c_captured", "totals", "captured_usd", "Fraud dollars stopped"), 4, 2, 1, 3),
        at(counter("c_friction", "totals", "friction_per_1000_good", "Good customers asked, per 1,000"), 5, 2, 1, 3),
        at(
            chart(
                "daily",
                "line",
                "daily",
                ("day", "temporal", "Day"),
                ("decisions", "Decisions"),
                "Step ups and declines per day",
                ("verdict", "Verdict"),
            ),
            0,
            5,
            6,
            6,
        ),
        at(
            chart(
                "weekly",
                "bar",
                "weekly_capture",
                ("week", "temporal", "Week"),
                ("captured_usd", "Dollars stopped"),
                "Fraud dollars stopped per week",
            ),
            0,
            11,
            3,
            6,
        ),
        at(
            chart(
                "codes",
                "bar",
                "codes",
                ("says", "categorical", "Reason"),
                ("decisions", "Decisions"),
                "Why: reason codes on step ups and declines",
            ),
            3,
            11,
            3,
            6,
        ),
    ]
    platform = [
        at(
            text(
                "p_intro",
                [
                    "## The platform under the desks\n",
                    "Rows the contracts refused, compute per job from the run ledger, and who signs "
                    "in to the login desk.\n",
                ],
            ),
            0,
            0,
            6,
            2,
        ),
        at(
            chart(
                "quarantine",
                "bar",
                "quarantine",
                ("rule", "categorical", "Rule"),
                ("rows_refused", "Rows"),
                "Rows refused, by rule",
                ("desk", "Desk"),
            ),
            0,
            2,
            3,
            6,
        ),
        at(
            chart(
                "ledger",
                "bar",
                "ledger",
                ("job", "categorical", "Job"),
                ("compute_minutes", "Minutes"),
                "Compute minutes, by job",
                ("task", "Task"),
            ),
            3,
            2,
            3,
            6,
        ),
        at(
            chart(
                "accounts",
                "bar",
                "accounts",
                ("account", "categorical", "Account"),
                ("attempts", "Attempts"),
                "Sign in attempts, known and unknown accounts",
            ),
            0,
            8,
            3,
            6,
        ),
    ]
    return {
        "datasets": datasets,
        "pages": [
            {"name": "payments", "displayName": "Payments", "layout": payments},
            {"name": "platform", "displayName": "Platform", "layout": platform},
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    text_out = json.dumps(build(), indent=1) + "\n"
    if args.check:
        same = OUT.exists() and OUT.read_text(encoding="utf-8") == text_out
        print("dashboard: the committed definition is current" if same else "dashboard: rebuild it")
        return 0 if same else 1
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text_out, encoding="utf-8")
    print(f"wrote {OUT.relative_to(ROOT)}: {len(DATASETS)} datasets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
