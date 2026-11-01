"""Turn the BI extracts into files a BI team opens, and check them against the graded numbers.

    python scripts/build_bi.py --raw results/bi/raw --out results/bi

Reads the extracts the sql request saved (sql/bi_extracts.sql, promoted into results/bi/raw) and
writes: CSV files with a header row; a Power BI project (friction.pbip with a TMDL semantic model
whose tables import the CSV files from a folder parameter and whose measures are the DAX of
dashboards/measures.yaml); a Tableau data source (friction.tds) over the same CSV files with the
same measures as calculated fields; and checks.json, where each measure computed in Python over the
test months must equal the point estimate results/evaluation.json graded for the champion. The
Power BI and Tableau files are generated text; nothing here opens them in either product.
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import re
import sys
from pathlib import Path
from typing import Any
from xml.sax.saxutils import quoteattr

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import yaml

TOLERANCE = 1e-9
# Column types of each extract, in order: the CSV header, the TMDL data types and the Tableau ones.
TABLES: dict[str, list[tuple[str, str]]] = {
    "decisions_daily": [
        ("day", "date"),
        ("channel", "int"),
        ("verdict", "text"),
        ("decisions", "int"),
        ("amount_cents", "int"),
        ("frauds", "int"),
        ("fraud_cents", "int"),
    ],
    "codes_daily": [("day", "date"), ("verdict", "text"), ("code", "text"), ("decisions", "int")],
    "reason_codes": [("desk", "text"), ("code", "text"), ("says", "text"), ("features", "text"), ("source", "text")],
    "quarantine": [("desk", "text"), ("rule", "text"), ("rows_refused", "int")],
}
# What each column says, for the data dictionary readback and a BI team read.
SAYS = {
    ("decisions_daily", "day"): "the day of the decided events, in UTC",
    ("decisions_daily", "channel"): "0 swipe, 1 chip, 2 online; only online can be stepped up",
    ("decisions_daily", "verdict"): "approve, step_up or decline, at the thresholds the grade chose",
    ("decisions_daily", "decisions"): "payment decisions of the day, channel and verdict",
    ("decisions_daily", "amount_cents"): "the amount of those decisions in cents, a refund as zero",
    ("decisions_daily", "frauds"): "those decisions whose label as eventually reported is fraud",
    ("decisions_daily", "fraud_cents"): "the amount of those frauds in cents, a refund as zero",
    ("codes_daily", "day"): "the day of the flagged events, in UTC",
    ("codes_daily", "verdict"): "step_up or decline",
    ("codes_daily", "code"): "a reason code carried by the decision (reason_codes.code)",
    ("codes_daily", "decisions"): "flagged decisions carrying the code",
    ("reason_codes", "desk"): "payments or logins",
    ("reason_codes", "code"): "the code",
    ("reason_codes", "says"): "the code in plain words",
    ("reason_codes", "features"): "the features that map to the code, comma separated",
    ("reason_codes", "source"): "the code table it comes from",
    ("quarantine", "desk"): "the feed the refused rows came from",
    ("quarantine", "rule"): "the contract rule that refused them",
    ("quarantine", "rows_refused"): "rows refused by the rule, a row counted once per rule",
}
SOURCE = "friction gold tables, payments decisions since 1 July 2019, synthetic data (TabFormer)"
TMDL_TYPE = {"date": "dateTime", "int": "int64", "text": "string"}
M_TYPE = {"date": "type date", "int": "Int64.Type", "text": "type text"}
TABLEAU_TYPE = {"date": "date", "int": "integer", "text": "string"}


def load(raw: Path) -> dict[str, list[dict[str, Any]]]:
    out = {}
    for name, cols in TABLES.items():
        doc = json.loads((raw / f"bi_{name}.json").read_text(encoding="utf-8"))
        if [c.lower() for c in doc["columns"]] != [c for c, _ in cols]:
            raise ValueError(f"bi_{name}: columns {doc['columns']} are not {[c for c, _ in cols]}")
        rows = []
        for r in doc["rows"]:
            row: dict[str, Any] = {}
            for (c, kind), v in zip(cols, r, strict=True):
                row[c] = int(v) if kind == "int" and v is not None else v
            rows.append(row)
        out[name] = rows
    return out


def to_csv(rows: list[dict[str, Any]], cols: list[tuple[str, str]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow([c for c, _ in cols])
    for r in rows:
        w.writerow(["" if r[c] is None else r[c] for c, _ in cols])
    return buf.getvalue()


def sums(rows: list[dict[str, Any]], start: str, end: str) -> dict[str, float]:
    """The sums the measures read over [start, end): plain, on flagged rows and by verdict."""
    s: dict[str, float] = {
        "decisions": 0,
        "fraud_cents": 0,
        "goods": 0,
        "fraud_cents__flagged": 0,
        "goods__step_up": 0,
        "goods__decline": 0,
    }
    for r in rows:
        if not (start <= str(r["day"]) < end):
            continue
        goods = r["decisions"] - r["frauds"]
        s["decisions"] += r["decisions"]
        s["fraud_cents"] += r["fraud_cents"]
        s["goods"] += goods
        if r["verdict"] != "approve":
            s["fraud_cents__flagged"] += r["fraud_cents"]
        if r["verdict"] in ("step_up", "decline"):
            s[f"goods__{r['verdict']}"] += goods
    return s


def evaluate(expression: str, values: dict[str, float]) -> float:
    """A measure's Python expression over the sums: name[sub] reads the sum name__sub."""
    code = re.sub(r"(\w+)\[(\w+)\]", r"\1__\2", expression)
    return float(eval(code, {"__builtins__": {}}, dict(values)))  # measures.yaml is part of the repository


def checks(rows: list[dict[str, Any]], spec: dict[str, Any], evaluation: dict[str, Any]) -> dict[str, Any]:
    start, end = spec["checks"]["window"]
    values = sums(rows, start, end)
    graded = evaluation["desks"]["payments"]["models"][spec["checks"]["model"]]["test"]
    by_name = {m["name"]: m for m in spec["measures"]}
    out: dict[str, Any] = {"window": [start, end], "model": spec["checks"]["model"], "measures": {}}
    for name, key in spec["checks"]["against"].items():
        got = evaluate(by_name[name]["python"], values)
        want = float(graded[key]["estimate"])
        out["measures"][name] = {
            "extract": got,
            "graded": want,
            "equal": abs(got - want) <= TOLERANCE * max(abs(want), 1.0),
        }
    out["all_equal"] = all(m["equal"] for m in out["measures"].values())
    return out


def tmdl_table(name: str, cols: list[tuple[str, str]], measures: list[dict[str, Any]]) -> str:
    lines = [f"table {name}", ""]
    for m in measures:
        lines += [
            f"\tmeasure '{m['name']}' = {m['dax']}",
            f"\t\tformatString: {'0.0%' if m['unit'] == 'share' else '#,0.00'}",
            f"\t\tdescription: {m['says']}",
            "",
        ]
    for c, kind in cols:
        lines += [
            f"\tcolumn {c}",
            f"\t\tdataType: {TMDL_TYPE[kind]}",
            f"\t\tsourceColumn: {c}",
            f"\t\tsummarizeBy: {'sum' if kind == 'int' else 'none'}",
            "",
        ]
    typed = ", ".join(f'{{"{c}", {M_TYPE[k]}}}' for c, k in cols)
    lines += [
        f"\tpartition {name} = m",
        "\t\tmode: import",
        "\t\tsource =",
        "\t\t\t\tlet",
        f'\t\t\t\t    Source = Csv.Document(File.Contents(CsvFolder & "{name}.csv"), [Delimiter = ",", '
        "Encoding = 65001, QuoteStyle = QuoteStyle.Csv]),",
        "\t\t\t\t    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),",
        f"\t\t\t\t    Typed = Table.TransformColumnTypes(Promoted, {{{typed}}})",
        "\t\t\t\tin",
        "\t\t\t\t    Typed",
        "",
    ]
    return "\n".join(lines)


def powerbi(out: Path, spec: dict[str, Any]) -> list[Path]:
    model = out / "powerbi" / "friction.SemanticModel"
    report = out / "powerbi" / "friction.Report"
    files = {
        out / "powerbi" / "friction.pbip": json.dumps(
            {
                "version": "1.0",
                "artifacts": [{"report": {"path": "friction.Report"}}],
                "settings": {"enableAutoRecovery": True},
            },
            indent=2,
        ),
        report / "definition.pbir": json.dumps(
            {"version": "4.0", "datasetReference": {"byPath": {"path": "../friction.SemanticModel"}}}, indent=2
        ),
        model / "definition.pbism": json.dumps({"version": "4.0", "settings": {}}, indent=2),
        model / "definition" / "database.tmdl": "database friction\n\tcompatibilityLevel: 1567\n",
        model / "definition" / "model.tmdl": "model Model\n\tculture: en-US\n\tdefaultPowerBIDataSourceVersion: "
        "powerBI_V3\n\n" + "".join(f"ref table {t}\n" for t in TABLES),
        model / "definition" / "expressions.tmdl": 'expression CsvFolder = "C:\\friction\\bi\\csv\\" meta '
        '[IsParameterQuery = true, Type = "Text", IsParameterQueryRequired = true]\n',
    }
    for name, cols in TABLES.items():
        measures = spec["measures"] if name == spec["table"] else []
        files[model / "definition" / "tables" / f"{name}.tmdl"] = tmdl_table(name, cols, measures)
    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text if text.endswith("\n") else text + "\n", encoding="utf-8")
    return sorted(files)


def dictionary(spec: dict[str, Any]) -> tuple[str, str]:
    """The data dictionary (file, column, format, source) and the metric definitions over the mart."""
    fmt = {"date": "text", "int": "int", "text": "text"}
    d = io.StringIO()
    w = csv.writer(d, lineterminator="\n")
    w.writerow(["file", "column", "format", "source"])
    for name, cols in TABLES.items():
        for c, kind in cols:
            w.writerow([f"{name}.csv", c, fmt[kind], f"{SAYS[(name, c)]}. Source: {SOURCE}."])
    m = io.StringIO()
    w = csv.writer(m, lineterminator="\n")
    w.writerow(["Metric", "Over the events", "Over the mart"])
    for measure in spec["measures"]:
        w.writerow([measure["name"].lower(), measure["says"], measure["sql"]])
    return d.getvalue(), m.getvalue()


def tableau(out: Path, spec: dict[str, Any]) -> Path:
    named, relations = [], []
    for i, (name, cols) in enumerate(TABLES.items(), start=1):
        conn = f"textscan.{i}"
        named.append(
            f"      <named-connection caption={quoteattr(name)} name={quoteattr(conn)}>\n"
            f"        <connection class='textscan' directory='csv' filename={quoteattr(name + '.csv')} />\n"
            "      </named-connection>"
        )
        columns = "\n".join(
            f"          <column datatype={quoteattr(TABLEAU_TYPE[k])} name={quoteattr(c)} ordinal='{j}' />"
            for j, (c, k) in enumerate(cols)
        )
        relations.append(
            f"    <relation connection={quoteattr(conn)} name={quoteattr(name)} "
            f"table={quoteattr(f'[{name}#csv]')} type='table'>\n"
            "      <columns character-set='UTF-8' header='yes' locale='en_US' separator=','>\n"
            f"{columns}\n      </columns>\n    </relation>"
        )
    calcs = "\n".join(
        f"  <column caption={quoteattr(m['name'])} datatype='real' name={quoteattr(f'[{m["name"]}]')} role='measure' "
        f"type='quantitative'>\n    <calculation class='tableau' formula={quoteattr(m['tableau'])} />\n  </column>"
        for m in spec["measures"]
    )
    text = (
        "<?xml version='1.0' encoding='utf-8' ?>\n"
        "<datasource formatted-name='friction payments' inline='true' version='18.1'>\n"
        "  <connection class='federated'>\n    <named-connections>\n"
        + "\n".join(named)
        + "\n    </named-connections>\n"
        + "\n".join(relations)
        + "\n  </connection>\n"
        + calcs
        + "\n</datasource>\n"
    )
    path = out / "tableau" / "friction.tds"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", type=Path, default=ROOT / "results" / "bi" / "raw")
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "bi")
    parser.add_argument("--evaluation", type=Path, default=ROOT / "results" / "evaluation.json")
    args = parser.parse_args()
    spec = yaml.safe_load((ROOT / "dashboards" / "measures.yaml").read_text(encoding="utf-8"))
    data = load(args.raw)
    (args.out / "csv").mkdir(parents=True, exist_ok=True)
    for name, cols in TABLES.items():
        (args.out / "csv" / f"{name}.csv").write_text(to_csv(data[name], cols), encoding="utf-8")
    powerbi(args.out, spec)
    tableau(args.out, spec)
    words, metrics = dictionary(spec)
    (args.out / "dictionary.csv").write_text(words, encoding="utf-8")
    (args.out / "metrics_definitions.csv").write_text(metrics, encoding="utf-8")
    result = checks(data["decisions_daily"], spec, json.loads(args.evaluation.read_text(encoding="utf-8")))
    (args.out / "checks.json").write_text(json.dumps(result, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    for name, m in result["measures"].items():
        print(f"{name}: extract {m['extract']:.12g}, graded {m['graded']:.12g}, {'equal' if m['equal'] else 'DIFFERS'}")
    return 0 if result["all_equal"] else 1


if __name__ == "__main__":
    sys.exit(main())
