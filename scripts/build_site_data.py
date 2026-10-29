"""The site's data: formatted manifest values and tables, chart series, slips and the replay set.

    python scripts/build_site_data.py

Writes web/src/data/site.json. Every number the site prints is a manifest value formatted by the
same code the documents use (friction_core.formats), or a chart point from results/evaluation.json.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import yaml
from friction_core.formats import format_value
from friction_core.manifest import load as load_manifest
from friction_core.statements import STATEMENT

COLORS = {"floor": "control", "logistic": "m-logistic", "lightgbm": "m-lightgbm", "freeman": "m-freeman",
          "lightgbm_rep": "m-rep"}  # fmt: skip
NAMES = {"floor": "Floor", "logistic": "Logistic", "lightgbm": "LightGBM", "freeman": "Freeman RBA",
         "lightgbm_rep": "LightGBM with attack IP flag"}  # fmt: skip
REPLAY_EVENTS = 60


def _json(rel: str) -> Any:
    p = ROOT / rel
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def frontier(ev: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for desk, d in ev["desks"].items():
        budget = d["budget"]["step_up_per_1000"] + d["budget"]["decline_per_1000"]
        models = []
        for m, e in d["models"].items():
            pts = [
                {"x": r["friction_per_1000"], "y": r["capture_usd"]["estimate"], "lo": r["capture_usd"]["low"],
                 "hi": r["capture_usd"]["high"]}
                for r in e["frontier"] if r["friction_per_1000"] is not None
            ]  # fmt: skip
            t = e["test"]
            models.append({
                "model": m, "name": NAMES[m], "color": COLORS[m], "points": sorted(pts, key=lambda p: p["x"]),
                "op": {"x": t["friction_per_1000"]["estimate"], "y": t["capture_usd"]["estimate"],
                       "lo": t["capture_usd"]["low"], "hi": t["capture_usd"]["high"]},
            })  # fmt: skip
        out[desk] = {"budget": budget, "models": models, "champion": d["champion"]["model"]}
    return out


def replay_set() -> dict[str, Any]:
    out: dict[str, Any] = {}
    for desk in ("payments", "logins"):
        p = _json(f"results/parity/{desk}.json")
        if not p:
            continue
        exp = {e["event_ref"]: e for e in p["expected"]}
        events = [e for e in p["events"] if e["event_ref"] in exp][:REPLAY_EVENTS]
        out[desk] = {"model": p["model"], "events": events, "recorded": [exp[e["event_ref"]] for e in events]}
    return out


def classification() -> list[dict[str, str]]:
    rows = []
    for path in sorted((ROOT / "contracts").glob("*.yaml")):
        c = yaml.safe_load(path.read_text(encoding="utf-8"))
        if "columns" not in c:
            continue
        for col in c["columns"]:
            if col.get("classification") in {"restricted", "sensitive"}:
                rows.append({"table": c["table"], "column": col["name"], "classification": col["classification"],
                             "says": col.get("says", "")})  # fmt: skip
    return rows


def main() -> int:
    manifest = load_manifest(ROOT / "results" / "manifest.json")
    values = {k: format_value(v.value, v.fmt) for k, v in manifest.values.items()}
    tables = {
        k: {"columns": t.columns, "align": ["left" if f == "text" else "right" for f in t.formats],
            "rows": [[format_value(x, f) for x, f in zip(r, t.formats, strict=True)] for r in t.rows],
            "source": t.provenance.source, "origin": t.provenance.origin}
        for k, t in manifest.tables.items()
    }  # fmt: skip
    ev = _json("results/evaluation.json")
    monitor = _json("results/monitor.json")
    site = {
        "statement": STATEMENT,
        "as_of": manifest.as_of,
        "values": values,
        "tables": tables,
        "frontier": frontier(ev) if ev else None,
        "replay": replay_set(),
        "classification": classification(),
        "monitor": {"alerts": monitor["alerts"], "maturity": monitor["label_maturity"]} if monitor else None,
        "parity": _json("results/parity.json"),
    }
    out = ROOT / "web" / "src" / "data" / "site.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(site, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    print(f"site data: {len(values)} values, {len(tables)} tables -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
