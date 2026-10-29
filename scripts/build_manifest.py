"""Assemble results/manifest.json from the results the build produced, and nothing else.

    python scripts/build_manifest.py

Every number a published document prints comes from here, with its provenance. A section whose
result file does not exist yet is left out, and the checklist line that needs it says not done;
the documents then print what is missing instead of a number.
"""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import yaml
from friction_core.manifest import ManifestBuilder, write
from friction_core.statements import STATEMENT

MODEL_NAMES = {
    "floor": "the floor",
    "logistic": "logistic regression",
    "lightgbm": "LightGBM",
    "lightgbm_rep": "LightGBM with the attack IP flag",
    "freeman": "Freeman RBA",
}
GATE_NAMES = {
    "capture_above_floor": "dollar capture above the floor, interval clear of zero",
    "friction_within_budget": "friction within the budget on test",
    "calibration_error": "expected calibration error under the bound",
    "score_stability": "score stability, PSI under the bound",
    "reason_code_coverage": "reason code coverage of every step up and decline",
}


def _load(rel: str) -> Any:
    path = ROOT / rel
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    return json.loads(text) if path.suffix == ".json" else yaml.safe_load(text)


def _git(*args: str) -> str:
    try:
        return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


class Builder:
    def __init__(self) -> None:
        self.m = ManifestBuilder(datetime.now(UTC).strftime("%Y-%m-%d"))

    def v(
        self, key: str, value: Any, fmt: str, origin: str, population: str, source: str, note: str | None = None
    ) -> None:
        self.m.value(key, value, fmt, origin=origin, population=population, source=source, note=note)

    def iv(self, key: str, d: dict[str, float], fmt: str, origin: str, population: str, source: str) -> None:
        for part in ("estimate", "low", "high"):
            self.v(f"{key}.{part}", d[part], fmt, origin, population, source)


def sources(b: Builder) -> None:
    lock = _load("data/sources.lock.json")
    replay = _load("data/replay.lock.json")
    tab, rba = _load("results/profile/tabformer.json"), _load("results/profile/rba.json")
    if tab:
        o = "scripts/profile_sources.py over the staged TabFormer file"
        b.v("data.tabformer.rows", tab["rows"], "int", o, "every row of card_transaction.v1.csv", "local")
        b.v("data.tabformer.users", tab["exact"]["users"], "int", o, "distinct users", "local")
        b.v("data.tabformer.cards", tab["exact"]["cards"], "int", o, "distinct user and card pairs", "local")
    if rba:
        o = "scripts/profile_sources.py over the staged RBA file"
        e = rba["exact"]
        b.v("data.rba.rows", rba["rows"], "int", o, "every row of rba-dataset.csv", "local")
        b.v("data.rba.users", e["users"], "int", o, "distinct user ids", "local")
        b.v(
            "data.rba.rtt_missing_share",
            e["rtt_missing"] / rba["rows"],
            "pct1",
            o,
            "rows without a round trip time",
            "local",
        )
        b.v("data.rba.takeovers", e["takeover"], "int", o, "rows marked as account takeover", "local")
        b.v("data.rba.attack_ip", e["attack_ip"], "int", o, "rows from addresses on the attacker list", "local")
        b.v("data.rba.ip_private", e["ip_10_8"], "int", o, "rows with an address in 10.0.0.0/8", "local")
        b.v("data.rba.asn_unrealistic", e["asn_unrealistic"], "int", o, "rows with an ASN of 500,000 or more", "local")
        b.v("data.rba.successes", e["success"], "int", o, "successful sign ins", "local")
    if lock:
        for name in ("tabformer", "rba"):
            entry = lock.get(name) or lock.get("sources", {}).get(name)
            if entry and "sha256" in entry:
                b.v(
                    f"data.{name}.digest",
                    entry["sha256"][:12],
                    "text",
                    "data/sources.lock.json",
                    "the archive",
                    "static",
                )
    if replay:
        o = "scripts/replay.py, digests in data/replay.lock.json"
        for desk, d in replay["desks"].items():
            b.v(f"replay.{desk}.rows", d.get("rows"), "int", o, f"rows the {desk} desk delivers", "simulator")
            b.v(f"replay.{desk}.drops", len(d.get("drops", [])), "int", o, f"drops of the {desk} desk", "simulator")
        for feed, d in replay["feeds"].items():
            b.v(f"replay.feeds.{feed}.rows", d.get("rows"), "int", o, f"rows of the {feed} feed", "simulator")


def lake(b: Builder) -> None:
    # The full open source run happens on the Actions runner with the workspace salt (request kind
    # portable); its summary is promoted to results/workspace/full_open_source.json.
    rec = _load("results/workspace/full_open_source.json") or _load("results/reconcile.json")
    if not rec or "local" not in rec:
        return
    loc = rec["local"]
    o = "scripts/summarize_lake.py over the full open source run"
    rows = []
    for name, d in sorted(loc["tables"].items()):
        b.v(f"lake.local.{name}.rows", d["rows"], "int", o, f"rows of {name}", "local")
        rows.append([name, d["rows"], d["digest"].split(":")[1][:12]])
    b.m.table(
        "lake.local.tables",
        ["Table", "Rows", "Digest"],
        ["text", "int", "text"],
        rows,
        origin=o,
        population="every table of the full local run",
        source="local",
    )
    q = loc["reconcile"]["quarantine"]
    b.m.table(
        "lake.local.quarantine",
        ["Desk", "Rule", "Rows"],
        ["text", "text", "int"],
        [[r["desk"], r["rule"], r["count"]] for r in q],
        origin=o,
        population="quarantined rows by the rule they broke",
        source="local",
    )
    b.v("lake.local.quarantine_rows", sum(r["count"] for r in q), "int", o, "rule violations in quarantine", "local")
    u = loc["unknown_account"]
    b.v(
        "data.rba.unknown_account_attempts",
        u["attempts"],
        "int",
        o,
        "attempts filed under the unknown account",
        "local",
    )
    b.v("data.rba.unknown_account_successes", u["successes"], "int", o, "successful attempts on it", "local")
    b.v(
        "data.rba.known_users",
        loc["users"]["logins_known"],
        "int",
        o,
        "user ids other than the unknown account",
        "local",
    )
    b.v(
        "data.tabformer.accounts_in_silver",
        loc["users"]["payments_accounts"],
        "int",
        o,
        "card accounts in silver",
        "local",
    )


def plan(b: Builder) -> None:
    p = _load("evaluation/plan.yaml")
    lock = _load("evaluation/plan.lock")
    o = "evaluation/plan.yaml"
    b.v("plan.sha256", lock["sha256"][:12], "text", "evaluation/plan.lock", "the registered plan", "plan")
    if "hypotheses" in lock:
        b.v(
            "plan.hypotheses_sha256",
            lock["hypotheses"]["sha256"][:12],
            "text",
            "evaluation/plan.lock",
            "the hypotheses",
            "plan",
        )
    for desk, d in p["desks"].items():
        b.v(f"plan.{desk}.step_up_budget", d["budget"]["step_up_per_1000"] / 1000, "per1k1", o, "good events", "plan")
        b.v(f"plan.{desk}.decline_budget", d["budget"]["decline_per_1000"] / 1000, "per1k1", o, "good events", "plan")
        for split, s in d["splits"].items():
            b.v(f"plan.{desk}.{split}.start", s["start"], "text", o, split, "plan")
            b.v(f"plan.{desk}.{split}.end", s["end"], "text", o, split, "plan")
    b.v("plan.resamples", p["intervals"]["resamples"], "int", o, "bootstrap resamples", "plan")


def evaluation(b: Builder) -> dict[str, Any] | None:
    ev = _load("results/evaluation.json")
    if not ev:
        return None
    src = "evaluation"
    o = f"scripts/grade.py over the scored sets of the {ev['source']} run"
    # "local" is the open source engine; the graded sets come from its run on the Actions runner.
    label = {"local": "open source", "workspace": "workspace", "fixture": "fixture"}.get(ev["source"], ev["source"])
    b.v("eval.source", label, "text", o, "where the scored sets came from", src)
    for desk, d in ev["desks"].items():
        pfx = f"eval.{desk}"
        t = d["test"]
        b.v(f"{pfx}.test.events", t["events"], "int", o, f"graded {desk} test events", src)
        b.v(f"{pfx}.test.frauds", t["frauds"], "int", o, "positives among them", src)
        b.v(f"{pfx}.test.users", t["users"], "int", o, "units of resampling", src)
        if desk == "payments":
            b.v(f"{pfx}.test.fraud_usd", t["fraud_usd"], "usd0", o, "fraud dollars in the test months", src)
        champ = d["champion"]["model"]
        b.v(f"{pfx}.champion", MODEL_NAMES.get(champ, "none") if champ else "none", "text", o, "the champion", src)
        rows = []
        for m, e in d["models"].items():
            mp = f"{pfx}.{m}"
            te = e["test"]
            b.iv(f"{mp}.capture_usd", te["capture_usd"], "pct1", o, f"{desk} test, {m}", src)
            b.iv(f"{mp}.capture", te["capture"], "pct1", o, f"{desk} test, {m}", src)
            for k in ("friction_per_1000", "step_ups_per_1000", "declines_per_1000"):
                b.iv(f"{mp}.{k}", {q: x / 1000 for q, x in te[k].items()}, "per1k2", o, f"{desk} test, {m}", src)
            b.iv(f"{mp}.precision_all", te["precision_all"], "pct1", o, f"{desk} test, {m}", src)
            b.v(f"{mp}.roc_auc", e["roc_auc"], "float3", o, f"{desk} test, {m}", src)
            b.v(f"{mp}.average_precision", e["average_precision"], "float3", o, f"{desk} test, {m}", src)
            b.v(f"{mp}.psi", e["psi_validation_to_test"], "float3", o, f"{desk} validation to test, {m}", src)
            b.v(f"{mp}.ece", e["calibration"]["ece"], "float3", o, f"{desk} test, {m}", src)
            b.v(f"{mp}.coverage", e["reasons"]["coverage"], "pct1", o, f"flagged {desk} test events, {m}", src)
            b.v(
                f"{mp}.validation_capture_usd",
                e["validation"]["capture_usd"],
                "pct1",
                o,
                f"{desk} validation, {m}",
                src,
            )
            rows.append(
                [
                    MODEL_NAMES[m],
                    te["capture_usd"]["estimate"],
                    te["capture_usd"]["low"],
                    te["capture_usd"]["high"],
                    te["step_ups_per_1000"]["estimate"] / 1000,
                    te["declines_per_1000"]["estimate"] / 1000,
                    te["precision_all"]["estimate"],
                    e["calibration"]["ece"],
                    e["roc_auc"],
                    e["average_precision"],
                ]
            )
        b.m.table(
            f"{pfx}.scorecard",
            ["Model", "Capture", "Low", "High", "Step ups", "Declines", "Precision", "ECE", "ROC AUC", "AP"],
            ["text", "pct1", "pct1", "pct1", "per1k2", "per1k2", "pct1", "float3", "float3", "float3"],
            rows,
            origin=o,
            population=f"{desk} test months at the operating point chosen on validation",
            source=src,
        )
        comp = [
            [
                MODEL_NAMES[c["a"]],
                MODEL_NAMES[c["b"]],
                c["difference"]["estimate"],
                c["difference"]["low"],
                c["difference"]["high"],
                c["p"],
                c["p_adjusted"],
                "yes" if c["rejected"] else "no",
            ]
            for c in d["comparisons"]
        ]
        b.m.table(
            f"{pfx}.comparisons",
            ["Model", "Against", "Difference", "Low", "High", "p", "Adjusted p", "Rejected"],
            ["text", "text", "pts1", "pts1", "pts1", "float3", "float3", "text"],
            comp,
            origin=o,
            population=f"{desk} test, dollar capture at the operating point",
            source=src,
        )
        gate_rows = []
        for m, g in d["gates"].items():
            for k, r in g.items():
                gate_rows.append([MODEL_NAMES[m], GATE_NAMES[k], "passed" if r["passed"] else "failed"])
        b.m.table(
            f"{pfx}.gates",
            ["Model", "Gate", "Result"],
            ["text", "text", "text"],
            gate_rows,
            origin=o,
            population=f"{desk} promotion gates the repository decides",
            source=src,
        )
        ib = d["interior"]["budget"]
        b.v(f"{pfx}.interior.budget", ib["verdict"], "text", o, "champion under half and double the budget", src)
        b.m.table(
            f"{pfx}.interior_budget",
            ["Budget", *[MODEL_NAMES[m] for m in ib["by_setting"]["registered"]]],
            ["text", *["pct1"] * len(ib["by_setting"]["registered"])],
            [[k, *v.values()] for k, v in ib["by_setting"].items()],
            origin=o,
            population=f"{desk} test dollar capture, thresholds re-chosen on validation",
            source=src,
        )
        cost = d["cost"]
        b.m.table(
            f"{pfx}.cost",
            ["Model", "Cost per 1,000 events"],
            ["text", "usd2"],
            [[MODEL_NAMES[m], c] for m, c in cost["per_1000_events"].items()],
            origin=o,
            population=f"{desk} test, config/costs.yaml assumptions",
            source=src,
        )
        b.v(f"{pfx}.cost.cheapest", MODEL_NAMES[cost["cheapest"]], "text", o, "cheapest at the assumptions", src)
        flips = sorted({s["parameter"] for s in cost["sensitivity"] if s["cheapest"] != cost["cheapest"]})
        b.v(
            f"{pfx}.cost.flips",
            ", ".join(flips) if flips else "none",
            "text",
            o,
            "assumptions that change the cheapest",
            src,
        )
    if "logins" in ev["desks"]:
        lg = ev["desks"]["logins"]
        at = lg["attackers"]
        for k, n in at["counts"].items():
            b.v(f"eval.logins.attackers.{k}.count", n, "int", o, f"{k} attacks in the test months", src)
        rows = [
            [MODEL_NAMES[m], *[v.get(k) for k in ("naive", "vpn", "targeted", "takeover")]]
            for m, v in at["models"].items()
        ]
        b.m.table(
            "eval.logins.attackers",
            ["Model", "Naive", "VPN", "Targeted", "Takeovers"],
            ["text", "pct1", "pct1", "pct1", "pct1"],
            rows,
            origin=o,
            population="capture at the operating point, by attacker model",
            source=src,
        )
        rba = lg["rba"]
        rsr = [
            [k, rba["risk_score_relation"]["ours"].get(k), rba["risk_score_relation"]["published"].get(k)]
            for k in ("naive", "vpn", "targeted")
        ]
        b.m.table(
            "eval.logins.rba_rsr",
            ["Attacker", "Ours", "Published"],
            ["text", "float2", "float2"],
            rsr,
            origin=o + "; published from evaluation/rba_published.yaml",
            population="risk score relation of the Freeman model",
            source=src,
        )
        rr = []
        for k, v in rba["median_reauthentication_rate"]["published"].items():
            for tpr, pub in v.items():
                rr.append([k, tpr, rba["median_reauthentication_rate"]["ours"].get(k, {}).get(tpr), pub])
        b.m.table(
            "eval.logins.rba_reauth",
            ["Attacker", "TPR", "Ours", "Published"],
            ["text", "text", "pct0", "pct0"],
            rr,
            origin=o + "; published read from figure rq1-general",
            population="median re-authentication rate per user, history of ten to fourteen",
            source=src,
        )
        b.v(
            "eval.logins.rba.users_in_band",
            rba["users_in_band"],
            "int",
            o,
            "users with ten to fourteen earlier sign ins",
            src,
        )
    hyp = ev["hypotheses"]
    b.m.table(
        "eval.hypotheses",
        ["Id", "Question", "A", "B", "Adjusted p", "Verdict"],
        ["text", "text", "float4", "float4", "float3", "text"],
        [[h["id"], h["question"], h["a_rate"], h["b_rate"], h["p_adjusted"], h["verdict"]] for h in hyp],
        origin=o,
        population="validation months, one sided, Benjamini-Hochberg across the five",
        source=src,
    )
    return ev


def workspace(b: Builder) -> None:
    probe = _load("results/workspace_probe.json")
    if not probe:
        return
    rows = [[k, v.get("status", "")] for k, v in sorted(probe.items()) if isinstance(v, dict) and "status" in v]
    b.m.table(
        "workspace.probe",
        ["Check", "Result"],
        ["text", "text"],
        rows,
        origin="requests/2026-10-29-probe.json",
        population="what Free Edition allowed the API on 29 October 2026",
        source="workspace",
    )


def incident(b: Builder) -> None:
    inc = _load("results/incident.json")
    if not inc or "drop_totals" not in inc.get("after", {}):
        return
    o = "scripts/replay_incident.py over the drops around replay day 40 (open source, build machine)"
    b.v("incident.drop", inc["drop"], "text", o, "the incident drop", "local")
    b.v("incident.truth_rows", inc["truth"]["rows"], "int", o, "rows in the corrected redelivery", "local")
    b.v("incident.truth_usd", inc["truth"]["amount_cents"] / 100, "usd2", o, "dollars in the redelivery", "local")
    for side in ("before", "after"):
        s = inc[side]
        b.v(f"incident.{side}.rows", s["drop_totals"]["rows"], "int", o, f"silver rows from the drop, {side}", "local")
        b.v(f"incident.{side}.usd", s["drop_totals"]["amount_cents"] / 100, "usd2", o, f"dollars, {side}", "local")
        comp = s["completeness"][0]["ratio"] if s["completeness"] else None
        b.v(f"incident.{side}.completeness", comp, "pct2", o, f"completeness of the drop, {side}", "local")
        b.m.table(f"incident.{side}.quarantine", ["Rule", "Rows"], ["text", "int"], sorted(s["quarantine"].items()),
                  origin=o, population=f"quarantined rows of the drop, {side} the fix", source="local")  # fmt: skip
    delayed = inc["truth"]["rows"] - inc["before"]["drop_totals"]["rows"]
    delayed_usd = (inc["truth"]["amount_cents"] - inc["before"]["drop_totals"]["amount_cents"]) / 100
    b.v("incident.delayed_rows", delayed, "int", o, "transactions held out of silver until the replay", "local")
    b.v("incident.delayed_usd", delayed_usd, "usd2", o, "dollars of decisions delayed", "local")
    b.v("incident.other_drops_unchanged", "yes" if inc["other_drops_unchanged"] else "no", "text", o,
        "digest of every row from every other drop equal before and after", "local")  # fmt: skip
    b.v("incident.drop_matches_truth", "yes" if inc["drop_matches_truth"] else "no", "text", o,
        "the drop's silver totals equal the corrected source", "local")  # fmt: skip
    timeline = [[r["at"], r["what"], r["seconds"]] for r in inc["timeline"]]
    prov = {e["file"]: e for e in (_load("results/provenance.json") or {}).get("promoted", [])}
    for rel, what in (
        ("results/workspace/monitor_alerts.json", "workspace monitor raised the alerts; the issue opened"),
        ("results/workspace/incident_drop.json", "workspace replay of drop 387 with the redelivery verified"),
        ("results/workspace/monitor_after_alerts.json", "workspace monitor after the replay"),
    ):  # fmt: skip
        if rel in prov:
            timeline.append([prov[rel]["ran_at"][:19].replace("T", " "), what, None])
    timeline.sort(key=lambda r: str(r[0]))
    b.m.table("incident.timeline", ["When (UTC)", "What", "Seconds"], ["text", "text", "float1"], timeline,
              origin=o, population="the runs that found and fixed the incident", source="local")  # fmt: skip
    alerts = _load("results/workspace/monitor_alerts.json")
    if alerts:
        rows = [[a["severity"], a["check"], a["scope"], a["note"]] for a in alerts if inc["drop"] in a["scope"]]
        b.m.table("incident.alerts", ["Severity", "Check", "Scope", "Note"], ["text"] * 4, rows,
                  origin="jobs/monitor.py in the workspace (request 2026-10-29-12g-monitor)",
                  population="alerts on the incident drop", source="workspace")  # fmt: skip
        b.v("monitor.alerts", len(alerts), "int", "jobs/monitor.py", "alerts over the backfill", "workspace")
        drift = sorted({a["scope"].split("/")[1] for a in alerts if a["check"] == "feature_psi"})
        b.v("monitor.drift_features", ", ".join(drift) or "none", "text", "jobs/monitor.py",
            "features drifting against the training months", "workspace")  # fmt: skip
    issue = _load("results/workspace/monitor_issue.json")
    if issue:
        b.v("monitor.issue", f"GitHub issue {issue['number']}", "text", "gh api, issues", "the issue the workflow "
            "opened from the monitor's alerts", "workspace")  # fmt: skip
    ws = _load("results/workspace/incident_drop.json")
    if ws and ws.get("rows"):
        r = ws["rows"][0]
        b.v("incident.workspace.rows", int(r[0]), "int", "sql/incident_verify.sql", "silver rows from drop 387 in "
            "the workspace after the replay", "workspace")  # fmt: skip
        b.v("incident.workspace.usd", int(r[1]) / 100, "usd2", "sql/incident_verify.sql", "their dollars", "workspace")
        match = int(r[0]) == inc["truth"]["rows"] and int(r[1]) == inc["truth"]["amount_cents"]
        b.v("incident.workspace.matches", "yes" if match else "no", "text", "sql/incident_verify.sql",
            "the workspace's drop equals the corrected source", "workspace")  # fmt: skip


def live(b: Builder) -> None:
    parity = _load("results/parity.json")
    if parity:
        for name, r in parity.items():
            for desk in ("payments", "logins"):
                if desk in r:
                    d = r[desk]
                    o = f"scripts/check_parity.py against {name}"
                    src = "live" if name == "live" else "build"
                    b.v(f"parity.{name}.{desk}.compared", d["compared"], "int", o, f"{desk} decisions compared", src)
                    b.v(f"parity.{name}.{desk}.equal", d["decisions_equal"], "int", o, f"{desk} decisions equal", src)
                    b.v(
                        f"parity.{name}.{desk}.worst",
                        d["max_abs_score_difference"],
                        "float4",
                        o,
                        "largest score difference",
                        src,
                    )


CHECKLIST = [
    (1, "Both sources fetched with digests in Actions, profiled at full size, quarantine rules, contracts, provenance"),
    (2, "The replay splitter, the lateness rule, the simulator and the incident config, deterministic with digests"),
    (3, "The portable pipeline on open source Spark 4.2 with local tests and the full local run"),
    (4, "Terraform with state in Neon, the bundle, the workspace workflow, the results branch, reviewed promotion"),
    (5, "The Lakeflow pipeline deployed to dev and run; the reconcile against the local run"),
    (6, "Jobs with retries and the run ledger; idempotency, order and late label tests in the workspace"),
    (7, "Every row of both sources backfilled in the workspace"),
    (8, "Governance: tags, masks, the row filter, grants, lineage, the scan; the planted drift caught by the plan"),
    (9, "Point in time features with the leakage test; training sets as of a cut with the two clocks test"),
    (
        10,
        "Floors and models on both desks registered in Unity Catalog; the plan hashed before any test month was scored",
    ),
    (
        11,
        "The frontier, operating points, intervals, BH, interior tests, calibration, hypotheses, the RBA comparison",
    ),
    (12, "Reason codes, model cards, validation reports, promotion gates and the champion aliases"),
    (13, "The API live on Vercel with the online store and the decision log; parity exact against the live URL"),
    (14, "Persistence: independent connection, audit before response, shared transaction, out of process check"),
    (15, "Monitoring with delayed label cohorts; the day 40 incident detected, handled by the runbook and written up"),
    (16, "Performance experiments and the compute ledger published"),
    (
        17,
        "The site on Pages; the live check; latency with cold start; RESULTS, README, DECISIONS, notebooks, AGENTS",
    ),
    (18, "Coverage, mypy, ruff, gates, skills valid, forty to sixty commits, the rederive, v0.1.0 after the commits"),
]


def checklist(b: Builder, ev: dict[str, Any] | None) -> None:
    status: dict[int, tuple[str, str]] = {}
    exists = lambda rel: (ROOT / rel).exists()  # noqa: E731
    probe = _load("results/workspace_probe.json") or {}
    rec_first = _load("results/reconcile_first_month.json") or {}
    rec_full = _load("results/reconcile_full.json") or {}
    full_export = _load("results/workspace/full_export.json") or {}
    status[1] = (
        (
            "done",
            "results/workspace/fetch_status_on_runner.json (fetched and verified in Actions), results/profile, "
            "contracts/, data/PROVENANCE.md",
        )
        if exists("results/workspace/fetch_status_on_runner.json") and exists("results/profile")
        else (
            "not done",
            "fetched and profiled at full size on the build machine (results/profile); the Actions fetch has not run",
        )
    )
    status[2] = ("done", "data/replay.lock.json, equal to the lock regenerated in Actions; tests/simulator")
    status[3] = (
        (
            "done",
            "results/workspace/full_open_source.json: every drop on open source Spark, on the Actions runner "
            "(the build machine has 2 cores and 19 GB of disk); tests/transforms",
        )
        if exists("results/workspace/full_open_source.json")
        else ("not done", "the full open source run has not finished")
    )
    status[4] = (
        ("done", "requests/ and the results branch; terraform state in Neon (request 03c); results/provenance.json")
        if exists("results/provenance.json") and exists("results/workspace")
        else ("not done", "terraform has not been applied from Actions")
    )
    if rec_full and not rec_full.get("drift"):
        status[5] = ("done", "results/reconcile_first_month.json and results/reconcile_full.json: every exported "
                             "table equal by digest across the engines")  # fmt: skip
    elif rec_first and not rec_first.get("drift"):
        equal = sum(t["digest_equal"] for t in rec_full.get("tables", {}).values())
        status[5] = ("not done", f"the first month is equal on all nine tables (results/reconcile_first_month.json); "
                                 f"over every row {equal} of {len(rec_full.get('tables', {}))} are equal and the "
                                 "card age feature differs (results/reconcile_full.json)")  # fmt: skip
    else:
        status[5] = ("not done", "the pipeline has not been reconciled against the open source run")
    integ = _load("results/workspace/integration.json") or {}
    labels_run = (_load("results/workspace/integration_labels.json") or {}).get("tests", {}).get("labels_out_of_order")
    ledger_rows = _load("results/workspace/run_ledger.json")
    clean = integ.get("status") == "succeeded" and all(
        not t.get("differences") for t in integ.get("tests", {}).values()
    )
    labels_ok = bool(labels_run) and not labels_run.get("differences") and labels_run.get("history_rows", 0) > 0
    if clean and labels_ok and ledger_rows:
        status[6] = ("done", "results/workspace/integration.json and integration_labels.json (every table "
                             "digest identical after re-landing, shuffled delivery, a repeat backfill and out of "
                             "order labels); retries in databricks.yml; results/workspace/run_ledger.json")  # fmt: skip
    elif clean:
        status[6] = ("not done", "results/workspace/integration.json: re-landing, shuffled delivery and a repeat "
                                 "backfill leave every table identical; the out of order label test and the run "
                                 "ledger summary have not been read yet")  # fmt: skip
    else:
        status[6] = ("not done", "the jobs are defined in databricks.yml; their workspace tests have not run")
    rows_full = {k: v["rows"] for k, v in full_export.get("digests", {}).items()}
    status[7] = (
        (
            "done",
            f"results/workspace/full_export.json: {rows_full.get('payments_silver', 0):,} payments and "
            f"{rows_full.get('logins_silver', 0):,} logins in silver",
        )
        if rows_full
        else ("not done", "nothing is backfilled in the workspace yet")
    )
    status[8] = (
        "not done",
        "the probe found masks and functions allowed (results/workspace_probe.json); nothing applied",
    )
    status[9] = ("done", "tests/features/test_point_in_time.py; tests/transforms/test_training.py")
    status[10] = (
        "not done",
        "floors and models fit on the local run, plan hashed before scoring (DECISIONS.md); "
        "Unity Catalog registration waits on the workspace",
    )
    status[11] = (
        ("done", f"results/evaluation.json from the {ev['source']} run's scored sets")
        if ev
        else ("not done", "no scored sets graded yet")
    )
    status[12] = ("not done", "codes, gates and cards are in the repository; the champion alias waits on Unity Catalog")
    status[13] = (
        ("not done", "the API runs and passes parity locally; Vercel and Neon are not connected yet")
        if not (_load("results/parity.json") or {}).get("live")
        else ("done", "results/parity.json")
    )
    status[14] = ("done", "tests/api/test_api.py and tests/api/test_out_of_process.py against Postgres")
    inc = _load("results/incident.json") or {}
    ws_inc = _load("results/workspace/incident_drop.json") or {}
    ws_ok = bool(ws_inc.get("rows")) and int(ws_inc["rows"][0][0]) == inc.get("truth", {}).get("rows")
    if (
        inc.get("drop_matches_truth")
        and inc.get("other_drops_unchanged")
        and ws_ok
        and exists("results/workspace/monitor_alerts.json")
        and exists("docs/incidents/2026-10-29-day-40.md")
    ):
        status[15] = ("done", "results/workspace/monitor_alerts.json (GitHub issue 1), results/incident.json, "
                      "results/workspace/incident_drop.json, docs/incidents/2026-10-29-day-40.md")  # fmt: skip
    else:
        status[15] = (
            "not done",
            "config/monitor.yaml and the incident plant exist; the incident has not been replayed",
        )
    if exists("results/performance.json") and exists("results/compute_ledger.json"):
        billed = (
            "with workspace usage from system.billing.usage"
            if exists("results/workspace/billing.json")
            else ("workspace usage from the billing table not read yet")
        )
        status[16] = ("done", f"results/performance.json (Actions runner, full data), results/compute_ledger.json "
                              f"(wall seconds per request; {billed})")  # fmt: skip
    else:
        status[16] = ("not done", "no performance experiment has run")
    status[17] = ("not done", "the site is not built yet")
    status[18] = ("not done", "the tag waits on every line above")
    del probe
    rows = [[n, status[n][0], text, status[n][1]] for n, text in CHECKLIST]
    b.m.table(
        "checklist",
        ["Step", "Status", "Line", "Evidence"],
        ["int", "text", "text", "text"],
        rows,
        origin="scripts/build_manifest.py",
        population="the definition of done",
        source="build",
    )
    missing = [str(n) for n, s in status.items() if s[0] != "done"]
    b.v("checklist.done", 18 - len(missing), "int", "scripts/build_manifest.py", "lines done", "build")
    b.v(
        "checklist.missing",
        ", ".join(missing) if missing else "none",
        "text",
        "scripts/build_manifest.py",
        "lines not done",
        "build",
    )


def static(b: Builder) -> None:
    b.v("static.statement", STATEMENT, "text", "friction_core.statements", "every surface", "static")
    b.v("build.commits", int(_git("rev-list", "--count", "HEAD") or 0), "int", "git", "commits on main", "build")


def main() -> int:
    b = Builder()
    static(b)
    sources(b)
    lake(b)
    plan(b)
    ev = evaluation(b)
    workspace(b)
    incident(b)
    live(b)
    checklist(b, ev)
    out = ROOT / "results" / "manifest.json"
    write(b.m.build(), out)
    print(f"manifest: {len(b.m.build().values)} values, {len(b.m.build().tables)} tables -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
