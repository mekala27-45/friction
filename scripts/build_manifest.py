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
    rec = _load("results/reconcile.json")
    if not rec or "local" not in rec:
        return
    loc = rec["local"]
    o = "scripts/summarize_lake.py over the full local run"
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
    b.v("eval.source", ev["source"], "text", o, "where the scored sets came from", src)
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
    workspace_ran = exists("results/workspace/first-month.json")
    status[1] = (
        (
            "not done",
            "fetched and profiled at full size on the build machine (results/profile); the Actions "
            "fetch waits on the workspace secrets",
        )
        if not exists("results/workspace/sources.json")
        else ("done", "results/workspace/sources.json")
    )
    status[2] = ("done", "data/replay.lock.json; tests/simulator")
    rec = _load("results/reconcile.json") or {}
    status[3] = (
        ("done", "results/reconcile.json (local full run); tests/transforms")
        if "local" in rec
        else ("not done", "the full local run has not finished")
    )
    status[4] = (
        (
            "not done",
            "written and validated in CI; apply waits on the Actions secrets FRICTION_SALT, "
            "TF_STATE_CONN and DATABASE_URL",
        )
        if not workspace_ran
        else ("done", "results/workspace")
    )
    status[5] = (
        ("not done", "requests 03 to 07 are committed and wait on the same secrets")
        if not workspace_ran
        else ("done", "results/workspace/first-month.json")
    )
    status[6] = ("not done", "the jobs are defined in databricks.yml; their workspace tests have not run")
    status[7] = ("not done", "nothing is backfilled in the workspace yet; the full local run holds every row")
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
    status[15] = ("not done", "config/monitor.yaml and the incident plant exist; the monitor has not run")
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
    live(b)
    checklist(b, ev)
    out = ROOT / "results" / "manifest.json"
    write(b.m.build(), out)
    print(f"manifest: {len(b.m.build().values)} values, {len(b.m.build().tables)} tables -> {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
