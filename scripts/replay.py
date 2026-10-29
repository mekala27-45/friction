"""Cut both sources into drops and write the simulated feeds into the same calendar.

    python scripts/replay.py --all            # both desks, the three feeds, the lock
    python scripts/replay.py --desk payments  # one desk's drops only

Outputs, all outside git: data/landing/<desk>/drops/<drop>/*.csv.gz, the feeds under
data/landing/{labels_payments,labels_logins,cdc_cards}/drops/, the redelivered day 40 under
data/landing/payments/redelivery/, and the simulator's truth tables under data/work/truth/.
Commits data/replay.lock.json, the digest of every file.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import duckdb
import numpy as np
import pandas as pd
from friction_core.hashing import sha256_json
from friction_core.paths import landing_dir, raw_dir, work_dir
from friction_simulator import config, labels, lifecycle
from friction_simulator.replay import calendar, split_desk, write_feed

LOCK = ROOT / "data" / "replay.lock.json"


def _con() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET threads = 2")
    con.execute("SET memory_limit = '2500MB'")
    return con


def payments_frame(con: duckdb.DuckDBPyConnection, where: str, extra: str = "") -> pd.DataFrame:
    glob = (raw_dir() / "tabformer" / "staged" / "*.parquet").as_posix()
    return con.execute(
        f"""
        SELECT source_row AS txn_ref, CAST("user" AS BIGINT) AS "user", CAST(card AS BIGINT) AS card,
               strptime(year || '-' || month || '-' || day || ' ' || time, '%Y-%m-%d %H:%M') AS event_ts {extra}
        FROM read_parquet('{glob}') WHERE {where} ORDER BY txn_ref
        """
    ).df()


def feeds(rep: config.Replay, sim: config.Simulator, entries: dict[str, Any]) -> dict[str, Any]:
    con = _con()
    out: dict[str, Any] = {}
    truth = work_dir() / "truth"
    truth.mkdir(parents=True, exist_ok=True)
    pay = entries["payments"]
    pay_drops = calendar(
        date.fromisoformat(pay["first_day"]),
        date.fromisoformat(pay["last_day"]),
        rep.desks["payments"].daily_days,
    )
    frauds = payments_frame(
        con, "is_fraud = 'Yes'", ", try_cast(regexp_replace(amount, '[$,]', '') AS DOUBLE) AS amount_usd"
    )
    by_setting = {}
    for setting in sim.chargebacks.settings:
        lab = labels.chargeback_labels(frauds, sim, setting)
        by_setting[setting] = lab
    # Friendly fraud claims on good transactions, the same in every setting.
    good = payments_frame(con, "is_fraud = 'No'")
    claims = labels.friendly_fraud(good["txn_ref"].to_numpy(np.int64), good["event_ts"], sim)
    del good
    for setting, lab in by_setting.items():
        full = labels.with_sequence(pd.concat([lab.drop(columns=["incident_key"]), claims], ignore_index=True))
        full.to_parquet(truth / f"labels_payments_{setting}.parquet", index=False)
        if setting == "default":
            out["labels_payments"] = write_feed(full, "reported_at", pay_drops, landing_dir(), "labels_payments")
    # The card lifecycle uses the default setting's report times: a card is blocked when reported.
    reported = by_setting["default"]
    incident_reports = (
        reported.merge(frauds[["txn_ref", "user", "card"]], left_on="event_ref", right_on="txn_ref")
        .groupby(["user", "card", "incident_key"], as_index=False)["reported_at"]
        .min()
    )
    accounts = (
        payments_frame(con, "TRUE")
        .groupby(["user", "card"], as_index=False)
        .agg(first_ts=("event_ts", "min"), last_ts=("event_ts", "max"))
    )
    data_end = pd.Timestamp(pay["last_day"]) + pd.Timedelta(days=1)
    cdc = lifecycle.build(accounts, incident_reports[["user", "card", "reported_at"]], data_end, sim)
    cdc.to_parquet(truth / "cdc_cards.parquet", index=False)
    out["cdc_cards"] = write_feed(cdc, "emitted_at", pay_drops, landing_dir(), "cdc_cards")
    # Account takeover labels on the login desk.
    log = entries["logins"]
    log_drops = calendar(
        date.fromisoformat(log["first_day"]),
        date.fromisoformat(log["last_day"]),
        rep.desks["logins"].daily_days,
    )
    glob = (raw_dir() / "rba" / "staged" / "*.parquet").as_posix()
    ato = con.execute(
        f"""
        SELECT CAST(login_index AS BIGINT) AS login_index,
               strptime(login_timestamp, '%Y-%m-%d %H:%M:%S.%g') AS event_ts
        FROM read_parquet('{glob}') WHERE is_account_takeover = 'True' ORDER BY login_index
        """
    ).df()
    tl = labels.with_sequence(labels.takeover_labels(ato, sim))
    tl.to_parquet(truth / "labels_logins.parquet", index=False)
    out["labels_logins"] = write_feed(tl, "reported_at", log_drops, landing_dir(), "labels_logins")
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--desk", choices=["payments", "logins"])
    parser.add_argument("--feeds", action="store_true", help="only the simulated feeds, from the drops in the lock")
    args = parser.parse_args()
    rep = config.replay()
    sim = config.simulator()
    inc = config.incident()
    lock: dict[str, Any] = json.loads(LOCK.read_text(encoding="utf-8")) if LOCK.exists() else {}
    desks = [] if args.feeds else ["payments", "logins"] if args.all or not args.desk else [args.desk]
    for desk in desks:
        started = time.monotonic()
        d = rep.desks[desk]
        lock.setdefault("desks", {})[desk] = split_desk(
            desk,
            raw_dir() / d.source / "staged",
            landing_dir(),
            seed=rep.seed,
            lateness=rep.lateness,
            daily_days=d.daily_days,
            incident=inc,
        )
        print(
            f"{desk}: {lock['desks'][desk]['rows']} rows in {len(lock['desks'][desk]['drops'])} drops, "
            f"{round(time.monotonic() - started)} s"
        )
    if args.all or args.feeds:
        started = time.monotonic()
        lock["feeds"] = feeds(rep, sim, lock["desks"])
        print(f"feeds: {round(time.monotonic() - started)} s")
    lock["config_digest"] = sha256_json(
        {"replay": rep.model_dump(), "simulator": sim.model_dump(), "incident": inc.model_dump()}
    )
    LOCK.write_text(json.dumps(lock, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
