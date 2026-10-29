"""Cut the fixture slice the CI runs the whole pipeline on: a few cards and users, every drop.

Chooses card accounts and users by rule, not by hand: the accounts with the most fraud in the
modeling window plus a keyed sample of others, every user with a takeover plus a keyed sample of
others, then copies their rows from every drop file (the day 40 incident files included, headers
as they are) into data/fixtures/landing/. Small enough to commit, real enough to break things.

    python scripts/build_fixture_slice.py
"""

from __future__ import annotations

import gzip
import io
import json
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import duckdb
import numpy as np
import pandas as pd
from friction_core.paths import landing_dir, raw_dir
from friction_simulator import draws

OUT = ROOT / "data" / "fixtures" / "landing"
ACCOUNTS_BY_FRAUD = 4
ACCOUNTS_SAMPLED = 4
USERS_SAMPLED = 400


def choose() -> tuple[set[tuple[str, str]], set[str]]:
    con = duckdb.connect()
    con.execute("SET memory_limit = '2GB'")
    tf = (raw_dir() / "tabformer" / "staged" / "*.parquet").as_posix()
    fraud = con.execute(
        f"""SELECT "user", card, count(*) n FROM read_parquet('{tf}')
            WHERE is_fraud = 'Yes' AND year = '2019' GROUP BY 1, 2 ORDER BY n DESC, 1, 2 LIMIT {ACCOUNTS_BY_FRAUD}"""
    ).fetchall()
    accounts = con.execute(f"""SELECT DISTINCT "user", card FROM read_parquet('{tf}') ORDER BY 1, 2""").fetchall()
    keys = np.array([int(u) * 100 + int(c) for u, c in accounts], dtype=np.int64)
    u = draws.uniform(keys, 1, "fixture_accounts")
    sampled = [accounts[i] for i in np.argsort(u)[:ACCOUNTS_SAMPLED]]
    chosen = {(str(a), str(b)) for a, b, *_ in fraud} | {(str(a), str(b)) for a, b in sampled}
    rba = (raw_dir() / "rba" / "staged" / "*.parquet").as_posix()
    ato = {
        r[0]
        for r in con.execute(
            f"SELECT DISTINCT user_id FROM read_parquet('{rba}') WHERE is_account_takeover = 'True'"
        ).fetchall()
    }
    users = [r[0] for r in con.execute(f"SELECT DISTINCT user_id FROM read_parquet('{rba}') ORDER BY 1").fetchall()]
    uk = np.array([int(x) for x in users], dtype=np.int64)
    pick = draws.uniform(uk, 1, "fixture_users")
    sampled_users = {users[i] for i in np.argsort(pick)[:USERS_SAMPLED]}
    return chosen, ato | sampled_users


def main() -> int:
    accounts, users = choose()
    if OUT.exists():
        shutil.rmtree(OUT)
    account_refs = {str(int(a) * 100 + int(b)) for a, b in accounts}
    counts: dict[str, int] = {}
    payments_refs: set[str] = set()
    logins_refs: set[str] = set()
    order = ["payments", "logins", "cdc_cards", "labels_payments", "labels_logins"]
    for feed in order:
        for path in sorted((landing_dir() / feed).rglob("*.csv.gz")):
            with gzip.open(path, "rb") as fh:
                frame = pd.read_csv(io.BytesIO(fh.read()), dtype=str, keep_default_na=False)
            if feed == "payments":
                keep = frame[[(u, c) in accounts for u, c in zip(frame["user"], frame["card"], strict=True)]]
                payments_refs |= set(keep["txn_ref"])
            elif feed == "logins":
                keep = frame[frame["user_id"].isin(users)]
                logins_refs |= set(keep["login_index"])
            elif feed == "cdc_cards":
                keep = frame[frame["account_ref"].isin(account_refs)]
            elif feed == "labels_payments":
                keep = frame[frame["event_ref"].isin(payments_refs)]
            else:
                keep = frame[frame["event_ref"].isin(logins_refs)]
            if keep.empty:
                continue
            target = OUT / path.relative_to(landing_dir())
            target.parent.mkdir(parents=True, exist_ok=True)
            buf = io.StringIO()
            keep.to_csv(buf, index=False)
            with gzip.GzipFile(filename="", mode="wb", fileobj=target.open("wb"), mtime=0) as gz:
                gz.write(buf.getvalue().encode())
            counts[feed] = counts.get(feed, 0) + len(keep)
    summary = {"accounts": sorted(f"{a}:{b}" for a, b in accounts), "users": len(users), "rows": counts}
    (OUT.parent / "slice.json").write_text(json.dumps(summary, indent=1) + "\n", encoding="utf-8")
    print(summary["rows"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
