"""The transforms on small frames through a local Spark Connect session."""

from __future__ import annotations

import gzip
import json
import random
from pathlib import Path

import pandas as pd
import pytest

pytestmark = pytest.mark.spark


def _bronze_payment(**over: str) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        "txn_ref": "1", "user": "7", "card": "0", "year": "2019", "month": "12", "day": "30", "time": "10:15",
        "amount": "$12.50", "use_chip": "Chip Transaction", "merchant_name": "3527213246127876953",
        "merchant_city": " ONLINE", "merchant_state": "NV", "zip": "3054.0", "mcc": "5411", "errors": "Bad PIN,",
        "_rescued_data": None, "source_file": "/x/drops/0390_2019-12-31/payments_2019-12-31.csv.gz",
        "file_modified_at": None, "drop_id": "0390_2019-12-31", "ingested_at": None,
    }  # fmt: skip
    row.update(over)
    return row


def _payments_df(spark, rows):  # type: ignore[no-untyped-def]
    from friction_transforms.bronze import PAYMENT_TEXT

    cols = [*PAYMENT_TEXT, "_rescued_data", "source_file", "file_modified_at", "drop_id", "ingested_at"]
    schema = ", ".join(
        f"`{c}` string" if c not in {"file_modified_at", "ingested_at"} else f"`{c}` timestamp" for c in cols
    )
    return spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema)


def _silver(spark, rows, **kw):  # type: ignore[no-untyped-def]
    from friction_transforms import silver
    from pyspark.sql import functions as F

    checked = silver.payments_checked(_payments_df(spark, rows), F.lit("salt"), **kw)
    s, q = silver.payments_split(checked)
    return s.toPandas(), q.toPandas()


def test_tokens_match_python(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_core.tokens import token
    from friction_transforms.keys import token_col
    from pyspark.sql import functions as F

    values = ["-4324475583306591935", "81.167.144.58", "7:0"]
    df = spark.createDataFrame([(v,) for v in values], "v string")
    got = [r[0] for r in df.select(token_col("user", F.col("v"), F.lit("salt"))).collect()]
    assert got == [token("user", v, key="salt") for v in values]


def test_repairs_and_types(spark) -> None:  # type: ignore[no-untyped-def]
    s, q = _silver(spark, [_bronze_payment()])
    assert q.empty and len(s) == 1
    row = s.iloc[0]
    assert row["zip5"] == "03054" and row["merchant_city"] == "ONLINE" and list(row["errors"]) == ["Bad PIN"]
    assert row["amount_cents"] == 1250 and row["currency"] == "USD" and row["channel"] == 1
    assert row["merchant_state"] == "NV" and row["merchant_country"] == "US"
    assert row["merchant_key"].startswith("m") and len(row["merchant_key"]) == 20


def test_foreign_merchant_splits_state_and_country(spark) -> None:  # type: ignore[no-untyped-def]
    s, _ = _silver(spark, [_bronze_payment(merchant_state="Mexico", zip="")])
    assert s.iloc[0]["merchant_state"] is None and s.iloc[0]["merchant_country"] == "Mexico"
    assert bool(s.iloc[0]["foreign"]) and s.iloc[0]["zip5"] is None


@pytest.mark.parametrize(
    ("over", "rule"),
    [
        ({"txn_ref": "x"}, "P_KEY"),
        ({"time": "25:99"}, "P_TIME"),
        ({"amount": "$abc"}, "P_AMOUNT"),
        ({"mcc": "0"}, "P_MCC"),
        ({"use_chip": "Contactless"}, "P_CHANNEL"),
        ({"_rescued_data": '{"merchant_category":"5411","_file_path":"x"}', "mcc": None}, "P_RESCUED"),
        ({"day": "31", "time": "23:59", "drop_id": "0390_2019-12-30"}, "P_FUTURE"),
        ({"day": "1", "drop_id": "0390_2019-12-30"}, "P_LATE"),
    ],
)
def test_each_payment_rule(spark, over, rule) -> None:  # type: ignore[no-untyped-def]
    s, q = _silver(spark, [_bronze_payment(**over)])
    assert s.empty
    assert rule in list(q.iloc[0]["violations"])


def test_rescued_file_path_alone_is_not_a_violation_and_rename_repairs(spark) -> None:  # type: ignore[no-untyped-def]
    s, q = _silver(spark, [_bronze_payment(_rescued_data='{"_file_path":"x"}')])
    assert len(s) == 1 and q.empty
    renamed = _bronze_payment(_rescued_data='{"merchant_category":"5999"}', mcc=None)
    s, q = _silver(spark, [renamed], renames={"merchant_category": "mcc"})
    assert len(s) == 1 and int(s.iloc[0]["mcc"]) == 5999


def test_rejected_file(spark) -> None:  # type: ignore[no-untyped-def]
    s, q = _silver(spark, [_bronze_payment()], rejected_files=("payments_2019-12-31.csv.gz",))
    assert s.empty and "P_REJECTED_FILE" in list(q.iloc[0]["violations"])


def test_repeat_with_same_payload_is_deduplicated_and_with_another_is_quarantined(spark) -> None:  # type: ignore[no-untyped-def]
    first = _bronze_payment()
    resend = _bronze_payment(source_file="/x/drops/0391_2020-01-01/resend.csv.gz", drop_id="0391_2020-01-01")
    changed = _bronze_payment(
        amount="$99.00", source_file="/x/drops/0392_2020-01-02/b.csv.gz", drop_id="0392_2020-01-02"
    )
    s, q = _silver(spark, [first, resend])
    assert len(s) == 1 and q.empty
    s, q = _silver(spark, [first, changed])
    assert len(s) == 1 and s.iloc[0]["amount_cents"] == 1250
    assert list(q.iloc[0]["violations"]) == ["P_KEY_CONFLICT"]


def test_business_key_conflict_quarantines_both(spark) -> None:  # type: ignore[no-untyped-def]
    a = _bronze_payment(txn_ref="1", mcc="5411")
    b = _bronze_payment(txn_ref="2", mcc="5812")
    s, q = _silver(spark, [a, b])
    assert s.empty and len(q) == 2 and all("P_BUSINESS_KEY" in list(v) for v in q["violations"])


def _bronze_login(**over: str) -> dict[str, str | None]:
    row: dict[str, str | None] = {
        "login_index": "5", "login_timestamp": "2020-06-01 10:00:00.123", "user_id": "1234",
        "rtt_ms": "", "ip_address": "81.167.144.58", "country": "NO", "region": "-", "city": "-", "asn": "29695",
        "user_agent": "Mozilla/5.0", "browser": "Chrome Mobile 46.0.2490", "os": "Android 4.1", "device_type": "mobile",
        "login_successful": "True", "is_attack_ip": "False", "_rescued_data": None,
        "source_file": "/x/drops/0099_2020-06-01/logins.csv.gz", "file_modified_at": None,
        "drop_id": "0010_2020-06", "ingested_at": None,
    }  # fmt: skip
    row.update(over)
    return row


def _logins(spark, rows):  # type: ignore[no-untyped-def]
    from friction_transforms import silver
    from friction_transforms.bronze import LOGIN_TEXT
    from pyspark.sql import functions as F

    cols = [*LOGIN_TEXT, "_rescued_data", "source_file", "file_modified_at", "drop_id", "ingested_at"]
    schema = ", ".join(
        f"`{c}` string" if c not in {"file_modified_at", "ingested_at"} else f"`{c}` timestamp" for c in cols
    )
    df = spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema)
    s, q = silver.logins_split(silver.logins_checked(df, F.lit("salt")), silver.login_account_stats(df, F.lit("salt")))
    return s.toPandas(), q.toPandas()


def test_unknown_account_flag(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import silver
    from friction_transforms.bronze import LOGIN_TEXT
    from pyspark.sql import functions as F

    cols = [*LOGIN_TEXT, "_rescued_data", "source_file", "file_modified_at", "drop_id", "ingested_at"]
    schema = ", ".join(
        f"`{c}` string" if c not in {"file_modified_at", "ingested_at"} else f"`{c}` timestamp" for c in cols
    )
    rows = [_bronze_login(login_index=str(i), user_id="7", login_successful="False") for i in range(5)]
    rows += [_bronze_login(login_index="9", user_id="8", login_successful="True")]
    df = spark.createDataFrame([tuple(r[c] for c in cols) for r in rows], schema)
    stats = silver.login_account_stats(df, F.lit("salt"), attempts=3, share=0.1).toPandas()
    assert sorted(stats["unknown_account"].tolist()) == [False, True]
    s, _ = silver.logins_split(
        silver.logins_checked(df, F.lit("salt")), silver.login_account_stats(df, F.lit("salt"), 3)
    )
    assert int(s.where(F.col("unknown_account")).count()) == 5


def test_login_typing_and_flags(spark) -> None:  # type: ignore[no-untyped-def]
    s, q = _logins(spark, [_bronze_login(asn="512345", ip_address="10.1.2.3")])
    assert q.empty
    r = s.iloc[0]
    assert r["region"] is None and r["ip_prefix16"] == "10.1.0.0/16" and bool(r["flag_asn_unrealistic"])
    assert bool(r["flag_ip_private"]) and r["browser_family"] == "Chrome Mobile" and pd.isna(r["rtt_ms"])
    assert "ip_address" not in s.columns and "user_agent" not in s.columns


@pytest.mark.parametrize(
    ("over", "rule"),
    [
        ({"login_index": "x"}, "L_KEY"),
        ({"login_timestamp": "yesterday"}, "L_TIME"),
        ({"login_timestamp": "2019-06-01 10:00:00.000"}, "L_SPAN"),
        ({"ip_address": "300.1.1.1"}, "L_IP"),
        ({"device_type": ""}, "L_UA"),
        ({"_rescued_data": '{"extra":"1"}'}, "L_RESCUED"),
    ],
)
def test_each_login_rule(spark, over, rule) -> None:  # type: ignore[no-untyped-def]
    s, q = _logins(spark, [_bronze_login(**over)])
    assert s.empty and rule in list(q.iloc[0]["violations"])


def _naive_scd2(feed: pd.DataFrame) -> list[tuple]:
    """An independent SCD2 in plain pandas, written from the semantics in the docstring of scd.py."""
    feed = feed.drop_duplicates(["account_ref", "changed_at", "seq", "status", "credit_limit_cents", "op"])
    feed = feed.sort_values(["account_ref", "changed_at", "seq"])
    out = []
    for key, g in feed.groupby("account_ref"):
        rows = list(g.itertuples(index=False))
        open_v = None
        for i, r in enumerate(rows):
            payload = (r.card_generation, r.status, r.credit_limit_cents, r.currency)
            nxt = (rows[i + 1].changed_at, rows[i + 1].seq) if i + 1 < len(rows) else None
            if r.op == "DELETE":
                open_v = None
                continue
            if open_v is not None and open_v[1] == payload:
                out[-1] = (*out[-1][:3], nxt)
                continue
            out.append((key, payload, (r.changed_at, r.seq), nxt))
            open_v = (key, payload)
    return sorted(out)


def test_scd2_reference_on_out_of_order_and_repeated_feed(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import scd

    rng = random.Random(9)
    rows = []
    for acc in (101, 202):
        t = pd.Timestamp("2019-01-01")
        for seq in range(1, 9):
            t = t + pd.Timedelta(days=rng.randrange(1, 30))
            status = rng.choice(["active", "active", "blocked"])
            op = "DELETE" if seq == 8 and acc == 202 else "UPDATE"
            rows.append(dict(account_ref=acc, op=op, seq=seq, changed_at=t, emitted_at=t, card_generation=seq // 4,
                             status=status, credit_limit_cents=rng.choice([100000, 200000]), currency="USD",
                             drop_id=f"{seq:04d}"))  # fmt: skip
    feed = pd.DataFrame(rows)
    shuffled = pd.concat([feed, feed.sample(5, random_state=1)]).sample(frac=1, random_state=2)
    df = spark.createDataFrame(shuffled)
    history, conflicts = scd.scd2(
        df, ["account_ref"], ["changed_at", "seq"], ["card_generation", "status", "credit_limit_cents", "currency"],
        arrival=["emitted_at", "drop_id"],
    )  # fmt: skip
    got = sorted(
        (r["account_ref"], (r["card_generation"], r["status"], r["credit_limit_cents"], r["currency"]),
         (pd.Timestamp(r["__START_AT"]["changed_at"]), r["__START_AT"]["seq"]),
         None if r["__END_AT"] is None else (pd.Timestamp(r["__END_AT"]["changed_at"]), r["__END_AT"]["seq"]))
        for r in history.collect()
    )  # fmt: skip
    assert got == _naive_scd2(feed)
    assert conflicts.count() == 0


def test_issue_age_reads_the_generation_in_effect(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import gold

    silver = spark.createDataFrame(
        [(1, 100, pd.Timestamp("2019-03-10").to_pydatetime()), (2, 100, pd.Timestamp("2019-05-10").to_pydatetime())],
        "txn_ref long, account_ref long, event_ts timestamp",
    )
    issues = spark.createDataFrame(
        [(100, 0, pd.Timestamp("2019-01-01").to_pydatetime()), (100, 1, pd.Timestamp("2019-05-01").to_pydatetime())],
        "account_ref long, card_generation long, issued_at timestamp",
    )
    got = {r["txn_ref"]: r["days_since_issue"] for r in gold.issue_age(silver, issues).collect()}
    assert got == {1: pytest.approx(68.0), 2: pytest.approx(9.0)}


def test_known_frauds_respects_the_system_time_clock(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import gold

    silver = spark.createDataFrame(
        [(1, 5411, pd.Timestamp("2019-01-15").date(), False), (2, 5411, pd.Timestamp("2019-01-20").date(), False)],
        "txn_ref long, mcc int, event_date date, is_refund boolean",
    )
    hist = spark.createDataFrame(
        [(1, True, {"reported_at": pd.Timestamp("2019-01-31 12:00").to_pydatetime(), "seq": 1}, None),
         (2, True, {"reported_at": pd.Timestamp("2019-02-01 00:00:01").to_pydatetime(), "seq": 2}, None)],
        "event_ref long, is_fraud boolean, __START_AT struct<reported_at:timestamp,seq:bigint>, "
        "__END_AT struct<reported_at:timestamp,seq:bigint>",
    )  # fmt: skip
    rows = {str(r["month_start"]): r["frauds_known"] for r in gold.known_frauds(hist, silver).collect()}
    # At 1 February only the first label was known; the second arrived one second later.
    assert rows["2019-02-01"] == 1 and rows["2019-03-01"] == 2


def test_digest_is_order_free_and_counts_repeats(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms.digests import table_digest

    a = spark.createDataFrame([(1, "x"), (2, "y")], "k long, v string")
    b = spark.createDataFrame([(2, "y"), (1, "x")], "k long, v string")
    c = spark.createDataFrame([(1, "x"), (2, "y"), (2, "y")], "k long, v string")
    assert table_digest(a) == table_digest(b) != table_digest(c)


def test_reader_rescues_a_renamed_column(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import reader

    folder = tmp_path / "payments" / "drops" / "0001_2019-01"
    folder.mkdir(parents=True)
    from friction_transforms.bronze import PAYMENT_TEXT

    good = ",".join(PAYMENT_TEXT) + "\n" + ",".join(["1"] + ["a"] * (len(PAYMENT_TEXT) - 1)) + "\n"
    renamed_header = [("merchant_category" if c == "mcc" else c) for c in PAYMENT_TEXT]
    bad = ",".join(renamed_header) + "\n" + ",".join(["2"] + ["b"] * (len(PAYMENT_TEXT) - 1)) + "\n"
    for name, text in (("a.csv.gz", good), ("b.csv.gz", bad)):
        with gzip.open(folder / name, "wt") as fh:
            fh.write(text)
    rows = {
        r["txn_ref"]: r for r in reader.read_drops(spark, str(tmp_path / "payments" / "drops"), "payments").collect()
    }
    assert rows["1"]["mcc"] == "a" and rows["1"]["_rescued_data"] is None
    assert rows["2"]["mcc"] is None and json.loads(rows["2"]["_rescued_data"]) == {"merchant_category": "b"}


def test_reconcile_stats_at_every_grain(spark) -> None:  # type: ignore[no-untyped-def]
    from datetime import date

    from friction_transforms.digests import reconcile_stats

    pay = spark.createDataFrame(
        [(1, "a1", date(2019, 3, 1), 500, False), (2, "a1", date(2019, 3, 2), -500, True)],
        "txn_ref bigint, account_key string, event_date date, amount_cents bigint, is_refund boolean",
    )
    log = spark.createDataFrame(
        [(7, "u1", date(2020, 2, 3), True, False)],
        "login_ref bigint, user_key string, event_date date, success boolean, is_attack_ip boolean",
    )
    feats = spark.createDataFrame([(1, 0.5), (2, 1.5)], "txn_ref bigint, amount_z double")
    hist = spark.createDataFrame([(1, True)], "event_ref bigint, is_fraud boolean")
    quarantine = spark.createDataFrame([("payments", "P_LATE")], "desk string, rule string")
    out = reconcile_stats(
        {
            "payments_silver": pay,
            "logins_silver": log,
            "payments_features": feats,
            "labels_payments_history": hist,
            "quarantine": quarantine,
        }
    )
    assert out["payments"]["desk"] == [{"desk": "payments", "rows": 2, "keys": 2, "amount_cents": 0, "refunds": 1}]
    assert [r["day"] for r in out["payments"]["day"]] == ["2019-03-01", "2019-03-02"]
    assert out["logins"]["month"][0]["successes"] == 1
    assert out["feature_sums"]["payments_features"] == {"rows": 2.0, "amount_z": 2.0}
    assert out["histories"]["labels_payments_history"]["rows"] == 1
    assert out["quarantine"] == [{"desk": "payments", "rule": "P_LATE", "count": 1}]


def test_a_feed_with_no_drop_yet_is_an_empty_table(spark, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms import reader
    from friction_transforms.bronze import TEXT_COLUMNS

    (tmp_path / "payments" / "drops").mkdir(parents=True)
    df = reader.read_drops(spark, str(tmp_path / "labels_payments" / "drops"), "labels_payments")
    assert df.columns == [*TEXT_COLUMNS["labels_payments"], "_rescued_data", "source_file", "file_modified_at"]
    assert df.count() == 0


def test_digest_sums_many_rows_without_overflow(spark) -> None:  # type: ignore[no-untyped-def]
    from friction_transforms.digests import table_digest

    df = spark.range(5000).selectExpr("id as k", "cast(id * 7 as string) as v")
    got = table_digest(df)
    assert got["rows"] == 5000 and got == table_digest(df.orderBy(df.k.desc()))
