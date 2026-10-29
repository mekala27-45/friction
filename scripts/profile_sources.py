"""The first look: both sources profiled at full size with Spark, before any transformation.

Row counts, empties per column, distinct counts, ranges, label rates by month, the time span,
duplicated natural keys, and the ten strangest rows of each desk. What it finds becomes the first
quarantine rules (contracts/quarantine_rules.yaml), each with a test. Writes results/profile/.

    python scripts/profile_sources.py [--only tabformer|rba]
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from friction_core.paths import raw_dir
from friction_transforms import session

OUT = ROOT / "results" / "profile"


def _rows(df: Any) -> list[dict[str, Any]]:
    return [r.asDict() for r in df.collect()]


def _empties(spark: Any, view: str, columns: list[str]) -> dict[str, int]:
    expr = ", ".join(f"sum(CASE WHEN `{c}` = '' THEN 1 ELSE 0 END) AS `{c}`" for c in columns)
    return {k: int(v or 0) for k, v in spark.sql(f"SELECT {expr} FROM {view}").collect()[0].asDict().items()}


def _distincts(spark: Any, view: str, columns: list[str]) -> dict[str, int]:
    expr = ", ".join(f"approx_count_distinct(`{c}`, 0.01) AS `{c}`" for c in columns)
    return {k: int(v) for k, v in spark.sql(f"SELECT {expr} FROM {view}").collect()[0].asDict().items()}


def profile_tabformer(spark: Any) -> dict[str, Any]:
    df = spark.read.parquet(str(raw_dir() / "tabformer" / "staged"))
    df.createOrReplaceTempView("tf")
    cols = [c for c in df.columns if c != "source_row"]
    out: dict[str, Any] = {"source": "tabformer"}
    out["rows"] = df.count()
    out["empties"] = _empties(spark, "tf", cols)
    out["distinct_approx"] = _distincts(spark, "tf", cols)
    out["exact"] = _rows(
        spark.sql(
            """
            SELECT count(DISTINCT user) AS users,
                   count(DISTINCT user, card) AS cards,
                   min(try_cast(year AS INT)) AS year_min, max(try_cast(year AS INT)) AS year_max,
                   min(try_cast(mcc AS INT)) AS mcc_min, max(try_cast(mcc AS INT)) AS mcc_max,
                   min(try_cast(card AS INT)) AS card_slot_min, max(try_cast(card AS INT)) AS card_slot_max
            FROM tf
            """
        )
    )[0]
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW tfp AS
        SELECT *, try_cast(regexp_replace(amount, '[$,]', '') AS DECIMAL(12,2)) AS amount_dec,
               try_to_timestamp(concat(year, '-', lpad(month, 2, '0'), '-', lpad(day, 2, '0'), ' ', time),
                                'yyyy-MM-dd HH:mm') AS event_ts
        FROM tf
        """
    )
    out["amounts"] = _rows(
        spark.sql(
            """
            SELECT count_if(amount_dec IS NULL) AS unparsed,
                   count_if(amount_dec < 0) AS negative,
                   count_if(amount_dec = 0) AS zero,
                   min(amount_dec) AS min, max(amount_dec) AS max,
                   percentile_approx(amount_dec, 0.5) AS median,
                   percentile_approx(amount_dec, 0.99) AS p99
            FROM tfp
            """
        )
    )[0]
    out["time"] = _rows(
        spark.sql(
            """
            SELECT count_if(event_ts IS NULL) AS unparsed, min(event_ts) AS first, max(event_ts) AS last
            FROM tfp
            """
        )
    )[0]
    out["use_chip"] = _rows(spark.sql("SELECT use_chip, count(*) AS n FROM tf GROUP BY 1 ORDER BY 2 DESC"))
    out["is_fraud"] = _rows(spark.sql("SELECT is_fraud, count(*) AS n FROM tf GROUP BY 1 ORDER BY 2 DESC"))
    out["errors"] = _rows(spark.sql("SELECT errors, count(*) AS n FROM tf GROUP BY 1 ORDER BY 2 DESC LIMIT 30"))
    out["merchant_state_online"] = _rows(
        spark.sql(
            """
            SELECT use_chip, merchant_city, merchant_state = '' AS state_empty, zip = '' AS zip_empty, count(*) AS n
            FROM tf WHERE use_chip LIKE 'Online%' GROUP BY 1, 2, 3, 4 ORDER BY n DESC LIMIT 5
            """
        )
    )
    out["state_values"] = spark.sql("SELECT count(DISTINCT merchant_state) AS n FROM tf").collect()[0][0]
    out["non_us_states"] = _rows(
        spark.sql(
            """
            SELECT merchant_state, count(*) AS n FROM tf
            WHERE length(merchant_state) > 2 GROUP BY 1 ORDER BY 2 DESC LIMIT 15
            """
        )
    )
    out["zip_shapes"] = _rows(
        spark.sql(
            """
            SELECT CASE WHEN zip = '' THEN 'empty' WHEN zip RLIKE '^[0-9]+\\.0$' THEN 'float text'
                        ELSE 'other' END AS shape, count(*) AS n
            FROM tf GROUP BY 1 ORDER BY 2 DESC
            """
        )
    )
    out["fraud_by_year"] = _rows(
        spark.sql(
            """
            SELECT year, count(*) AS n, count_if(is_fraud = 'Yes') AS fraud,
                   round(count_if(is_fraud = 'Yes') / count(*), 6) AS rate
            FROM tf GROUP BY year ORDER BY year
            """
        )
    )
    out["fraud_by_month_last_48"] = _rows(
        spark.sql(
            """
            SELECT date_format(event_ts, 'yyyy-MM') AS month, count(*) AS n, count_if(is_fraud = 'Yes') AS fraud,
                   round(sum(CASE WHEN is_fraud = 'Yes' THEN amount_dec ELSE 0 END), 2) AS fraud_usd
            FROM tfp GROUP BY 1 ORDER BY 1 DESC LIMIT 48
            """
        )
    )
    out["natural_key_duplicates"] = _rows(
        spark.sql(
            """
            WITH k AS (
              SELECT user, card, year, month, day, time, amount, merchant_name,
                     count(*) AS n, count(DISTINCT mcc, use_chip, errors, is_fraud, zip) AS payloads
              FROM tf GROUP BY ALL HAVING count(*) > 1
            )
            SELECT count(*) AS keys_repeated, coalesce(sum(n), 0) AS rows_in_repeats,
                   count_if(payloads > 1) AS keys_with_different_payloads
            FROM k
            """
        )
    )[0]
    out["same_card_same_minute"] = _rows(
        spark.sql(
            """
            WITH k AS (SELECT user, card, year, month, day, time, count(*) AS n FROM tf GROUP BY ALL HAVING n > 1)
            SELECT count(*) AS minutes, coalesce(sum(n), 0) AS rows FROM k
            """
        )
    )[0]
    out["refunds_marked_fraud"] = spark.sql(
        "SELECT count(*) FROM tfp WHERE amount_dec < 0 AND is_fraud = 'Yes'"
    ).collect()[0][0]
    out["strangest"] = _rows(
        spark.sql(
            """
            (SELECT 'largest refund' AS why, * EXCEPT (amount_dec, event_ts) FROM tfp ORDER BY amount_dec ASC, source_row LIMIT 2)
            UNION ALL
            (SELECT 'largest amount', * EXCEPT (amount_dec, event_ts) FROM tfp ORDER BY amount_dec DESC, source_row LIMIT 2)
            UNION ALL
            (SELECT 'most error codes', * EXCEPT (amount_dec, event_ts) FROM tfp
             ORDER BY size(split(errors, ',')) DESC, source_row LIMIT 2)
            UNION ALL
            (SELECT 'first row of the file', * EXCEPT (amount_dec, event_ts) FROM tfp ORDER BY source_row LIMIT 1)
            UNION ALL
            (SELECT 'last row of the file', * EXCEPT (amount_dec, event_ts) FROM tfp ORDER BY source_row DESC LIMIT 1)
            UNION ALL
            (SELECT 'fraud refund', * EXCEPT (amount_dec, event_ts) FROM tfp
             WHERE amount_dec < 0 AND is_fraud = 'Yes' ORDER BY source_row LIMIT 1)
            UNION ALL
            (SELECT 'zero amount', * EXCEPT (amount_dec, event_ts) FROM tfp WHERE amount_dec = 0 ORDER BY source_row LIMIT 1)
            UNION ALL
            (SELECT 'foreign merchant', * EXCEPT (amount_dec, event_ts) FROM tfp
             WHERE length(merchant_state) > 2 ORDER BY source_row LIMIT 1)
            """
        )
    )
    return out


def profile_rba(spark: Any) -> dict[str, Any]:
    df = spark.read.parquet(str(raw_dir() / "rba" / "staged"))
    df.createOrReplaceTempView("rba")
    cols = [c for c in df.columns if c != "source_row"]
    out: dict[str, Any] = {"source": "rba"}
    out["rows"] = df.count()
    out["empties"] = _empties(spark, "rba", cols)
    out["distinct_approx"] = _distincts(spark, "rba", cols)
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW rbap AS
        SELECT *, try_to_timestamp(login_timestamp, 'yyyy-MM-dd HH:mm:ss.SSS') AS ts,
               try_cast(rtt_ms AS DOUBLE) AS rtt, try_cast(asn AS BIGINT) AS asn_n
        FROM rba
        """
    )
    out["exact"] = _rows(
        spark.sql(
            """
            SELECT count(DISTINCT user_id) AS users,
                   count_if(ts IS NULL) AS ts_unparsed, min(ts) AS first, max(ts) AS last,
                   count_if(rtt IS NULL) AS rtt_missing, min(rtt) AS rtt_min, max(rtt) AS rtt_max,
                   percentile_approx(rtt, 0.5) AS rtt_median,
                   count_if(asn_n IS NULL) AS asn_unparsed, count_if(asn_n >= 500000) AS asn_unrealistic,
                   count_if(ip_address LIKE '10.%') AS ip_10_8,
                   count_if(NOT ip_address RLIKE '^[0-9]{1,3}(\\.[0-9]{1,3}){3}$') AS ip_not_v4,
                   count_if(device_type = '') AS device_empty,
                   count_if(region = '-') AS region_dash, count_if(city = '-') AS city_dash,
                   count_if(login_successful = 'True') AS success,
                   count_if(is_attack_ip = 'True') AS attack_ip,
                   count_if(is_account_takeover = 'True') AS takeover,
                   count_if(is_account_takeover = 'True' AND login_successful = 'True') AS takeover_success,
                   count_if(login_index != cast(source_row AS STRING)) AS index_not_row
            FROM rbap
            """
        )
    )[0]
    out["by_month"] = _rows(
        spark.sql(
            """
            SELECT date_format(ts, 'yyyy-MM') AS month, count(*) AS n,
                   count_if(login_successful = 'True') AS success,
                   count_if(is_attack_ip = 'True') AS attack_ip,
                   count_if(is_account_takeover = 'True') AS takeover,
                   count(DISTINCT user_id) AS users
            FROM rbap GROUP BY 1 ORDER BY 1
            """
        )
    )
    out["device_types"] = _rows(spark.sql("SELECT device_type, count(*) AS n FROM rba GROUP BY 1 ORDER BY 2 DESC"))
    out["countries_top"] = _rows(
        spark.sql("SELECT country, count(*) AS n FROM rba GROUP BY 1 ORDER BY 2 DESC LIMIT 12")
    )
    out["logins_per_user"] = _rows(
        spark.sql(
            """
            WITH u AS (SELECT user_id, count(*) AS n FROM rba GROUP BY 1)
            SELECT count(*) AS users, count_if(n = 1) AS one_login, percentile_approx(n, 0.5) AS median,
                   percentile_approx(n, 0.99) AS p99, max(n) AS max
            FROM u
            """
        )
    )[0]
    out["duplicates"] = _rows(
        spark.sql(
            """
            WITH k AS (SELECT user_id, login_timestamp, ip_address, user_agent, count(*) AS n
                       FROM rba GROUP BY ALL HAVING n > 1)
            SELECT count(*) AS keys_repeated, coalesce(sum(n), 0) AS rows_in_repeats FROM k
            """
        )
    )[0]
    out["out_of_order_rows"] = spark.sql(
        """
        SELECT count_if(ts < prev) FROM (
          SELECT ts, lag(ts) OVER (PARTITION BY floor(source_row / 1000000) ORDER BY source_row) AS prev FROM rbap
        )
        """
    ).collect()[0][0]
    out["strangest"] = _rows(
        spark.sql(
            """
            (SELECT 'largest round trip time' AS why, * EXCEPT (ts, rtt, asn_n) FROM rbap ORDER BY rtt DESC NULLS LAST, source_row LIMIT 2)
            UNION ALL
            (SELECT 'smallest round trip time', * EXCEPT (ts, rtt, asn_n) FROM rbap WHERE rtt IS NOT NULL ORDER BY rtt, source_row LIMIT 1)
            UNION ALL
            (SELECT 'empty device type', * EXCEPT (ts, rtt, asn_n) FROM rbap WHERE device_type = '' ORDER BY source_row LIMIT 2)
            UNION ALL
            (SELECT 'unrealistic ASN', * EXCEPT (ts, rtt, asn_n) FROM rbap WHERE asn_n >= 500000 ORDER BY source_row LIMIT 1)
            UNION ALL
            (SELECT 'account takeover', * EXCEPT (ts, rtt, asn_n) FROM rbap WHERE is_account_takeover = 'True' ORDER BY source_row LIMIT 2)
            UNION ALL
            (SELECT 'bot device', * EXCEPT (ts, rtt, asn_n) FROM rbap WHERE device_type = 'bot' ORDER BY source_row LIMIT 1)
            UNION ALL
            (SELECT 'last row of the file', * EXCEPT (ts, rtt, asn_n) FROM rbap ORDER BY source_row DESC LIMIT 1)
            """
        )
    )
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=["tabformer", "rba"])
    args = parser.parse_args()
    spark = session.get(shuffle_partitions=16)
    OUT.mkdir(parents=True, exist_ok=True)
    for name, fn in (("tabformer", profile_tabformer), ("rba", profile_rba)):
        if args.only and name != args.only:
            continue
        started = time.monotonic()
        result = fn(spark)
        result["seconds"] = round(time.monotonic() - started, 1)
        (OUT / f"{name}.json").write_text(json.dumps(result, indent=1, default=str) + "\n", encoding="utf-8")
        print(f"{name}: {result['rows']} rows profiled in {result['seconds']} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
