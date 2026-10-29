"""Silver in the workspace: the same functions as the portable pipeline, with expectations.

Expectations come in three classes. Warn: the synthesis flags the RBA README describes, counted in
the event log and kept. Drop to quarantine: every rule in contracts/quarantine_rules.yaml, counted
per rule on the checked view and routed to the quarantine table, never discarded. Fail: an
invariant that must hold after the split (one currency, no null key) stops the update.
"""

import yaml
from _setup import CONTRACT, ROOT, fq, spark
from friction_transforms import silver
from pyspark import pipelines as dp
from pyspark.sql import functions as F

SALT = F.lit(spark.conf.get("spark.friction.salt"))
RULES = yaml.safe_load(open(f"{ROOT}/contracts/quarantine_rules.yaml", encoding="utf-8"))  # noqa: SIM115


def _counted(desk: str) -> dict[str, str]:
    return {code: f"NOT array_contains(violations, '{code}')" for code in RULES[desk]}


@dp.temporary_view(name="payments_checked")
@dp.expect_all(_counted("payments"))
def payments_checked():  # type: ignore[no-untyped-def]
    c = CONTRACT.get("payments", {})
    checked = silver.payments_checked(
        spark.read.table(fq("bronze", "payments_bronze")),
        SALT,
        c.get("renames", {}),
        tuple(c.get("rejected_files", [])),
    )
    return checked


@dp.temporary_view(name="logins_checked")
@dp.expect_all(_counted("logins"))
@dp.expect("asn_realistic", "NOT flag_asn_unrealistic")
@dp.expect("ip_not_private", "NOT flag_ip_private")
def logins_checked():  # type: ignore[no-untyped-def]
    c = CONTRACT.get("logins", {})
    return silver.logins_checked(
        spark.read.table(fq("bronze", "logins_bronze")), SALT, c.get("renames", {}), tuple(c.get("rejected_files", []))
    )


@dp.materialized_view(name=fq("silver", "payments_silver"), comment="Typed, conformed, deduplicated card transactions")
@dp.expect_or_fail("one_currency", "currency = 'USD'")
@dp.expect_or_fail("has_key", "txn_ref IS NOT NULL")
def payments_silver():  # type: ignore[no-untyped-def]
    return silver.payments_split(spark.read.table("payments_checked"))[0]


@dp.materialized_view(name=fq("silver", "logins_silver"), comment="Typed, conformed, deduplicated login attempts")
@dp.expect_or_fail("has_key", "login_ref IS NOT NULL")
def logins_silver():  # type: ignore[no-untyped-def]
    return silver.logins_split(spark.read.table("logins_checked"))[0]


@dp.materialized_view(name=fq("silver", "quarantine"), comment="Every row a rule refused, one row per rule")
def quarantine():  # type: ignore[no-untyped-def]
    p = silver.quarantine_rows(silver.payments_split(spark.read.table("payments_checked"))[1], "payments", "txn_ref")
    lg = silver.quarantine_rows(silver.logins_split(spark.read.table("logins_checked"))[1], "logins", "login_ref")
    return p.unionByName(lg)
