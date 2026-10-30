"""Gold on open source Spark: the as of inputs, and the inputs of the state machines.

Each flow reads each of its inputs once and names every column it unions, because the graph
analyses a flow before its inputs exist. The state machines themselves (applyInPandas) run in the
features step after the pipeline, for the same reason; see friction_transforms.gold.
"""

from friction_transforms import gold
from pyspark import pipelines as dp
from pyspark.sql import DataFrame


@dp.materialized_view(name="debit_counts", comment="Debits per merchant category and month, with the rolling year")
def debit_counts() -> DataFrame:
    return gold.debit_counts(spark.read.table("payments_silver"))  # noqa: F821


@dp.materialized_view(name="known_frauds", comment="Frauds per category known at each month start")
def known_frauds() -> DataFrame:
    return gold.known_frauds(spark.read.table("labels_payments_history"), spark.read.table("payments_silver"))  # noqa: F821


@dp.materialized_view(name="mcc_fraud_rates", comment="Fraud rate per merchant category as known at each month start")
def mcc_fraud_rates() -> DataFrame:
    return gold.mcc_rates(spark.read.table("debit_counts"), spark.read.table("known_frauds"))  # noqa: F821


@dp.materialized_view(name="payment_feature_inputs", comment="What the payments state machine reads, per transaction")
def payment_feature_inputs() -> DataFrame:
    silver = spark.read.table("payments_silver")  # noqa: F821
    issues = gold.card_issue_times(spark.read.table("cards_history"))  # noqa: F821
    enriched = gold.enrich(silver, gold.issue_age(silver, issues), spark.read.table("mcc_fraud_rates"))  # noqa: F821
    return gold.payment_inputs(enriched)


@dp.materialized_view(name="payments_issue_age", comment="Days since the card generation in effect was issued")
def payments_issue_age() -> DataFrame:
    return spark.read.table("payment_feature_inputs").select("txn_ref", "days_since_issue")  # noqa: F821


@dp.materialized_view(name="login_feature_inputs", comment="What the login state machines read, per attempt")
def login_feature_inputs() -> DataFrame:
    return gold.login_inputs(spark.read.table("logins_silver"))  # noqa: F821
