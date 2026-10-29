"""Gold in the workspace: the as of inputs and the inputs of the state machines, liquid clustered.

The state machines themselves run in the features task of the jobs, after the pipeline update
(see friction_transforms.gold.payments_features for why they cannot run inside a pipeline flow).
"""

from _setup import fq, spark
from friction_transforms import gold
from pyspark import pipelines as dp


@dp.materialized_view(name=fq("gold", "debit_counts"), comment="Debits per merchant category and month, rolling year")
def debit_counts():  # type: ignore[no-untyped-def]
    return gold.debit_counts(spark.read.table(fq("silver", "payments_silver")))


@dp.materialized_view(name=fq("gold", "known_frauds"), comment="Frauds per category known at each month start")
def known_frauds():  # type: ignore[no-untyped-def]
    return gold.known_frauds(
        spark.read.table(fq("cdc", "labels_payments_history")), spark.read.table(fq("silver", "payments_silver"))
    )


@dp.materialized_view(
    name=fq("gold", "mcc_fraud_rates"), comment="Merchant category fraud rate as known at each month start"
)
def mcc_fraud_rates():  # type: ignore[no-untyped-def]
    return gold.mcc_rates(spark.read.table(fq("gold", "debit_counts")), spark.read.table(fq("gold", "known_frauds")))


@dp.materialized_view(
    name=fq("gold", "payments_issue_age"), comment="Days since the card generation in effect was issued"
)
def payments_issue_age():  # type: ignore[no-untyped-def]
    issues = gold.card_issue_times(spark.read.table(fq("cdc", "cards_history")))
    return gold.issue_age(spark.read.table(fq("silver", "payments_silver")), issues)


@dp.materialized_view(
    name=fq("gold", "payment_feature_inputs"), comment="What the payments state machine reads", cluster_by=["_bucket"]
)
def payment_feature_inputs():  # type: ignore[no-untyped-def]
    enriched = gold.enrich(
        spark.read.table(fq("silver", "payments_silver")),
        spark.read.table(fq("gold", "payments_issue_age")),
        spark.read.table(fq("gold", "mcc_fraud_rates")),
    )
    return gold.payment_inputs(enriched)


@dp.materialized_view(
    name=fq("gold", "login_feature_inputs"), comment="What the login state machines read", cluster_by=["user_key"]
)
def login_feature_inputs():  # type: ignore[no-untyped-def]
    return gold.login_inputs(spark.read.table(fq("silver", "logins_silver")))
