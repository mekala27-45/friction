"""Card and label history in the workspace, by AUTO CDC with SCD type 2.

Cards are sequenced by (changed_at, seq) and a DELETE closes the open version. Labels are kept on
two clocks: the flow sequences by (reported_at, seq), so __START_AT and __END_AT are system time
(when the bank learned each version), and event_ts is carried as the valid time. The reference
SCD2 in packages/transforms/scd.py writes the same history, and the reconcile compares them.
"""

from _setup import fq, spark
from friction_transforms import silver
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.temporary_view(name="card_changes")
def card_changes():  # type: ignore[no-untyped-def]
    return silver.cdc_typed(spark.readStream.table(fq("bronze", "cdc_cards_bronze")))


dp.create_streaming_table(name=fq("cdc", "cards_history"), comment="Card lifecycle as SCD type 2")
dp.create_auto_cdc_flow(
    target=fq("cdc", "cards_history"),
    source="card_changes",
    keys=["account_ref"],
    sequence_by=F.struct("changed_at", "seq"),
    apply_as_deletes=F.expr("op = 'DELETE'"),
    except_column_list=["op", "seq", "changed_at", "emitted_at", "drop_id", "source_file"],
    stored_as_scd_type=2,
)


def _labels(desk: str, events: str, key: str) -> None:
    @dp.temporary_view(name=f"label_changes_{desk}")
    @dp.expect_all_or_drop({"B_NO_EVENT": "NOT array_contains(violations, 'B_NO_EVENT')",
                            "B_REFUND": "NOT array_contains(violations, 'B_REFUND')"})  # fmt: skip
    def _view():  # type: ignore[no-untyped-def]
        typed = silver.labels_typed(spark.readStream.table(fq("bronze", f"labels_{desk}_bronze")))
        return silver.labels_checked(typed, spark.read.table(fq("silver", events)), key, has_refunds=desk == "payments")

    dp.create_streaming_table(name=fq("cdc", f"labels_{desk}_history"), comment=f"{desk} labels on two clocks")
    dp.create_auto_cdc_flow(
        target=fq("cdc", f"labels_{desk}_history"),
        source=f"label_changes_{desk}",
        keys=["event_ref"],
        sequence_by=F.struct("reported_at", "seq"),
        except_column_list=["label_ref", "desk", "reported_at", "seq", "drop_id", "source_file", "violations"],
        stored_as_scd_type=2,
    )


_labels("payments", "payments_silver", "txn_ref")
_labels("logins", "logins_silver", "login_ref")
