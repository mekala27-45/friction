"""Pure helpers for the workspace integration job (jobs/integration.py): which drops make a replay
day, and how two sets of table digests differ.

The job proves four things in an environment of its own (schemas with the friction_it prefix and a
pipeline of their own): re-landing a day leaves every table identical; days delivered shuffled, late
events first, give the same tables as in order; a backfill over a processed range changes nothing;
labels delivered out of order give the same label history.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

DAILY = re.compile(r"^\d{4}_\d{4}-\d{2}-\d{2}$")
# The feeds that follow each desk's calendar, as jobs/land.py lands them.
FEEDS = {"payments": ["payments", "labels_payments", "cdc_cards"], "logins": ["logins", "labels_logins"]}
TABLES = {
    "bronze": [
        "payments_bronze", "logins_bronze", "labels_payments_bronze", "labels_logins_bronze", "cdc_cards_bronze",
    ],
    "silver": ["payments_silver", "logins_silver", "quarantine", "login_account_stats"],
    "cdc": ["cards_history", "labels_payments_history", "labels_logins_history"],
    "gold": ["debit_counts", "known_frauds", "mcc_fraud_rates", "payments_issue_age", "payment_feature_inputs",
             "login_feature_inputs"],
}  # fmt: skip


def daily_drops(names: Iterable[str]) -> list[str]:
    """The daily drops of a desk's main feed, in order: replay day 1 is the first of them."""
    return sorted(n for n in names if DAILY.match(n))


def day_drop(names: Iterable[str], day: int) -> str:
    drops = daily_drops(names)
    if not 1 <= day <= len(drops):
        raise ValueError(f"replay day {day} is outside the {len(drops)} daily drops")
    return drops[day - 1]


def same_date(feed_names: Iterable[str], drop: str) -> list[str]:
    """A feed's drops delivered with a main drop: the same date, whatever their sequence number."""
    date = drop.split("_", 1)[1]
    return sorted(n for n in feed_names if n.split("_", 1)[-1] == date)


def differences(before: dict[str, dict[str, object]], after: dict[str, dict[str, object]]) -> list[str]:
    """Tables whose digest differs, or that exist on one side only."""
    return sorted(t for t in set(before) | set(after) if before.get(t) != after.get(t))


def guard(prefix: str) -> None:
    """The job resets its tables, so it refuses any prefix but the integration one."""
    if not prefix.endswith("_it"):
        raise SystemExit(f"the integration job resets its tables and runs only under a *_it prefix, not {prefix}")
