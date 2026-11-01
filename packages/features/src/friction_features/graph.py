"""The names of the graph features, without Spark, for the model code, the registry and the API.

The lakehouse computes them in friction_transforms.graph. Three of the payment features read labels
(by the system time they were known, never the event's): those are the known fraud propagation
features, and the structure features are the rest. evaluation/plan_v2.yaml registers a model on the
structure features alone, so the share of the lift that comes from known fraud is measured, not
assumed.
"""

from __future__ import annotations

PAYMENT_STRUCTURE: tuple[str, ...] = ("m_cards_before", "m_new_cards_7d", "m_txn_7d")
PAYMENT_KNOWN_FRAUD: tuple[str, ...] = ("m_fraud_cards_known", "m_fraud_share", "u_other_cards_known",
                                        "card_known_fraud")  # fmt: skip
PAYMENT_GRAPH: tuple[str, ...] = (*PAYMENT_STRUCTURE, *PAYMENT_KNOWN_FRAUD)
LOGIN_GRAPH: tuple[str, ...] = ("ip_users_before", "ip_new_users_1d", "ip_unknown_1h", "asn_new_users_1h",
                                "ua_users_before")  # fmt: skip
