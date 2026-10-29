"""The two sources as they arrive: their column names, and the names the build gives them.

Staging keeps every value as the text the file carried and adds ``source_row``, the zero based line
number of the row in the locked file. That number is the natural key of a TabFormer transaction,
which has no identifier of its own, and it is stable because the file is held to its digest.
"""

from __future__ import annotations

TABFORMER_COLUMNS: dict[str, str] = {
    "User": "user",
    "Card": "card",
    "Year": "year",
    "Month": "month",
    "Day": "day",
    "Time": "time",
    "Amount": "amount",
    "Use Chip": "use_chip",
    "Merchant Name": "merchant_name",
    "Merchant City": "merchant_city",
    "Merchant State": "merchant_state",
    "Zip": "zip",
    "MCC": "mcc",
    "Errors?": "errors",
    "Is Fraud?": "is_fraud",
}

RBA_COLUMNS: dict[str, str] = {
    "index": "login_index",
    "Login Timestamp": "login_timestamp",
    "User ID": "user_id",
    "Round-Trip Time [ms]": "rtt_ms",
    "IP Address": "ip_address",
    "Country": "country",
    "Region": "region",
    "City": "city",
    "ASN": "asn",
    "User Agent String": "user_agent",
    "Browser Name and Version": "browser",
    "OS Name and Version": "os",
    "Device Type": "device_type",
    "Login Successful": "login_successful",
    "Is Attack IP": "is_attack_ip",
    "Is Account Takeover": "is_account_takeover",
}

SOURCES: dict[str, dict[str, str]] = {"tabformer": TABFORMER_COLUMNS, "rba": RBA_COLUMNS}
MEMBERS: dict[str, str] = {"tabformer": "card_transaction.v1.csv", "rba": "rba-dataset.csv"}
DESK_OF: dict[str, str] = {"tabformer": "payments", "rba": "logins"}
