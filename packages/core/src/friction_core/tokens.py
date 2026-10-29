"""Salted tokens and the reductions used by the column masks.

A restricted value (a user id in clear, a card account, an IP address, a user agent string) never
leaves bronze in clear. It becomes a salted SHA-256 token, ``sha256(salt:kind:value)``, written so
Spark SQL can compute the same thing with ``sha2(concat(...), 256)``: the pipeline, the API and the
tests share one definition, so the online store and the lakehouse agree on keys without either
holding the clear value. The salt is secret and the input has a fixed prefix, so a plain salted
hash serves here; nothing relies on it being a MAC. The masks reduce an IP to its /16 and a user agent to its
family; those reductions live here too so the SQL and the Python agree.
"""

from __future__ import annotations

import hashlib
import ipaddress
import os
import re

# A fixed salt for tests and the fixture slice only. Every real run reads FRICTION_SALT.
TEST_SALT = "friction-fixture-salt-not-a-secret"


def salt() -> str:
    value = os.environ.get("FRICTION_SALT")
    if value:
        return value
    if os.environ.get("FRICTION_ALLOW_TEST_SALT") == "1":
        return TEST_SALT
    raise RuntimeError("FRICTION_SALT is not set; set it, or FRICTION_ALLOW_TEST_SALT=1 for fixtures")


def token(kind: str, value: str | int, key: str | None = None) -> str:
    """A 20 character token: the kind's first letter and 19 hex digits. Matches token_sql()."""
    material = f"{key or salt()}:{kind}:{value}".encode()
    return f"{kind[0]}{hashlib.sha256(material).hexdigest()[:19]}"


def token_sql(kind: str, column: str, salt_expr: str) -> str:
    """The same token as a Spark SQL expression over a column, with the salt as an expression."""
    return f"concat('{kind[0]}', substr(sha2(concat({salt_expr}, ':{kind}:', {column}), 256), 1, 19))"


# Merchants are not people. Their keys use a fixed public salt so every environment agrees and the
# 19 digit merchant numbers of the source (which can pass Luhn by chance) never reach silver.
MERCHANT_SALT = "friction-merchants"


def ip_prefix16(ip: str | None) -> str | None:
    """The /16 an IPv4 address belongs to, which is what a masked reader sees."""
    if ip is None:
        return None
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return None
    if addr.version != 4:
        return str(ipaddress.ip_network(f"{addr}/32", strict=False).supernet(new_prefix=32))
    network = ipaddress.ip_network(f"{addr}/16", strict=False)
    return str(network)


_FAMILY = re.compile(r"^([A-Za-z][A-Za-z .]*?)(?:\s+\d.*)?$")


def browser_family(browser: str | None) -> str | None:
    """'Chrome Mobile 46.0.2490' -> 'Chrome Mobile'. The masked view of a user agent."""
    if browser is None:
        return None
    m = _FAMILY.match(browser.strip())
    return m.group(1).strip() if m else None
