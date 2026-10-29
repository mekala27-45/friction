"""Salted tokens and the reductions used by the column masks.

A restricted value (a user id in clear, a card key, an IP address, a user agent string) never
leaves bronze in clear. It becomes an HMAC-SHA256 token keyed by FRICTION_SALT, the same function
in the pipeline, the API and the tests, so the online store and the lakehouse agree on keys
without either holding the clear value. The masks reduce an IP to its /16 and a user agent to its
family; those reductions live here too so the SQL and the Python agree.
"""

from __future__ import annotations

import hashlib
import hmac
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
    """A 20 hex character token for a value of a kind. The kind keeps a user id and a card id apart."""
    material = f"{kind}:{value}".encode()
    digest = hmac.new((key or salt()).encode(), material, hashlib.sha256).hexdigest()
    return f"{kind[0]}{digest[:19]}"


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
