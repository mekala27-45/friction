"""Neon connection strings for clients that cannot send the server name. Standard library only,
so a CI job without the project installed can use it."""

from __future__ import annotations

from urllib.parse import parse_qs, urlsplit, urlunsplit


def neon_endpoint(url: str) -> str:
    """Name the Neon endpoint in the connection options.

    Terraform's pg backend uses lib/pq, which does not send the server name, and Neon's proxy then
    refuses the connection. The endpoint id is the first label of the host, without -pooler.
    """
    parts = urlsplit(url)
    host = parts.hostname or ""
    if not host.endswith(".neon.tech") or "options" in parse_qs(parts.query):
        return url
    endpoint = host.split(".")[0].removesuffix("-pooler")
    query = "&".join(q for q in (parts.query, f"options=endpoint%3D{endpoint}") if q)
    return urlunsplit(parts._replace(query=query))


if __name__ == "__main__":
    import os

    print(neon_endpoint(os.environ["PG_CONN_STR"]))
