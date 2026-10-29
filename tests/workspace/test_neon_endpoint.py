"""Terraform's pg backend reaches Neon only when the endpoint id travels in the options."""

from friction_workspace.neon import neon_endpoint


def test_direct_host_gets_its_endpoint() -> None:
    url = "postgresql://u:p@ep-calm-sea-123456.us-east-2.aws.neon.tech/neondb?sslmode=require"
    assert neon_endpoint(url) == url + "&options=endpoint%3Dep-calm-sea-123456"


def test_pooler_suffix_is_dropped() -> None:
    url = "postgresql://u:p@ep-calm-sea-123456-pooler.us-east-2.aws.neon.tech/neondb"
    assert neon_endpoint(url).endswith("?options=endpoint%3Dep-calm-sea-123456")


def test_existing_options_and_other_hosts_are_left_alone() -> None:
    named = "postgresql://u:p@ep-a.aws.neon.tech/db?options=endpoint%3Dep-a"
    local = "postgresql://u:p@localhost:5432/db"
    assert neon_endpoint(named) == named
    assert neon_endpoint(local) == local


def test_redact_removes_every_secret_and_the_neon_form() -> None:
    from friction_workspace.run import redact

    conn = "postgresql://u:pw@ep-calm-sea-1.us-east-2.aws.neon.tech/db?sslmode=require"
    env = {"FRICTION_SALT": "s" * 40, "TF_STATE_CONN": conn, "DATABRICKS_TOKEN": "short"}
    text = f"plan Literal({'s' * 40}) at {neon_endpoint(conn)} and {conn}; token short"
    out = redact(text, env)
    assert "s" * 40 not in out and "ep-calm-sea-1" not in out and "pw@" not in out
    assert out.count("<secret>") == 3 and out.endswith("token short")
