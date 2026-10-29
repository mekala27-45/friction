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
