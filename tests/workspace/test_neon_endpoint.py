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


def test_a_secret_split_over_lines_or_escaped_leaves_the_log() -> None:
    from friction_workspace.run import redact

    value = "a" * 20 + "\n" + "b" * 20 + "\n"
    env = {"FRICTION_REPLAY_KEY": value}
    printed = f"Illegal header value b'Bearer {value!r}' and line {'b' * 20}"
    out = redact(printed, env)
    assert "a" * 20 not in out and "b" * 20 not in out


def test_a_replay_key_with_whitespace_is_refused_without_printing_it(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import pytest
    from friction_core.secrets import single_line

    monkeypatch.setenv("FRICTION_REPLAY_KEY", " first-key-0000\nsecond-key-1111 ")
    with pytest.raises(SystemExit) as err:
        single_line("FRICTION_REPLAY_KEY")
    assert "first-key" not in str(err.value) and "FRICTION_REPLAY_KEY" in str(err.value)
    monkeypatch.setenv("FRICTION_REPLAY_KEY", " only-key-0000\n")
    assert single_line("FRICTION_REPLAY_KEY") == "only-key-0000"
    monkeypatch.delenv("FRICTION_REPLAY_KEY")
    with pytest.raises(SystemExit):
        single_line("FRICTION_REPLAY_KEY")
