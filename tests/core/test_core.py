"""Units, tokens, digests and paths: the small definitions every other package leans on."""

from __future__ import annotations

import math
from pathlib import Path

import pytest
from friction_core import hashing, paths, tokens, units
from pydantic import ValidationError


@pytest.mark.parametrize(
    ("text", "cents"),
    [
        ("$134.09", 13409),
        ("$-77.00", -7700),
        ("1,204.50", 120450),
        ("$0.005", 0),
        ("$0.015", 2),
        (" $12 ", 1200),
        ("$", None),
        ("", None),
        ("abc", None),
        ("$NaN", None),
        ("$Infinity", None),
        (None, None),
    ],
)
def test_parse_amount_cents(text: str | None, cents: int | None) -> None:
    assert units.parse_amount_cents(text) == cents


def test_money_adds_within_a_currency() -> None:
    total = units.Money(cents=150) + units.Money(cents=250)
    assert total == units.Money(cents=400, currency="USD")
    assert total.dollars == 4.0


def test_money_refuses_a_currency_it_does_not_know() -> None:
    with pytest.raises(ValidationError):
        units.Money(cents=1, currency="EUR")  # type: ignore[arg-type]


def test_a_rate_says_per_what() -> None:
    rate = units.Rate(numerator=3, denominator=1000, per="1,000 good payment events")
    assert rate.value == 0.003
    assert math.isnan(units.Rate(numerator=1, denominator=0, per="login").value)
    with pytest.raises(ValidationError):
        units.Rate(numerator=1, denominator=2, per="  ")


def test_salt_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FRICTION_SALT", "s" * 40)
    assert tokens.salt() == "s" * 40
    monkeypatch.delenv("FRICTION_SALT")
    monkeypatch.setenv("FRICTION_ALLOW_TEST_SALT", "1")
    assert tokens.salt() == tokens.TEST_SALT
    monkeypatch.delenv("FRICTION_ALLOW_TEST_SALT")
    with pytest.raises(RuntimeError):
        tokens.salt()


def test_token_shape_and_key() -> None:
    a = tokens.token("user", 42, key="k")
    assert len(a) == 20 and a[0] == "u" and int(a[1:], 16) >= 0
    assert a == tokens.token("user", "42", key="k") != tokens.token("user", 42, key="other")
    assert tokens.token("ip", "10.0.0.1", key="k")[0] == "i"


def test_token_sql_names_the_kind_and_column() -> None:
    sql = tokens.token_sql("account", "acct", "salt_col")
    assert sql.startswith("concat('a', substr(sha2(concat(salt_col, ':account:', acct), 256), 1, 19))")


@pytest.mark.parametrize(
    ("ip", "masked"),
    [
        ("10.1.2.3", "10.1.0.0/16"),
        (" 192.168.200.7 ", "192.168.0.0/16"),
        ("2001:db8::1", "2001:db8::1/32"),
        ("not an ip", None),
        (None, None),
    ],
)
def test_ip_prefix16(ip: str | None, masked: str | None) -> None:
    got = tokens.ip_prefix16(ip)
    if masked and ":" in masked:
        assert got is not None and got.endswith("/32")
    else:
        assert got == masked


@pytest.mark.parametrize(
    ("browser", "family"),
    [
        ("Chrome Mobile 46.0.2490", "Chrome Mobile"),
        ("Firefox 20.0", "Firefox"),
        ("Opera", "Opera"),
        ("12.0", None),
        (None, None),
    ],
)
def test_browser_family(browser: str | None, family: str | None) -> None:
    assert tokens.browser_family(browser) == family


def test_canonical_json_is_order_free_and_refuses_nan() -> None:
    assert hashing.canonical_json({"b": 1, "a": [1, 2]}) == '{"a":[1,2],"b":1}'
    assert hashing.sha256_json({"a": 1, "b": 2}) == hashing.sha256_json({"b": 2, "a": 1})
    with pytest.raises(ValueError):
        hashing.canonical_json({"x": float("nan")})


def test_file_and_bytes_digests_agree(tmp_path: Path) -> None:
    path = tmp_path / "blob.bin"
    data = b"friction" * 300_000
    path.write_bytes(data)
    assert hashing.sha256_file(path) == hashing.sha256_bytes(data)
    assert hashing.sha256_text("x") == hashing.sha256_bytes(b"x")
    assert hashing.short(hashing.sha256_text("x")) == hashing.sha256_text("x")[:12]
    assert len(hashing.short("abcdef", 3)) == 3


def test_paths_follow_the_environment(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    paths.root.cache_clear()
    try:
        monkeypatch.setenv("FRICTION_ROOT", str(tmp_path))
        monkeypatch.delenv("FRICTION_DATA", raising=False)
        assert paths.root() == tmp_path.resolve()
        assert paths.raw_dir() == tmp_path.resolve() / "data" / "raw"
        assert paths.landing_dir().name == "landing" and paths.lake_dir().name == "lake"
        assert paths.work_dir().name == "work"
        assert paths.config_dir() == tmp_path.resolve() / "config"
        assert paths.contracts_dir().name == "contracts"
        assert paths.results_dir().name == "results" and paths.evaluation_dir().name == "evaluation"
        monkeypatch.setenv("FRICTION_DATA", str(tmp_path / "big"))
        assert paths.data_home() == (tmp_path / "big").resolve()
        paths.root.cache_clear()
        monkeypatch.delenv("FRICTION_ROOT")
        assert (paths.root() / "pyproject.toml").exists()
    finally:
        paths.root.cache_clear()
