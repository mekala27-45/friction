"""Every gate gets three tests: a clean pass, a deliberate violation, and refusing to pass over nothing."""

from __future__ import annotations

from pathlib import Path

import pytest
from friction_core.statements import STATEMENT

from scripts import check_no_em_dash, check_statement, check_tokens, check_vocabulary
from scripts import scan_for_planted_identifiers as scan


def _write(tmp: Path, name: str, text: str) -> Path:
    path = tmp / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


class TestDashGate:
    def test_clean(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "a.md", "Ranges are written with to, as in 1 to 3.")
        assert check_no_em_dash.check(tmp_path, [f]) == 1

    def test_violation(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "a.md", "a pause" + chr(0x2014) + "like this")
        with pytest.raises(ValueError, match="em dash"):
            check_no_em_dash.check(tmp_path, [f])

    def test_refuses_nothing(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no files"):
            check_no_em_dash.check(tmp_path, [])


class TestVocabularyGate:
    def test_clean(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "a.md", "The pipeline reruns a day and checks the digest.")
        assert check_vocabulary.check(tmp_path, [f]) == 1

    def test_violation(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "a.md", "We " + "lever" + "age the lakehouse.")
        with pytest.raises(ValueError, match="vocabulary"):
            check_vocabulary.check(tmp_path, [f])

    def test_refuses_nothing(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no files"):
            check_vocabulary.check(tmp_path, [])


class TestStatementGate:
    def test_clean(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "README.md", f"# x\n\n> {STATEMENT}\n")
        assert check_statement.check(tmp_path, [f]) == 1

    def test_violation(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "README.md", "# x\n\nNo statement here.\n")
        with pytest.raises(ValueError, match="missing the statement"):
            check_statement.check(tmp_path, [f])

    def test_html_is_read_as_visible_text(self, tmp_path: Path) -> None:
        words = STATEMENT.split(" ")
        html = "<p>" + " <span>".join(words) + "</p><script>var s='x'</script>"
        f = _write(tmp_path, "web/out/index.html", html)
        assert check_statement.check(tmp_path, [f]) == 1

    def test_refuses_nothing(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no surfaces"):
            check_statement.check(tmp_path, [])


class TestTokenGate:
    def test_clean(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "a.yml", "env:\n  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}\n")
        assert check_tokens.check(tmp_path, [f]) == 1

    @pytest.mark.parametrize(
        "text",
        [
            "token: dapi" + "0123456789abcdef" * 2,
            "postgresql://role:" + "s3cretpass" + "@ep-x.neon.tech/db",
            "Authorization: Bearer " + "abcdefghijklmnopqrstuvwxyz012345",
            "[DEFAULT]\nhost = x\ntoken = " + "dapiabcdefabcdefabcdefabcdef1234",
        ],
    )
    def test_violation(self, tmp_path: Path, text: str) -> None:
        f = _write(tmp_path, "leak.txt", text)
        with pytest.raises(ValueError, match="token gate failed"):
            check_tokens.check(tmp_path, [f])

    def test_refuses_nothing(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no files"):
            check_tokens.check(tmp_path, [])


class TestIdentifierScan:
    def test_clean(self, tmp_path: Path) -> None:
        text = "merchant m3a9f, ip prefix 81.167.0.0/16, browser Chrome 85.0.4183.127, uuid 4111-1111-1111-1111x"
        f = _write(tmp_path, "results/a.md", text)
        assert scan.check(tmp_path, [f], planted=set()) == 1

    @pytest.mark.parametrize(
        ("text", "finding"),
        [
            ("card 4111 1111 1111 1111 on file", "card number"),
            ('{"cvv": "123"}', "CVV"),
            ("ship to 221 Baker Street please", "street address"),
            ("from 81.167.144.58 at noon", "raw IPv4"),
        ],
    )
    def test_violation(self, tmp_path: Path, text: str, finding: str) -> None:
        f = _write(tmp_path, "results/a.md", text)
        with pytest.raises(ValueError, match=finding):
            scan.check(tmp_path, [f], planted=set())

    def test_planted_token(self, tmp_path: Path) -> None:
        f = _write(tmp_path, "results/a.md", "user PLANTED-USER-0001 logged in")
        with pytest.raises(ValueError, match="planted identifier"):
            scan.check(tmp_path, [f], planted={"planted-user-0001"})

    def test_parquet_columns_and_values(self, tmp_path: Path) -> None:
        import pyarrow as pa
        import pyarrow.parquet as pq

        path = tmp_path / "data" / "lake" / "silver" / "x.parquet"
        path.parent.mkdir(parents=True)
        pq.write_table(pa.table({"cvv": ["123"], "ip": ["10.1.2.3"]}), path)
        with pytest.raises(ValueError, match="CVV shaped column"):
            scan.check(tmp_path, [path], planted=set())

    def test_luhn_digits_inside_a_token_do_not_count(self) -> None:
        assert scan.find_card_numbers("id-4111111111111111-x") == []
        assert scan.find_card_numbers("4111111111111111") == ["4111111111111111"]

    def test_refuses_nothing(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no files"):
            scan.check(tmp_path, [], planted=set())
