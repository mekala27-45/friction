"""Scan everything past bronze for an identifier that should not be there.

Ported from ledger, with its Luhn check and the lesson it taught (a UUID once passed Luhn, so a
digit run inside a longer token is not a card). What fails the scan:

- a Luhn valid run of 13 to 19 digits that stands alone;
- a CVV shaped field: a column or key named like a card security code;
- a street address pattern;
- a raw IPv4 address (a /16 prefix such as 81.167.0.0/16, which is what a masked reader sees,
  is not raw and does not fail);
- any token on the planted list in data/planted_identifiers.txt, which the tests plant upstream
  and expect the bronze contract to remove.

What it reads: the local silver and gold tables, every export under results/, the decision log
and API trace exports, the documents, and the built site. The workspace runs the same functions
over its own silver and gold in the governance job and writes results/scan_workspace.json.
"""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Iterable
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# A run of 13 to 19 digits with optional separators, not inside a longer token.
PAN = re.compile(r"(?<![\w.-])(?:\d[ -]?){13,19}(?![\w-])")
_OCTET = r"(?:25[0-5]|2[0-4]\d|1\d\d|[1-9]?\d)"
# Not preceded by a word character or a dot, not followed by a digit, a dot, a slash or a word
# character: so a CIDR prefix and the four part version inside a user agent do not count.
IPV4 = re.compile(rf"(?<![\w.]){_OCTET}(?:\.{_OCTET}){{3}}(?![\w./])")
STREET = re.compile(
    r"\b\d{1,5}\s+(?:[A-Z][a-z]+\s){1,3}(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Lane|Ln|Drive|Dr|"
    r"Court|Ct|Place|Pl|Terrace|Parkway|Pkwy)\b\.?"
)
CVV_NAMES = {"cvv", "cvv2", "cvc", "cvc2", "cid", "security_code", "card_verification", "card_security_code"}
CVV_KEY = re.compile(r'"(?:cvv2?|cvc2?|security_code|card_security_code)"\s*:', re.I)
PLANTED_FILE = Path("data/planted_identifiers.txt")
SCAN_GLOBS = (
    "data/lake/silver/**/*.parquet",
    "data/lake/gold/**/*.parquet",
    "data/lake/cdc/**/*.parquet",
    "results/**/*.json",
    "results/**/*.csv",
    "results/**/*.parquet",
    "results/**/*.md",
    "report/**/*.md",
    "docs/**/*.md",
    "README.md",
    "RESULTS.md",
    "web/public/data/**/*",
    "web/out/**/*.html",
    "web/out/**/*.json",
    "logs/**/*.log",
)


def luhn_valid(digits: str) -> bool:
    total = 0
    for position, char in enumerate(reversed(digits)):
        number = int(char)
        if position % 2 == 1:
            number *= 2
            if number > 9:
                number -= 9
        total += number
    return total % 10 == 0


def epoch_millis(digits: str) -> bool:
    """A 13 digit run starting with 1 is a Unix time in milliseconds (2001 to 2033). The only 13 digit
    card numbers are Visa's, which start with 4, so this never hides a card number. The login state
    snapshots hold such times, and one in ten passes Luhn by chance."""
    return len(digits) == 13 and digits[0] == "1"


def find_card_numbers(text: str) -> list[str]:
    found: list[str] = []
    for match in PAN.finditer(text):
        digits = re.sub(r"[ -]", "", match.group(0))
        if 13 <= len(digits) <= 19 and len(set(digits)) > 1 and not epoch_millis(digits) and luhn_valid(digits):
            found.append(digits)
    return found


def load_planted(root: Path) -> set[str]:
    path = root / PLANTED_FILE
    if not path.exists():
        return set()
    return {
        line.strip().lower()
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    }


def text_of(path: Path) -> str:
    if path.suffix == ".parquet":
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.parquet as pq

        table = pq.read_table(path)
        parts: list[str] = [",".join(table.column_names)]
        for name in table.column_names:
            column = table.column(name)
            if pa.types.is_string(column.type) or pa.types.is_large_string(column.type):
                parts.extend(str(v) for v in pc.unique(column.drop_null()).to_pylist())
            elif pa.types.is_integer(column.type) and pa.types.is_int64(column.type) and not name.endswith("_ns"):
                values = pc.unique(column.drop_null()).to_pylist()
                parts.extend(str(v) for v in values if abs(v) >= 10**12)
        return "\n".join(parts)
    if path.suffix == ".json":
        try:
            return json.dumps(json.loads(path.read_text(encoding="utf-8")), ensure_ascii=False)
        except (json.JSONDecodeError, UnicodeDecodeError):
            return ""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return ""


def columns_of(path: Path) -> list[str]:
    if path.suffix == ".parquet":
        import pyarrow.parquet as pq

        return [c.lower() for c in pq.read_schema(path).names]
    if path.suffix == ".csv":
        try:
            with path.open(encoding="utf-8") as handle:
                return [c.strip().strip('"').lower() for c in handle.readline().split(",")]
        except UnicodeDecodeError:
            return []
    return []


def findings(text: str, planted: set[str], columns: list[str] | None = None) -> list[str]:
    out = [f"card number ending {pan[-4:]}" for pan in find_card_numbers(text)]
    out += [f"raw IPv4 address in {m.group(0).split('.')[0]}.x.x.x" for m in IPV4.finditer(text)]
    out += ["street address pattern" for _ in STREET.finditer(text)]
    if CVV_KEY.search(text):
        out.append("CVV shaped key")
    for column in columns or []:
        if column in CVV_NAMES:
            out.append(f"CVV shaped column '{column}'")
    if planted:
        lowered = text.lower()
        out.extend(f"planted identifier '{token}'" for token in sorted(planted) if token in lowered)
    return out


def targets(root: Path) -> list[Path]:
    found: list[Path] = []
    for pattern in SCAN_GLOBS:
        found.extend(p for p in root.glob(pattern) if p.is_file() and "node_modules" not in p.parts)
    return sorted(set(found))


def check(root: Path, files: Iterable[Path] | None = None, planted: set[str] | None = None) -> int:
    scan = list(files) if files is not None else targets(root)
    if not scan:
        raise ValueError("the identifier scan found no files to scan")
    tokens = planted if planted is not None else load_planted(root)
    problems: list[str] = []
    for path in scan:
        if path.name == PLANTED_FILE.name:
            continue
        for finding in findings(text_of(path), tokens, columns_of(path)):
            problems.append(f"{path.relative_to(root)}: {finding}")
    if problems:
        raise ValueError("identifier scan failed:\n" + "\n".join(sorted(set(problems))[:50]))
    return len(scan)


if __name__ == "__main__":
    count = check(Path(__file__).resolve().parents[1])
    print(
        f"identifier scan: {count} files past bronze, no card numbers, CVV fields, street addresses, "
        "raw IP addresses or planted tokens"
    )
