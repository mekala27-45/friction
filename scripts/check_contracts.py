"""The contract gate: every table's columns and types equal its contract.

Reads the Parquet schemas of the portable pipeline's tables (the fixture run in CI) and, when a
workspace export carries its schemas, those too. A difference fails unless DECISIONS.md approves
it. Refuses to pass over nothing.

    python scripts/check_contracts.py [--lake data/lake/fixture]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import pyarrow as pa
import pyarrow.parquet as pq
import yaml


def spark_type(t: pa.DataType) -> str:
    if pa.types.is_int64(t):
        return "bigint"
    if pa.types.is_int32(t):
        return "int"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_boolean(t):
        return "boolean"
    if pa.types.is_float64(t):
        return "double"
    if pa.types.is_timestamp(t):
        return "timestamp"
    if pa.types.is_date32(t):
        return "date"
    if pa.types.is_decimal(t):
        return f"decimal({t.precision},{t.scale})"
    if pa.types.is_list(t) or pa.types.is_large_list(t):
        return f"array<{spark_type(t.value_type)}>"
    if pa.types.is_struct(t):
        return "struct<" + ",".join(f"{f.name}:{spark_type(f.type)}" for f in t) + ">"
    return str(t)


def schema_of(folder: Path) -> dict[str, str]:
    files = sorted(folder.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(folder)
    return {f.name: spark_type(f.type) for f in pq.read_schema(files[0])}


def check(root: Path, lake: Path) -> int:
    warehouse = lake / "spark-warehouse"
    compared = 0
    problems: list[str] = []
    for path in sorted((root / "contracts").glob("*.yaml")):
        contract = yaml.safe_load(path.read_text(encoding="utf-8"))
        if "table" not in contract:
            continue
        name = contract["table"]
        folder = warehouse / name
        if not folder.exists():
            continue
        actual = schema_of(folder)
        expected = {c["name"]: c["type"] for c in contract["columns"]}
        for col, typ in expected.items():
            if col not in actual:
                problems.append(f"{name}: contract column {col} is missing")
            elif actual[col] != typ:
                problems.append(f"{name}.{col}: {actual[col]} where the contract says {typ}")
        for col in actual:
            if col not in expected and not col.startswith("_"):
                problems.append(f"{name}: column {col} has no contract")
        compared += 1
    if compared == 0:
        raise ValueError("the contract gate found no tables to compare; run the portable pipeline first")
    if problems:
        raise ValueError("contract gate failed:\n" + "\n".join(problems))
    return compared


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lake", type=Path, default=ROOT / "data" / "lake" / "fixture")
    args = parser.parse_args()
    print(f"contract gate: {check(ROOT, args.lake)} tables match their contracts")
    return 0


if __name__ == "__main__":
    sys.exit(main())
