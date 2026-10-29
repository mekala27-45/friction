"""Stage each locked source file as Parquet, every value kept as the text it was, plus source_row.

Reads the CSV in order with Arrow's streaming reader, so ``source_row`` is the zero based line
number of the row in the file. Nothing is typed, cleaned or dropped here; that is the bronze
contract's job, and the first look reads these files to decide what the contract must say.

    python scripts/stage_sources.py [--only tabformer|rba]
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from friction_core.paths import raw_dir
from friction_transforms.sources import MEMBERS, SOURCES

ROWS_PER_FILE = 4_000_000


def stage(name: str) -> dict[str, object]:
    columns = SOURCES[name]
    source = raw_dir() / name / MEMBERS[name]
    out_dir = raw_dir() / name / "staged"
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.parquet"):
        old.unlink()
    reader = pacsv.open_csv(
        source,
        read_options=pacsv.ReadOptions(block_size=64 << 20),
        convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in columns}, strings_can_be_null=False),
    )
    names = [columns[c] for c in reader.schema.names]
    if set(reader.schema.names) != set(columns):
        raise ValueError(f"{name}: the header {reader.schema.names} is not the one this build was written for")
    row = 0
    part = 0
    writer: pq.ParquetWriter | None = None
    in_part = 0
    schema = pa.schema([("source_row", pa.int64()), *[(n, pa.string()) for n in names]])
    for batch in reader:
        batch = batch.rename_columns(names)
        n = batch.num_rows
        ids = pa.array(range(row, row + n), type=pa.int64())
        table = pa.Table.from_batches([batch]).add_column(0, "source_row", ids).cast(schema)
        if writer is None or in_part >= ROWS_PER_FILE:
            if writer is not None:
                writer.close()
            writer = pq.ParquetWriter(out_dir / f"part-{part:03d}.parquet", schema, compression="zstd")
            part += 1
            in_part = 0
        writer.write_table(table, row_group_size=1_000_000)
        in_part += n
        row += n
    if writer is not None:
        writer.close()
    summary = {"source": name, "rows": row, "files": part}
    (out_dir / "_staged.json").write_text(json.dumps(summary) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", choices=list(SOURCES))
    args = parser.parse_args()
    for name in SOURCES:
        if args.only and name != args.only:
            continue
        print(stage(name))
    return 0


if __name__ == "__main__":
    sys.exit(main())
