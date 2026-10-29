"""The replay splitter: each source cut into the drops a bank's landing zone would receive.

One drop per calendar month for the history, then one per day for the final ``daily_days`` of the
desk. A stated share of events arrives late (most a day, a tail up to a week), so the pipeline
meets late data the way a bank does: an event lands in the drop of its arrival date, not its
event date. Payments drops carry no fraud flag and logins drops no takeover flag; those arrive
later in the label feeds, on their own clock.

Files are gzip CSV written with a fixed mtime, rows ordered by arrival then event time then key,
and digested over their uncompressed bytes into data/replay.lock.json. The planted incident is
applied to the payments drop of replay day 40 (config/incident.yaml).
"""

from __future__ import annotations

import gzip
import hashlib
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import duckdb
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

from friction_simulator import draws
from friction_simulator.config import Incident, Lateness

PAYMENT_COLUMNS = [
    "txn_ref", "user", "card", "year", "month", "day", "time", "amount", "use_chip",
    "merchant_name", "merchant_city", "merchant_state", "zip", "mcc", "errors",
]  # fmt: skip
LOGIN_COLUMNS = [
    "login_index", "login_timestamp", "user_id", "rtt_ms", "ip_address", "country", "region", "city",
    "asn", "user_agent", "browser", "os", "device_type", "login_successful", "is_attack_ip",
]  # fmt: skip


@dataclass(frozen=True)
class Drop:
    seq: int
    name: str
    kind: str  # "month" or "day"
    start: date
    end: date  # exclusive
    replay_day: int | None


def calendar(first: date, last: date, daily_days: int) -> list[Drop]:
    """Monthly drops up to the daily window, then one per day to the last day of data."""
    window = last - timedelta(days=daily_days - 1)
    drops: list[Drop] = []
    cursor = date(first.year, first.month, 1)
    while cursor < window:
        nxt = date(cursor.year + (cursor.month == 12), cursor.month % 12 + 1, 1)
        end = min(nxt, window)
        drops.append(Drop(len(drops) + 1, f"{len(drops) + 1:04d}_{cursor:%Y-%m}", "month", cursor, end, None))
        cursor = nxt
    for i in range(daily_days):
        d = window + timedelta(days=i)
        drops.append(Drop(len(drops) + 1, f"{len(drops) + 1:04d}_{d:%Y-%m-%d}", "day", d, d + timedelta(days=1), i + 1))
    return drops


def drop_index(arrival_days: np.ndarray, drops: list[Drop]) -> np.ndarray:
    """Map each arrival date (days since epoch) to the sequence number of the drop that holds it."""
    starts = np.array([(d.start - date(1970, 1, 1)).days for d in drops], dtype=np.int64)
    idx = np.searchsorted(starts, arrival_days, side="right") - 1
    return np.clip(idx, 0, len(drops) - 1) + 1


def delays(keys: np.ndarray, seed: int, desk: str, cfg: Lateness) -> np.ndarray:
    """Whole days of delivery delay per event: zero for most, one for most of the late ones."""
    late = draws.uniform(keys, seed, desk, "late") < cfg.share_late
    one = draws.uniform(keys, seed, desk, "one_day") < cfg.share_one_day
    tail = draws.integers(keys, 2, cfg.max_days, seed, desk, "tail_days")
    return np.where(late, np.where(one, 1, tail), 0).astype(np.int64)


class DigestingGzip:
    """A gzip file with a fixed mtime that also digests the uncompressed bytes it is given."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._raw = path.open("wb")
        self._gz = gzip.GzipFile(filename="", mode="wb", fileobj=self._raw, mtime=0, compresslevel=6)
        self.sha = hashlib.sha256()
        self.rows = 0

    def write_table(self, table: pa.Table, header: bool) -> None:
        sink = pa.BufferOutputStream()
        pacsv.write_csv(table, sink, write_options=pacsv.WriteOptions(include_header=header))
        data = sink.getvalue().to_pybytes()
        self.sha.update(data)
        self._gz.write(data)
        self.rows += table.num_rows

    def close(self) -> dict[str, Any]:
        self._gz.close()
        self._raw.close()
        return {"rows": self.rows, "sha256": self.sha.hexdigest()}


def _events_sql(desk: str, staged: Path) -> str:
    glob = (staged / "*.parquet").as_posix()
    if desk == "payments":
        cols = ", ".join(f'"{c}"' for c in PAYMENT_COLUMNS[1:])
        return f"""
            SELECT source_row AS txn_ref, {cols},
                   strptime(year || '-' || month || '-' || day || ' ' || time, '%Y-%m-%d %H:%M') AS event_ts
            FROM read_parquet('{glob}')
        """
    cols = ", ".join(f'"{c}"' for c in LOGIN_COLUMNS[1:])
    return f"""
        SELECT CAST(login_index AS BIGINT) AS login_index, {cols},
               strptime(login_timestamp, '%Y-%m-%d %H:%M:%S.%g') AS event_ts
        FROM read_parquet('{glob}')
    """


def _shift_clock(table: pa.Table, hours: int) -> pa.Table:
    ts = pc.add(table.column("event_ts"), pa.scalar(timedelta(hours=hours), type=pa.duration("us")))
    replacements = {
        "year": pc.strftime(ts, format="%Y"),
        "month": pc.cast(pc.cast(pc.strftime(ts, format="%m"), pa.int64()), pa.string()),
        "day": pc.cast(pc.cast(pc.strftime(ts, format="%d"), pa.int64()), pa.string()),
        "time": pc.strftime(ts, format="%H:%M"),
    }
    for name, values in replacements.items():
        table = table.set_column(table.schema.get_field_index(name), name, values)
    return table


def _batches(con: duckdb.DuckDBPyConnection, sql: str) -> Iterator[pa.RecordBatch]:
    reader = con.sql(sql).to_arrow_reader(500_000)
    yield from reader


def split_desk(
    desk: str,
    staged: Path,
    out_root: Path,
    *,
    seed: int,
    lateness: Lateness,
    daily_days: int,
    incident: Incident | None,
    threads: int = 2,
    memory_limit: str = "2500MB",
) -> dict[str, Any]:
    """Write every drop of a desk under out_root/<desk>/drops/<drop>/ and return its lock entry."""
    con = duckdb.connect()
    con.execute(f"SET threads = {threads}")
    con.execute("SET preserve_insertion_order = true")
    con.execute(f"SET memory_limit = '{memory_limit}'")
    spill = out_root.parent / "work" / "duckdb_spill"
    spill.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory = '{spill.as_posix()}'")
    key = "txn_ref" if desk == "payments" else "login_index"
    con.execute(f"CREATE TEMP VIEW ev AS {_events_sql(desk, staged)}")
    bad = con.execute("SELECT count(*) FROM ev WHERE event_ts IS NULL").fetchone()
    if bad and bad[0]:
        raise ValueError(f"{desk}: {bad[0]} rows have no event time; the first look must say why first")
    first, last = con.execute("SELECT min(event_ts)::DATE, max(event_ts)::DATE FROM ev").fetchone()  # type: ignore[misc]
    drops = calendar(first, last, daily_days)
    keys = con.execute(f"SELECT {key}, (event_ts::DATE - DATE '1970-01-01') AS d FROM ev ORDER BY {key}").fetchnumpy()
    k = np.asarray(keys[key], dtype=np.int64)
    arrival = np.asarray(keys["d"], dtype=np.int64) + delays(k, seed, desk, lateness)
    seqs = drop_index(arrival, drops)
    con.register(
        "assign",
        pa.table({"k": k, "drop_seq": seqs.astype(np.int32), "delay_days": arrival - np.asarray(keys["d"])}),
    )
    columns = PAYMENT_COLUMNS if desk == "payments" else LOGIN_COLUMNS
    select = ", ".join(f'ev."{c}"' for c in columns)
    sql = f"""
        SELECT a.drop_seq, a.delay_days, ev.event_ts, {select}
        FROM ev JOIN assign a ON ev.{key} = a.k
        ORDER BY a.drop_seq, ev.event_ts, ev.{key}
    """
    desk_root = out_root / desk
    if desk_root.exists():
        shutil.rmtree(desk_root)
    by_seq = {d.seq: d for d in drops}
    incident_day = incident.replay_day if incident and incident.desk == desk else None
    files: dict[int, dict[str, Any]] = {}
    writers: dict[str, DigestingGzip] = {}
    current: int | None = None
    late_rows = 0

    def close_all() -> None:
        for name, w in writers.items():
            files.setdefault(current or 0, {})[name] = w.close()
        writers.clear()

    for batch in _batches(con, sql):
        table = pa.Table.from_batches([batch])
        late_rows += int(pc.sum(pc.greater(table.column("delay_days"), 0)).as_py() or 0)
        seq_col = table.column("drop_seq").to_numpy()
        boundaries = np.flatnonzero(np.diff(seq_col)) + 1
        for chunk_start, chunk_end in zip(np.r_[0, boundaries], np.r_[boundaries, len(seq_col)], strict=True):
            chunk = table.slice(int(chunk_start), int(chunk_end - chunk_start))
            seq = int(seq_col[chunk_start])
            if seq != current:
                close_all()
                current = seq
            drop = by_seq[seq]
            folder = desk_root / "drops" / drop.name
            body = chunk.select(columns)
            if incident_day is not None and drop.replay_day == incident_day:
                _write_incident_parts(chunk, columns, folder, desk, drop, incident, writers, desk_root)  # type: ignore[arg-type]
                continue
            fname = f"{desk}_{drop.name.split('_', 1)[1]}.csv.gz"
            if fname not in writers:
                writers[fname] = DigestingGzip(folder / fname)
                writers[fname].write_table(body, header=True)
            else:
                writers[fname].write_table(body, header=False)
    close_all()
    if incident_day is not None:
        _resend(desk_root, drops, incident, files)  # type: ignore[arg-type]
    entry = {
        "first_day": first.isoformat(),
        "last_day": last.isoformat(),
        "window_start": drops[-daily_days].start.isoformat(),
        "rows": len(k),
        "late_rows": late_rows,
        "drops": [
            {
                "seq": d.seq,
                "name": d.name,
                "kind": d.kind,
                "replay_day": d.replay_day,
                "files": files.get(d.seq, {}),
            }
            for d in drops
        ],
    }
    return entry


def _write_incident_parts(
    chunk: pa.Table,
    columns: list[str],
    folder: Path,
    desk: str,
    drop: Drop,
    incident: Incident,
    writers: dict[str, DigestingGzip],
    desk_root: Path,
) -> None:
    """Day 40 arrives as two parts: one with a renamed column, one with its clock moved."""
    label = drop.name.split("_", 1)[1]
    keys = chunk.column(columns[0]).to_numpy()
    rename = next(f for f in incident.faults if f.kind == "renamed_column")
    shift = next(f for f in incident.faults if f.kind == "clock_shift")
    part1 = chunk.filter(pa.array(keys % 2 == 0))
    part2 = chunk.filter(pa.array(keys % 2 == 1))
    renamed = part1.select(columns).rename_columns(
        [rename.renamed_to if c == rename.column else c for c in columns]  # type: ignore[misc]
    )
    shifted = _shift_clock(part2, int(shift.hours or 0)).select(columns)
    for name, table in ((f"{desk}_{label}_part1.csv.gz", renamed), (f"{desk}_{label}_part2.csv.gz", shifted)):
        if name not in writers:
            writers[name] = DigestingGzip(folder / name)
            writers[name].write_table(table, header=True)
        else:
            writers[name].write_table(table, header=False)
    # What upstream sends again once the incident is understood: the day as it should have been.
    corrected = chunk.select(columns)
    name = f"{desk}_{label}_corrected.csv.gz"
    key = f"redelivery/{name}"
    if key not in writers:
        writers[key] = DigestingGzip(desk_root / "redelivery" / drop.name / name)
        writers[key].write_table(corrected, header=True)
    else:
        writers[key].write_table(corrected, header=False)


def _resend(desk_root: Path, drops: list[Drop], incident: Incident, files: dict[int, dict[str, Any]]) -> None:
    resend = next(f for f in incident.faults if f.kind == "resend")
    day = next(d for d in drops if d.replay_day == incident.replay_day)
    prev = next(d for d in drops if d.replay_day == resend.of_day)
    for name in sorted(files.get(prev.seq, {})):
        source = desk_root / "drops" / prev.name / name
        target_name = name.replace(".csv.gz", f"_{resend.name_suffix}.csv.gz")
        target = desk_root / "drops" / day.name / target_name
        shutil.copyfile(source, target)
        files.setdefault(day.seq, {})[target_name] = dict(files[prev.seq][name])


def write_feed(frame: pd.DataFrame, time_column: str, drops: list[Drop], out_root: Path, feed: str) -> dict[str, Any]:
    """Write a simulated feed (labels, CDC) into the same drop calendar, keyed by its own clock."""
    root = out_root / feed
    if root.exists():
        shutil.rmtree(root)
    # A record stamped after the last drop closes arrives after the replay ends; it is counted, not
    # written. One stamped before the first drop arrives with the first.
    horizon = pd.Timestamp(drops[-1].end)
    beyond = frame[time_column] >= horizon
    frame = frame[~beyond]
    days = (frame[time_column].dt.floor("D") - pd.Timestamp("1970-01-01")).dt.days.to_numpy()
    seqs = drop_index(days, drops)
    frame = frame.assign(_drop=seqs)
    entries: list[dict[str, Any]] = []
    for d in drops:
        part = frame[frame["_drop"] == d.seq].drop(columns="_drop")
        if part.empty:
            continue
        table = pa.Table.from_pandas(part, preserve_index=False)
        name = f"{feed}_{d.name.split('_', 1)[1]}.csv.gz"
        w = DigestingGzip(root / "drops" / d.name / name)
        w.write_table(table, header=True)
        entries.append({"seq": d.seq, "name": d.name, "files": {name: w.close()}})
    return {"rows": len(frame), "beyond_horizon": int(beyond.sum()), "drops": entries}
