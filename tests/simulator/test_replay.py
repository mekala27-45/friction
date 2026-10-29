from __future__ import annotations

import gzip
import io
import itertools
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from friction_simulator import config
from friction_simulator.replay import calendar, delays, drop_index, split_desk


def test_calendar_months_then_days() -> None:
    drops = calendar(date(2019, 1, 15), date(2019, 4, 10), 30)
    kinds = [d.kind for d in drops]
    assert kinds[:3] == ["month", "month", "month"] and kinds[-30:] == ["day"] * 30
    assert drops[-1].end == date(2019, 4, 11)
    assert [d.replay_day for d in drops[-30:]] == list(range(1, 31))
    # Drops cover time without gaps or overlaps.
    for a, b in itertools.pairwise(drops):
        assert a.end == b.start or (a.kind == "month" and b.start == a.end)
    names = [d.name for d in drops]
    assert names == sorted(names)


def test_drop_index_clips_to_last_drop() -> None:
    drops = calendar(date(2019, 1, 1), date(2019, 3, 31), 10)
    epoch = date(1970, 1, 1)
    days = np.array(
        [(date(2019, 1, 5) - epoch).days, (date(2019, 3, 31) - epoch).days, (date(2019, 5, 1) - epoch).days]
    )
    idx = drop_index(days, drops)
    assert idx[0] == 1 and idx[1] == len(drops) and idx[2] == len(drops)


def test_delays_shares() -> None:
    cfg = config.Lateness(share_late=0.1, share_one_day=0.8, max_days=7)
    d = delays(np.arange(100_000, dtype=np.int64), 1, "payments", cfg)
    assert abs((d > 0).mean() - 0.1) < 0.01
    late = d[d > 0]
    assert abs((late == 1).mean() - 0.8) < 0.02 and late.max() <= 7


@pytest.fixture
def staged_payments(tmp_path: Path) -> Path:
    rows = []
    start = pd.Timestamp("2020-01-01")
    for i in range(400):
        ts = start + pd.Timedelta(hours=i * 3)
        rows.append(
            {
                "source_row": i, "user": str(i % 5), "card": "0", "year": str(ts.year), "month": str(ts.month),
                "day": str(ts.day), "time": ts.strftime("%H:%M"), "amount": f"${i % 50}.25",
                "use_chip": "Chip Transaction", "merchant_name": str(10**18 + i % 7), "merchant_city": "Reno",
                "merchant_state": "NV", "zip": "89501.0", "mcc": "5411", "errors": "", "is_fraud": "No",
            }
        )  # fmt: skip
    folder = tmp_path / "staged"
    folder.mkdir()
    pq.write_table(pa.Table.from_pylist(rows), folder / "part-000.parquet")
    return folder


def _read(path: Path) -> pd.DataFrame:
    with gzip.open(path, "rb") as fh:
        return pd.read_csv(io.BytesIO(fh.read()), dtype=str, keep_default_na=False)


def test_split_writes_every_row_once_with_the_incident(staged_payments: Path, tmp_path: Path) -> None:
    inc = config.Incident(
        desk="payments",
        replay_day=10,
        faults=[
            config.Fault(kind="resend", of_day=9, name_suffix="resend"),
            config.Fault(kind="renamed_column", column="mcc", renamed_to="merchant_category", part=1),
            config.Fault(kind="clock_shift", hours=6, part=2),
        ],
    )
    out = tmp_path / "landing"
    entry = split_desk(
        "payments", staged_payments, out, seed=3, lateness=config.Lateness(share_late=0.0, share_one_day=1, max_days=7),
        daily_days=20, incident=inc, threads=1, memory_limit="500MB",
    )  # fmt: skip
    files = sorted((out / "payments" / "drops").rglob("*.csv.gz"))
    frames = {f.name: _read(f) for f in files}
    day10 = next(d for d in entry["drops"] if d["replay_day"] == 10)
    names = set(day10["files"])
    assert any(n.endswith("_resend.csv.gz") for n in names)
    part1 = next(v for k, v in frames.items() if k.endswith("_part1.csv.gz"))
    part2 = next(v for k, v in frames.items() if k.endswith("_part2.csv.gz"))
    assert "merchant_category" in part1.columns and "mcc" not in part1.columns
    assert "mcc" in part2.columns
    # Every source row lands exactly once outside the resend; the resend repeats day 9 byte for byte.
    refs = pd.concat([v["txn_ref"] for k, v in frames.items() if not k.endswith("_resend.csv.gz")])
    assert sorted(refs.astype(int)) == list(range(400))
    day9 = next(d for d in entry["drops"] if d["replay_day"] == 9)
    resend = next(v for k, v in day10["files"].items() if k.endswith("_resend.csv.gz"))
    assert resend["sha256"] == next(iter(day9["files"].values()))["sha256"]
    # The clock shifted part has every timestamp six hours later than the corrected redelivery.
    corrected = _read(next((out / "payments" / "redelivery").rglob("*.csv.gz")))
    merged = part2.merge(corrected, on="txn_ref", suffixes=("_s", "_c"))
    shifted = pd.to_datetime(
        merged["year_s"] + "-" + merged["month_s"] + "-" + merged["day_s"] + " " + merged["time_s"]
    )
    original = pd.to_datetime(
        merged["year_c"] + "-" + merged["month_c"] + "-" + merged["day_c"] + " " + merged["time_c"]
    )
    assert ((shifted - original) == pd.Timedelta(hours=6)).all() and len(merged) == len(part2)


def test_split_is_deterministic(staged_payments: Path, tmp_path: Path) -> None:
    kw = dict(
        seed=5,
        lateness=config.Lateness(share_late=0.2, share_one_day=0.5, max_days=7),
        daily_days=20,
        incident=None,
        threads=2,
        memory_limit="500MB",
    )
    a = split_desk("payments", staged_payments, tmp_path / "a", **kw)  # type: ignore[arg-type]
    b = split_desk("payments", staged_payments, tmp_path / "b", **kw)  # type: ignore[arg-type]
    assert a == b
    assert a["late_rows"] > 0
