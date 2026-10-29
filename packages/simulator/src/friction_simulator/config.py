"""The simulator and replay configuration, read from config/*.yaml into strict models."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from friction_core.model import StrictModel
from friction_core.paths import config_dir


class Range(StrictModel):
    min: int
    max: int


class Lateness(StrictModel):
    share_late: float
    share_one_day: float
    max_days: int


class DeskReplay(StrictModel):
    source: Literal["tabformer", "rba"]
    daily_days: int
    natural_key: str


class Replay(StrictModel):
    seed: int
    desks: dict[str, DeskReplay]
    lateness: Lateness


class LagSetting(StrictModel):
    median_days: float
    sigma: float
    max_days: int
    never_reported: float


class Chargebacks(StrictModel):
    incident_gap_days: int
    settings: dict[str, LagSetting]
    false_claim_rate: float
    reversal_days: Range


class Lifecycle(StrictModel):
    issue_before_first_use_days: Range
    reissue_after_block_days: Range
    limit_changes_per_year: float
    credit_limit_usd: Range
    close_after_last_use_days: Range
    share_out_of_order: float
    share_duplicated: float


class Takeovers(StrictModel):
    report_lag_days: Range


class Attackers(StrictModel):
    users: int
    attempts_per_user: int
    models: list[Literal["naive", "vpn", "targeted"]]


class Simulator(StrictModel):
    seed: int
    chargebacks: Chargebacks
    lifecycle: Lifecycle
    takeovers: Takeovers
    attackers: Attackers


class Fault(StrictModel):
    kind: Literal["resend", "renamed_column", "clock_shift"]
    of_day: int | None = None
    name_suffix: str | None = None
    column: str | None = None
    renamed_to: str | None = None
    hours: int | None = None
    part: int | None = None


class Incident(StrictModel):
    desk: str
    replay_day: int
    faults: list[Fault]


def _load(name: str, folder: Path | None = None) -> object:
    return yaml.safe_load(((folder or config_dir()) / name).read_text(encoding="utf-8"))


def replay(folder: Path | None = None) -> Replay:
    return Replay.model_validate(_load("replay.yaml", folder))


def simulator(folder: Path | None = None) -> Simulator:
    return Simulator.model_validate(_load("simulator.yaml", folder))


def incident(folder: Path | None = None) -> Incident:
    return Incident.model_validate(_load("incident.yaml", folder))
