"""The manifest: every number friction publishes, with where it came from.

The README, RESULTS, the briefing, the model cards, the validation reports and the site read their
figures from results/manifest.json and nothing else. Each entry carries a format code (so it prints
one way everywhere) and a provenance: the step that produced it, the population, and the source.

Sources: ``workspace`` for anything read from a result the Databricks workspace exported and the
repository promoted; ``local`` for the open source Spark run on the build machine; ``evaluation``
for a grade computed in the repository from exported scored sets; ``simulator`` for known truth;
``published`` for figures read from the RBA data set repository; ``plan`` for registered parameters;
``live`` for the deployed API; ``build`` and ``static`` for the build itself.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any

from pydantic import Field

from friction_core.formats import FORMATS, format_value
from friction_core.model import StrictModel

Scalar = float | int | str | None

SOURCES = frozenset(
    {
        "workspace",
        "local",
        "evaluation",
        "simulator",
        "published",
        "plan",
        "live",
        "build",
        "static",
    }
)


class Provenance(StrictModel):
    origin: str
    population: str
    source: str
    as_of: str
    note: str | None = None


class Value(StrictModel):
    value: Scalar
    fmt: str
    provenance: Provenance


class Table(StrictModel):
    columns: list[str]
    formats: list[str]
    rows: list[list[Scalar]]
    provenance: Provenance


class Figure(StrictModel):
    title: str
    subtitle: str
    sql: str | None = None
    data: str | None = None
    provenance: Provenance


class Manifest(StrictModel):
    as_of: str
    values: dict[str, Value] = Field(default_factory=dict)
    tables: dict[str, Table] = Field(default_factory=dict)
    figures: dict[str, Figure] = Field(default_factory=dict)

    def text(self, key: str) -> str:
        if key not in self.values:
            raise KeyError(f"manifest has no value {key!r}")
        entry = self.values[key]
        return format_value(entry.value, entry.fmt)

    def raw(self, key: str) -> Scalar:
        if key not in self.values:
            raise KeyError(f"manifest has no value {key!r}")
        return self.values[key].value

    def table_markdown(self, key: str) -> str:
        if key not in self.tables:
            raise KeyError(f"manifest has no table {key!r}")
        t = self.tables[key]
        head = "| " + " | ".join(t.columns) + " |"
        rule = "|" + "|".join("---:" if f != "text" else "---" for f in t.formats) + "|"
        body = [
            "| " + " | ".join(format_value(v, f) for v, f in zip(row, t.formats, strict=True)) + " |"
            for row in t.rows
        ]
        return "\n".join([head, rule, *body])

    def label(self, key: str) -> str:
        return self.figures[key].title


class ManifestBuilder:
    """Collects entries from the steps of the build, refusing a format or source it does not know."""

    def __init__(self, as_of: str) -> None:
        self.as_of = as_of
        self._values: dict[str, Value] = {}
        self._tables: dict[str, Table] = {}
        self._figures: dict[str, Figure] = {}

    def _prov(self, origin: str, population: str, source: str, note: str | None) -> Provenance:
        if source not in SOURCES:
            raise ValueError(f"unknown source {source!r} for a manifest entry")
        return Provenance(origin=origin, population=population, source=source, as_of=self.as_of, note=note)

    def value(
        self,
        key: str,
        value: Scalar,
        fmt: str,
        *,
        origin: str,
        population: str,
        source: str,
        note: str | None = None,
    ) -> None:
        if fmt not in FORMATS:
            raise KeyError(f"unknown format {fmt!r} for {key}")
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"{key} is not a finite number")
        if key in self._values:
            raise KeyError(f"{key} was written twice")
        self._values[key] = Value(
            value=value, fmt=fmt, provenance=self._prov(origin, population, source, note)
        )

    def table(
        self,
        key: str,
        columns: Sequence[str],
        formats: Sequence[str],
        rows: Iterable[Sequence[Scalar]],
        *,
        origin: str,
        population: str,
        source: str,
        note: str | None = None,
    ) -> None:
        if len(columns) != len(formats):
            raise ValueError(f"{key}: columns and formats differ in length")
        for f in formats:
            if f not in FORMATS:
                raise KeyError(f"unknown format {f!r} in {key}")
        materialized = [list(r) for r in rows]
        for r in materialized:
            if len(r) != len(columns):
                raise ValueError(f"{key}: a row has {len(r)} cells for {len(columns)} columns")
        self._tables[key] = Table(
            columns=list(columns),
            formats=list(formats),
            rows=materialized,
            provenance=self._prov(origin, population, source, note),
        )

    def figure(
        self,
        key: str,
        title: str,
        subtitle: str,
        *,
        origin: str,
        population: str,
        source: str,
        sql: str | None = None,
        data: str | None = None,
    ) -> None:
        self._figures[key] = Figure(
            title=title,
            subtitle=subtitle,
            sql=sql,
            data=data,
            provenance=self._prov(origin, population, source, None),
        )

    def merge(self, other: Mapping[str, Any]) -> None:
        """Merge the entries another step saved, so steps can run apart and the manifest is assembled once."""
        for k, v in other.get("values", {}).items():
            if k in self._values:
                raise KeyError(f"{k} was written twice")
            self._values[k] = Value.model_validate(v)
        for k, v in other.get("tables", {}).items():
            self._tables[k] = Table.model_validate(v)
        for k, v in other.get("figures", {}).items():
            self._figures[k] = Figure.model_validate(v)

    def build(self) -> Manifest:
        return Manifest(
            as_of=self.as_of,
            values=dict(sorted(self._values.items())),
            tables=dict(sorted(self._tables.items())),
            figures=dict(sorted(self._figures.items())),
        )

    def dump(self) -> dict[str, Any]:
        data: dict[str, Any] = json.loads(self.build().model_dump_json())
        return data


def load(path: Path) -> Manifest:
    return Manifest.model_validate_json(path.read_text(encoding="utf-8"))


def write(manifest: Manifest, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(json.loads(manifest.model_dump_json()), indent=1, sort_keys=False, ensure_ascii=False)
    path.write_text(text + "\n", encoding="utf-8")
