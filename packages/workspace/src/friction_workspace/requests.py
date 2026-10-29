"""Workspace runs are requested by commit.

GitHub Actions is the only machine that holds the workspace host and token. A run is asked for by
committing a JSON file under requests/; the workspace workflow runs every request that has no
record on the results branch yet, writes what it produced there, and the build machine promotes
the results into results/ after review, with results/provenance.json saying which request, run and
commit each file came from.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from friction_core.model import StrictModel
from pydantic import Field

Kind = Literal[
    "probe",
    "secrets",
    "upload_sources",
    "bundle_deploy",
    "bundle_run",
    "sql",
    "export",
    "terraform_plan",
    "terraform_apply",
    "portable",
]


class Request(StrictModel):
    id: str
    kind: Kind
    target: Literal["dev", "it", "prod"] = "dev"
    job: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
    note: str


def load(path: Path) -> Request:
    data = json.loads(path.read_text(encoding="utf-8"))
    request = Request.model_validate(data)
    if request.id != path.stem:
        raise ValueError(f"{path.name}: id {request.id!r} must equal the file name")
    return request


def pending(requests_dir: Path, done_dir: Path) -> list[Request]:
    """Requests in name order that have no done record on the results branch."""
    out: list[Request] = []
    for path in sorted(requests_dir.glob("*.json")):
        request = load(path)
        if not (done_dir / request.id / "done.json").exists():
            out.append(request)
    return out
