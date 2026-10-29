"""Run a committed SQL file through the serverless SQL warehouse and save each result as JSON.

Statements are separated by a line holding only ``-- statement``; a statement whose first comment
line names ``-- save: <name>`` has its rows written to <name>.json. Variables in ${name} form are
filled from the request and from the probe, never from anything a table returned.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

_SAVE = re.compile(r"^--\s*save:\s*([a-z0-9_]+)\s*$", re.M)


def split(text: str) -> list[str]:
    parts = re.split(r"^--\s*statement\s*$", text, flags=re.M)
    return [p.strip() for p in parts if p.strip()]


def fill(statement: str, values: dict[str, Any]) -> str:
    def sub(m: re.Match[str]) -> str:
        key = m.group(1)
        if key not in values:
            raise KeyError(f"no value for ${{{key}}}")
        return str(values[key])

    return re.sub(r"\$\{([a-z_]+)\}", sub, statement)


def run_file(path: Path, out: Path, values: dict[str, Any]) -> dict[str, Any]:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.sql import StatementState

    w = WorkspaceClient()
    warehouse = next(iter(w.warehouses.list()))
    saved: list[str] = []
    for statement in split(path.read_text(encoding="utf-8")):
        sql = fill(statement, values)
        resp = w.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse.id, wait_timeout="50s")
        while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
            resp = w.statement_execution.get_statement(resp.statement_id)
        if not resp.status or resp.status.state != StatementState.SUCCEEDED:
            message = resp.status.error.message if resp.status and resp.status.error else "no status"
            raise RuntimeError(f"statement failed: {message}")
        name = _SAVE.search(statement)
        if name:
            columns = [c.name for c in resp.manifest.schema.columns] if resp.manifest and resp.manifest.schema else []
            rows = resp.result.data_array if resp.result and resp.result.data_array else []
            (out / f"{name.group(1)}.json").write_text(
                json.dumps({"columns": columns, "rows": rows}, indent=1) + "\n", encoding="utf-8"
            )
            saved.append(name.group(1))
    return {"saved": saved}
