"""A Genie space over the payments decisions, created from dashboards/genie.yaml and graded.

Request kind genie. The space is replaced whole each time (one space per title), so the committed
file is the definition. Then every question in the file is asked in a new conversation, the SQL
Genie wrote and the first value it returned are recorded, the reference SQL is run on the warehouse,
and the two values are compared. The record keeps Genie's SQL, so a wrong answer can be read.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

TOLERANCE = 1e-6


def _id(text: str) -> str:
    return hashlib.md5(text.encode("utf-8"), usedforsecurity=False).hexdigest()


def serialized(spec: dict[str, Any], *, with_instructions: bool = True) -> str:
    """The space as Genie serializes it: tables, and the instructions and sample questions if asked."""
    space: dict[str, Any] = {
        "version": 1,
        "data_sources": {"tables": [{"identifier": t} for t in sorted(spec["tables"])]},
    }
    if with_instructions:
        ids = sorted((_id(q["question"]), q["question"]) for q in spec["questions"])
        questions = [{"id": i, "question": [text]} for i, text in ids]
        space["config"] = {"sample_questions": questions}
        lines = [line + "\n" for line in spec["instructions"].strip().splitlines()]
        space["instructions"] = {"text_instructions": [{"id": _id(spec["instructions"]), "content": lines}]}
    return json.dumps(space)


def first_value(rows: list[list[Any]] | None) -> Any:
    for row in rows or []:
        for cell in row:
            if cell is not None:
                return cell
    return None


def same(a: Any, b: Any) -> bool:
    try:
        x, y = float(a), float(b)
    except (TypeError, ValueError):
        return a is not None and str(a).strip().lower() == str(b).strip().lower()
    return abs(x - y) <= TOLERANCE * max(abs(y), 1.0)


def _statement(client: Any, warehouse: str, sql: str) -> list[list[Any]]:
    from databricks.sdk.service.sql import StatementState

    resp = client.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse, wait_timeout="50s")
    while resp.status and resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        resp = client.statement_execution.get_statement(resp.statement_id)
    if not resp.status or resp.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(resp.status.error.message if resp.status and resp.status.error else "no status")
    return list(resp.result.data_array or []) if resp.result else []


def replace_space(client: Any, warehouse: str, spec: dict[str, Any]) -> dict[str, Any]:
    for s in client.genie.list_spaces().spaces or []:
        if s.title == spec["title"]:
            client.genie.trash_space(s.space_id)
    out: dict[str, Any] = {}
    try:
        space = client.genie.create_space(warehouse_id=warehouse, serialized_space=serialized(spec),
                                          title=spec["title"], description=spec["description"])  # fmt: skip
        out["instructions"] = "accepted"
    except Exception as exc:  # the instructions format is the part most likely to change
        out["instructions"] = f"rejected: {type(exc).__name__}: {str(exc)[:300]}"
        space = client.genie.create_space(warehouse_id=warehouse, serialized_space=serialized(spec,
                                          with_instructions=False), title=spec["title"],
                                          description=spec["description"])  # fmt: skip
    out["space_id"] = space.space_id
    return out


def ask(client: Any, space_id: str, question: str) -> dict[str, Any]:
    msg = client.genie.start_conversation_and_wait(space_id, question)
    out: dict[str, Any] = {"status": str(getattr(msg.status, "value", msg.status)), "sql": None, "value": None}
    for att in msg.attachments or []:
        if att.query is not None:
            out["sql"] = att.query.query
            message_id = msg.message_id or getattr(msg, "id", None)
            result = client.genie.get_message_attachment_query_result(space_id, msg.conversation_id, message_id,
                                                                      att.attachment_id)  # fmt: skip
            sr = result.statement_response
            out["value"] = first_value(sr.result.data_array if sr and sr.result else None)
            break
        if att.text is not None and out.get("text") is None:
            out["text"] = (att.text.content or "")[:500]
    return out


def run(client: Any, params: dict[str, Any], root: Path, out: Path) -> dict[str, Any]:
    spec = yaml.safe_load((root / str(params.get("spec", "dashboards/genie.yaml"))).read_text(encoding="utf-8"))
    warehouse = str(next(iter(client.warehouses.list())).id)
    record: dict[str, Any] = {"title": spec["title"], "tables": sorted(spec["tables"]), "questions": []}
    record.update(replace_space(client, warehouse, spec))
    for q in spec["questions"]:
        want = first_value(_statement(client, warehouse, q["sql"]))
        try:
            got = ask(client, record["space_id"], q["question"])
        except Exception as exc:
            got = {"status": "error", "error": f"{type(exc).__name__}: {str(exc)[:300]}", "sql": None, "value": None}
        record["questions"].append({"question": q["question"], "reference": want, **got,
                                    "correct": got.get("value") is not None and same(got["value"], want)})  # fmt: skip
    record["correct"] = sum(q["correct"] for q in record["questions"])
    record["asked"] = len(record["questions"])
    record.pop("space_id")  # an id of the account's workspace, not needed outside it
    (out / "genie.json").write_text(json.dumps(record, indent=1, sort_keys=True, default=str) + "\n", encoding="utf-8")
    return {"asked": record["asked"], "correct": record["correct"], "instructions": record["instructions"]}
