"""The Genie request against a fake workspace: the space replaced, each answer graded on the reference SQL."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import yaml
from friction_workspace import genie

NS = SimpleNamespace
ROOT = Path(__file__).resolve().parents[2]


class FakeClient:
    def __init__(self, answers: dict[str, Any], refuse_instructions: bool = False) -> None:
        self.answers, self.refuse, self.created, self.trashed = answers, refuse_instructions, [], []
        self.warehouses = NS(list=lambda: [NS(id="wh")])
        self.statement_execution = NS(execute_statement=self._execute)
        spaces = NS(spaces=[NS(title="friction payments desk", space_id="old")])
        self.genie = NS(
            list_spaces=lambda: spaces,
            trash_space=self.trashed.append,
            create_space=self._create,
            start_conversation_and_wait=self._ask,
            get_message_attachment_query_result=self._result,
        )

    def _execute(self, statement: str, warehouse_id: str, wait_timeout: str) -> Any:
        from databricks.sdk.service.sql import StatementState

        return NS(status=NS(state=StatementState.SUCCEEDED), result=NS(data_array=[[str(len(statement) % 97)]]))

    def _create(self, warehouse_id: str, serialized_space: str, title: str, description: str) -> Any:
        space = json.loads(serialized_space)
        if self.refuse and "instructions" in space:
            raise ValueError("bad instructions")
        self.created.append(space)
        return NS(space_id="new")

    def _ask(self, space_id: str, content: str) -> Any:
        att = NS(attachment_id="a", query=NS(query=f"SELECT 1 -- {content}"), text=None)
        return NS(status=NS(value="COMPLETED"), attachments=[att], conversation_id="c", message_id=content)

    def _result(self, space_id: str, conversation_id: str, message_id: str, attachment_id: str) -> Any:
        return NS(statement_response=NS(result=NS(data_array=[[None, self.answers.get(message_id)]])))


def _spec() -> dict[str, Any]:
    return yaml.safe_load((ROOT / "dashboards" / "genie.yaml").read_text())


def test_each_answer_is_graded_on_the_reference_sql(tmp_path: Path) -> None:
    spec = _spec()
    right = {q["question"]: str(len(q["sql"]) % 97) for q in spec["questions"][:2]}
    client = FakeClient(right)
    result = genie.run(client, {}, ROOT, tmp_path)
    record = json.loads((tmp_path / "genie.json").read_text())
    assert result == {"asked": len(spec["questions"]), "correct": 2, "instructions": "accepted"}
    assert client.trashed == ["old"] and "space_id" not in record
    assert [q["correct"] for q in record["questions"]][:2] == [True, True]
    assert all(q["sql"].startswith("SELECT 1") for q in record["questions"])
    space = client.created[0]
    assert sorted(t["identifier"] for t in space["data_sources"]["tables"]) == sorted(spec["tables"])
    assert len(space["config"]["sample_questions"]) == len(spec["questions"])


def test_a_rejected_instruction_block_falls_back_to_the_tables(tmp_path: Path) -> None:
    client = FakeClient({}, refuse_instructions=True)
    result = genie.run(client, {}, ROOT, tmp_path)
    assert result["instructions"].startswith("rejected: ValueError") and result["correct"] == 0
    assert "instructions" not in client.created[0]


def test_values_compare_as_numbers_or_words() -> None:
    assert genie.same("1234.0", 1234) and genie.same(" P03 ", "p03") and not genie.same("12", "13")
    assert genie.first_value([[None, None], [None, 5]]) == 5 and genie.first_value(None) is None
