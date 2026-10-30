"""The live check's verdicts: a decision the other client cannot read, or reads differently, fails."""

from __future__ import annotations

from scripts import live_check


def _slip(d: str, verdict: str = "approve", score: float = 0.1) -> dict[str, object]:
    return {"decision": d, "verdict": verdict, "score": score}


def test_decisions_read_back_equal_pass_and_others_fail() -> None:
    printed = [_slip("a"), _slip("b", "decline", 0.9), _slip("c")]
    stored = {"a": {"verdict": "approve", "score": 0.1}, "b": {"verdict": "step_up", "score": 0.9}, "c": None}
    problems = {m["decision"]: m["problem"] for m in live_check.compare(printed, stored)}  # type: ignore[arg-type]
    assert problems == {"b": "differs", "c": "not found"}


def test_failures_name_routes_mode_and_slips() -> None:
    result = {
        "routes": [{"route": "/", "status": 200, "statement": True}, {"route": "/models/", "status": 404,
                                                                     "statement": False}],
        "replay": {"mode": "recorded", "live_slips": 0, "mismatches": []},
    }  # fmt: skip
    bad = live_check.failures(result, api=True)
    assert bad == ["route /models/", "replay mode recorded", f"0 of {live_check.EVENTS} slips live"]
    assert live_check.failures({"routes": result["routes"][:1]}, api=False) == []
