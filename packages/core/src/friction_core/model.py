"""StrictModel, ported from trajectory: every contract, request and response is one of these."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class StrictModel(BaseModel):
    """Frozen, no extra fields, validated on assignment. The default for anything that crosses a boundary."""

    model_config = ConfigDict(extra="forbid", frozen=True, validate_assignment=True, str_strip_whitespace=False)


class MutableStrictModel(BaseModel):
    """The same rules without the freeze, for builders that fill a record in steps."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)
