"""Stand ins for the few Databricks SDK types the workspace modules import, when CI lacks the SDK.

CI installs the spark extra, not the workspace one (only the workspace workflow holds a token). The
serving and Genie checks import an enum and two config classes from the SDK; these stand ins carry
the same names and values, and the real SDK is used whenever it is installed.
"""

from __future__ import annotations

import enum
import importlib.util
import sys
import types
from dataclasses import dataclass, field
from typing import Any

import pytest


class StatementState(enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass
class ServedEntityInput:
    entity_name: str
    entity_version: str
    workload_size: str
    scale_to_zero_enabled: bool


@dataclass
class EndpointCoreConfigInput:
    name: str
    served_entities: list[Any] = field(default_factory=list)


@pytest.fixture(autouse=True)
def databricks_sdk_types(monkeypatch: pytest.MonkeyPatch) -> None:
    if importlib.util.find_spec("databricks") is not None and importlib.util.find_spec("databricks.sdk"):
        return
    names = {"databricks": None, "databricks.sdk": None, "databricks.sdk.service": None,
             "databricks.sdk.service.sql": {"StatementState": StatementState},
             "databricks.sdk.service.serving": {"ServedEntityInput": ServedEntityInput,
                                                "EndpointCoreConfigInput": EndpointCoreConfigInput}}  # fmt: skip
    for name, attrs in names.items():
        module = types.ModuleType(name)
        for k, v in (attrs or {}).items():
            setattr(module, k, v)
        monkeypatch.setitem(sys.modules, name, module)
