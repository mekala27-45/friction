"""A module shipped by value must not carry a loader the workers cannot import."""

from __future__ import annotations

import subprocess
import sys
import types

import pytest

pytest.importorskip("pyspark")

LOAD = "import sys; from pyspark import cloudpickle; f = cloudpickle.loads(sys.stdin.buffer.read()); print(f.__name__)"


class _WorkspaceLoader:
    """Stands in for the workspace's file loader, from a module only the driver has."""


def _plant_loader(package: types.ModuleType) -> dict[str, tuple[object, object]]:
    fake = types.ModuleType("dbruntime_standin")
    _WorkspaceLoader.__module__ = fake.__name__
    fake._WorkspaceLoader = _WorkspaceLoader  # type: ignore[attr-defined]
    sys.modules[fake.__name__] = fake
    saved = {}
    for name, module in list(sys.modules.items()):
        if name == package.__name__ or name.startswith(package.__name__ + "."):
            saved[name] = (module.__loader__, module.__spec__)
            module.__loader__ = _WorkspaceLoader()
    return saved


def _load_elsewhere(payload: bytes) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, "-c", LOAD], input=payload.decode("latin-1"), capture_output=True,
                          text=True, encoding="latin-1")  # fmt: skip


def test_shipped_code_unpickles_where_the_loader_does_not_exist() -> None:
    import friction_features
    from friction_core.shipping import ship
    from friction_features import batch
    from pyspark import cloudpickle

    saved = _plant_loader(friction_features)
    try:
        cloudpickle.register_pickle_by_value(friction_features)
        before = _load_elsewhere(cloudpickle.dumps(batch.payments_partition))
        assert before.returncode != 0 and "dbruntime_standin" in before.stderr
        shipped = ship(friction_features)
        assert "friction_features.batch" in shipped
        after = _load_elsewhere(cloudpickle.dumps(batch.payments_partition))
        assert after.returncode == 0, after.stderr[-500:]
        assert after.stdout.strip() == "payments_partition"
    finally:
        cloudpickle.unregister_pickle_by_value(friction_features)
        for name, (loader, spec) in saved.items():
            sys.modules[name].__loader__ = loader
            sys.modules[name].__spec__ = spec  # type: ignore[assignment]
        sys.modules.pop("dbruntime_standin", None)
