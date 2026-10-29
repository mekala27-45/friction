"""Ship the project's packages to Spark's Python workers by value.

The workspace imports the packages from the bundle's synced files, which the UDF workers do not
have, so their code travels inside the pickled UDF (cloudpickle by value). A module pickled by value
carries its whole namespace, and on Databricks that includes the module's loader: the workspace
imports files through its own loader class, from a runtime module the workers cannot import, and
unpickling then fails with ModuleNotFoundError: dbruntime. ship() imports every submodule first,
registers each package for pickling by value, and clears the loader and spec of every module it
ships. Nothing imports these modules again after it runs, so nothing needs the spec.
"""

from __future__ import annotations

import importlib
import pkgutil
import sys
from types import ModuleType


def ship(*packages: ModuleType) -> list[str]:
    from pyspark import cloudpickle

    for package in packages:
        for info in pkgutil.walk_packages(package.__path__, package.__name__ + "."):
            importlib.import_module(info.name)
        cloudpickle.register_pickle_by_value(package)  # type: ignore[no-untyped-call]
    names = tuple(p.__name__ for p in packages)
    shipped = []
    for name, module in list(sys.modules.items()):
        if module is not None and any(name == n or name.startswith(n + ".") for n in names):
            module.__loader__ = None
            module.__spec__ = None
            shipped.append(name)
    return sorted(shipped)
