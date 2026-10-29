"""One local Spark Connect session for every Spark test in the package."""

from __future__ import annotations

import pytest


@pytest.fixture(scope="session")
def spark():  # type: ignore[no-untyped-def]
    from friction_transforms import session

    return session.get(cores=2, shuffle_partitions=4)
