from __future__ import annotations

import os
import subprocess
import sys

import numpy as np
from friction_simulator import draws


def test_draws_depend_on_key_not_order() -> None:
    keys = np.arange(1000, dtype=np.int64)
    a = draws.uniform(keys, 1, "x")
    b = draws.uniform(keys[::-1], 1, "x")[::-1]
    assert np.array_equal(a, b)


def test_streams_and_seeds_differ() -> None:
    keys = np.arange(1000, dtype=np.int64)
    assert not np.array_equal(draws.uniform(keys, 1, "x"), draws.uniform(keys, 1, "y"))
    assert not np.array_equal(draws.uniform(keys, 1, "x"), draws.uniform(keys, 2, "x"))


def test_uniform_is_uniform_enough() -> None:
    u = draws.uniform(np.arange(200_000, dtype=np.int64), 7, "check")
    assert u.min() >= 0.0 and u.max() < 1.0
    hist, _ = np.histogram(u, bins=10, range=(0, 1))
    assert hist.min() > 19_000 and hist.max() < 21_000


def test_integers_inclusive_and_lognormal_median() -> None:
    k = np.arange(100_000, dtype=np.int64)
    ints = draws.integers(k, 2, 7, 1, "i")
    assert ints.min() == 2 and ints.max() == 7
    ln = draws.lognormal(k, 21.0, 0.8, 1, "l")
    assert abs(np.median(ln) - 21.0) < 0.5


def test_same_draws_under_two_hash_seeds() -> None:
    code = (
        "import numpy as np; from friction_simulator import draws; print(draws.uniform(np.arange(5), 3, 'z').tolist())"
    )
    outs = []
    for seed in ("0", "12345"):
        env = {**os.environ, "PYTHONHASHSEED": seed}
        outs.append(
            subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True).stdout
        )
    assert outs[0] == outs[1]
