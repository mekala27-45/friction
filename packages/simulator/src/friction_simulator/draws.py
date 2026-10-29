"""Keyed draws: every random number in the simulator is a function of a seed and a natural key.

A draw for a row does not depend on which machine made it, how the rows were partitioned, or the
order they arrived in, because it is computed from the row's own key with SplitMix64 rather than
taken from a stream. This is stronger than sorting before a seeded sample (the Day 9 and Day 13
rule), and the tests run it under two PYTHONHASHSEED values to show Python's hash is not involved.
"""

from __future__ import annotations

import hashlib

import numpy as np
import numpy.typing as npt

_MASK = np.uint64(0xFFFFFFFFFFFFFFFF)


def stream_key(*parts: str | int) -> int:
    """A 64 bit key for a named stream, so two purposes never share draws."""
    text = "|".join(str(p) for p in parts).encode()
    return int.from_bytes(hashlib.sha256(text).digest()[:8], "little")


def _splitmix64(x: npt.NDArray[np.uint64]) -> npt.NDArray[np.uint64]:
    with np.errstate(over="ignore"):
        z = x + np.uint64(0x9E3779B97F4A7C15)
        z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        return z ^ (z >> np.uint64(31))


def uniform(keys: npt.ArrayLike, *stream: str | int) -> npt.NDArray[np.float64]:
    """One uniform in [0, 1) per key, for the named stream."""
    k = np.asarray(keys, dtype=np.int64).astype(np.uint64)
    salt = np.uint64(stream_key(*stream))
    with np.errstate(over="ignore"):
        mixed = _splitmix64(_splitmix64(k ^ salt) + salt)
    return (mixed >> np.uint64(11)).astype(np.float64) / float(1 << 53)


def integers(keys: npt.ArrayLike, low: int, high: int, *stream: str | int) -> npt.NDArray[np.int64]:
    """Integers uniform on [low, high], inclusive."""
    u = uniform(keys, *stream)
    return (low + np.floor(u * (high - low + 1))).astype(np.int64)


def lognormal(keys: npt.ArrayLike, median: float, sigma: float, *stream: str | int) -> npt.NDArray[np.float64]:
    """Lognormal with the given median, by the inverse normal of a keyed uniform."""
    from scipy.special import ndtri

    u = np.clip(uniform(keys, *stream), 1e-12, 1 - 1e-12)
    return np.asarray(median * np.exp(sigma * ndtri(u)), dtype=np.float64)
