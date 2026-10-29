"""The interior test (the Day 12 rule): an operating point printed beside its neighbours.

A result that holds at the registered setting and flips at the first neighbour is not a result.
For each parameter the champion's test number is printed at the setting and at each neighbour, and
the word degenerate is printed when the champion changes at the first neighbour.
"""

from __future__ import annotations

from collections.abc import Mapping


def champion(scores: Mapping[str, float]) -> str:
    return max(sorted(scores), key=lambda m: scores[m])


def verdict(by_setting: Mapping[str, Mapping[str, float]], registered: str, first_neighbours: list[str]) -> str:
    """by_setting: setting -> model -> metric. Returns 'stable' or 'degenerate'."""
    base = champion(by_setting[registered])
    for n in first_neighbours:
        if n in by_setting and champion(by_setting[n]) != base:
            return "degenerate"
    return "stable"
