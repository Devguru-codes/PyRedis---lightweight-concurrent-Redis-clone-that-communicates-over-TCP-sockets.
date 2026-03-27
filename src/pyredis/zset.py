"""Sorted set implementation using a skip list plus member index."""

from __future__ import annotations

from .skiplist import SkipList


class SortedSet:
    def __init__(self) -> None:
        self._scores: dict[str, float] = {}
        self._skiplist = SkipList()

    def add(self, score: float, member: str) -> int:
        existing = self._scores.get(member)
        if existing is not None:
            if existing == score:
                return 0
            self._skiplist.remove(existing, member)
        self._scores[member] = score
        self._skiplist.insert(score, member)
        return 0 if existing is not None else 1

    def range(self, start: int, stop: int) -> list[str]:
        return self._skiplist.range(start, stop)

