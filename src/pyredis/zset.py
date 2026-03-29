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

    def range_withscores(self, start: int, stop: int) -> list[tuple[str, float]]:
        members = self._skiplist.range(start, stop)
        return [(member, self._scores[member]) for member in members]

    def score(self, member: str) -> float | None:
        return self._scores.get(member)

    def remove(self, member: str) -> bool:
        score = self._scores.pop(member, None)
        if score is None:
            return False
        self._skiplist.remove(score, member)
        return True

    def card(self) -> int:
        return len(self._scores)

    def rank(self, member: str) -> int | None:
        if member not in self._scores:
            return None
        ordered = sorted(self._scores.items(), key=lambda item: (item[1], item[0]))
        for index, (candidate, _score) in enumerate(ordered):
            if candidate == member:
                return index
        return None

    def items(self) -> list[tuple[str, float]]:
        return [(member, score) for member, score in sorted(self._scores.items(), key=lambda item: (item[1], item[0]))]
