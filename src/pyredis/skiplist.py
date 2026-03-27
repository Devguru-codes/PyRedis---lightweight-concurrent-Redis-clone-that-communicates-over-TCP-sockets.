"""Minimal skip list for sorted set support."""

from __future__ import annotations

import random
from dataclasses import dataclass, field


MAX_LEVEL = 8
P_FACTOR = 0.5


@dataclass(slots=True)
class SkipNode:
    score: float
    member: str
    forward: list[SkipNode | None] = field(default_factory=list)


class SkipList:
    def __init__(self) -> None:
        self.level = 1
        self.head = SkipNode(float("-inf"), "", [None] * MAX_LEVEL)
        self.length = 0

    def __len__(self) -> int:
        return self.length

    def insert(self, score: float, member: str) -> None:
        update = [self.head] * MAX_LEVEL
        current = self.head
        for index in range(self.level - 1, -1, -1):
            while current.forward[index] and self._less(current.forward[index], score, member):
                current = current.forward[index]
            update[index] = current

        level = self._random_level()
        if level > self.level:
            for index in range(self.level, level):
                update[index] = self.head
            self.level = level

        node = SkipNode(score, member, [None] * level)
        for index in range(level):
            node.forward[index] = update[index].forward[index]
            update[index].forward[index] = node
        self.length += 1

    def remove(self, score: float, member: str) -> bool:
        update = [self.head] * MAX_LEVEL
        current = self.head
        for index in range(self.level - 1, -1, -1):
            while current.forward[index] and self._less(current.forward[index], score, member):
                current = current.forward[index]
            update[index] = current

        target = current.forward[0]
        if target is None or target.score != score or target.member != member:
            return False

        for index in range(self.level):
            if update[index].forward[index] is not target:
                continue
            update[index].forward[index] = target.forward[index]

        while self.level > 1 and self.head.forward[self.level - 1] is None:
            self.level -= 1
        self.length -= 1
        return True

    def range(self, start: int, stop: int) -> list[str]:
        if self.length == 0:
            return []
        if start < 0:
            start = self.length + start
        if stop < 0:
            stop = self.length + stop
        start = max(start, 0)
        stop = min(stop, self.length - 1)
        if start > stop:
            return []

        index = 0
        current = self.head.forward[0]
        values: list[str] = []
        while current is not None and index <= stop:
            if index >= start:
                values.append(current.member)
            current = current.forward[0]
            index += 1
        return values

    def _random_level(self) -> int:
        level = 1
        while level < MAX_LEVEL and random.random() < P_FACTOR:
            level += 1
        return level

    @staticmethod
    def _less(node: SkipNode, score: float, member: str) -> bool:
        return (node.score, node.member) < (score, member)

