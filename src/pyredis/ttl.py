"""TTL expiration management built around a min-heap."""

from __future__ import annotations

import heapq
import time
from dataclasses import dataclass, field


@dataclass(order=True, slots=True)
class ExpiryEntry:
    expires_at: float
    sequence: int
    key: str = field(compare=False)
    version: int = field(compare=False)


class TTLHeap:
    def __init__(self) -> None:
        self._heap: list[ExpiryEntry] = []
        self._sequence = 0

    def schedule(self, key: str, expires_at: float, version: int) -> None:
        self._sequence += 1
        heapq.heappush(self._heap, ExpiryEntry(expires_at, self._sequence, key, version))

    def pop_due(self, now: float | None = None) -> list[ExpiryEntry]:
        current = time.time() if now is None else now
        due: list[ExpiryEntry] = []
        while self._heap and self._heap[0].expires_at <= current:
            due.append(heapq.heappop(self._heap))
        return due

