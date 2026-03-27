"""Core in-memory datastore for PyRedis."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from .errors import CommandError, WrongTypeError
from .lru import LRUTracker
from .ttl import TTLHeap
from .zset import SortedSet


@dataclass(slots=True)
class Record:
    kind: str
    value: Any
    expires_at: float | None = None
    expiry_version: int = 0


class DataStore:
    def __init__(self, max_keys: int = 1024) -> None:
        self._records: dict[str, Record] = {}
        self._lock = asyncio.Lock()
        self._ttl_heap = TTLHeap()
        self._lru = LRUTracker()
        self._max_keys = max_keys

    @property
    def size(self) -> int:
        return len(self._records)

    async def get(self, key: str) -> str | None:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None:
                return None
            if record.kind != "string":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            self._lru.touch(key)
            return str(record.value)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        async with self._lock:
            self._purge_expired_locked()
            expires_at = time.time() + ex if ex is not None else None
            existing = self._records.get(key)
            version = 0 if existing is None else existing.expiry_version + 1
            self._records[key] = Record("string", value, expires_at, version)
            self._lru.touch(key)
            if expires_at is not None:
                self._ttl_heap.schedule(key, expires_at, version)
            self._evict_if_needed_locked()

    async def delete(self, *keys: str) -> int:
        deleted = 0
        async with self._lock:
            self._purge_expired_locked()
            for key in keys:
                if self._records.pop(key, None) is not None:
                    self._lru.remove(key)
                    deleted += 1
        return deleted

    async def exists(self, *keys: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            return sum(1 for key in keys if key in self._records)

    async def expire(self, key: str, seconds: int) -> bool:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None:
                return False
            record.expiry_version += 1
            record.expires_at = time.time() + seconds
            self._ttl_heap.schedule(key, record.expires_at, record.expiry_version)
            self._lru.touch(key)
            return True

    async def persist(self, key: str) -> bool:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None or record.expires_at is None:
                return False
            record.expiry_version += 1
            record.expires_at = None
            self._lru.touch(key)
            return True

    async def ttl(self, key: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None:
                return -2
            if record.expires_at is None:
                return -1
            return max(int(record.expires_at - time.time()), 0)

    async def incr(self, key: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None:
                record = Record("string", "0")
                self._records[key] = record
            if record.kind != "string":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            try:
                next_value = int(record.value) + 1
            except ValueError as exc:
                raise CommandError("ERR value is not an integer or out of range") from exc
            record.value = str(next_value)
            self._lru.touch(key)
            self._evict_if_needed_locked()
            return next_value

    async def flushall(self) -> None:
        async with self._lock:
            self._records.clear()
            self._lru = LRUTracker()
            self._ttl_heap = TTLHeap()

    async def info(self) -> dict[str, str]:
        async with self._lock:
            self._purge_expired_locked()
            return {
                "keys": str(len(self._records)),
                "max_keys": str(self._max_keys),
                "expiring_keys": str(sum(1 for record in self._records.values() if record.expires_at is not None)),
            }

    async def dbsize(self) -> int:
        async with self._lock:
            self._purge_expired_locked()
            return len(self._records)

    async def type_of(self, key: str) -> str:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None:
                return "none"
            self._lru.touch(key)
            return record.kind

    async def mget(self, keys: list[str]) -> list[str | None]:
        async with self._lock:
            self._purge_expired_locked()
            values: list[str | None] = []
            for key in keys:
                record = self._records.get(key)
                if record is None:
                    values.append(None)
                    continue
                if record.kind != "string":
                    raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
                self._lru.touch(key)
                values.append(str(record.value))
            return values

    async def mset(self, pairs: list[tuple[str, str]]) -> None:
        async with self._lock:
            self._purge_expired_locked()
            for key, value in pairs:
                existing = self._records.get(key)
                version = 0 if existing is None else existing.expiry_version + 1
                self._records[key] = Record("string", value, None, version)
                self._lru.touch(key)
            self._evict_if_needed_locked()

    async def zadd(self, key: str, pairs: list[tuple[float, str]]) -> int:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None:
                zset = SortedSet()
                record = Record("zset", zset)
                self._records[key] = record
            if record.kind != "zset":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            added = 0
            for score, member in pairs:
                added += record.value.add(score, member)
            self._lru.touch(key)
            self._evict_if_needed_locked()
            return added

    async def zrange(self, key: str, start: int, stop: int) -> list[str]:
        async with self._lock:
            self._purge_expired_locked()
            record = self._records.get(key)
            if record is None:
                return []
            if record.kind != "zset":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            self._lru.touch(key)
            return record.value.range(start, stop)

    async def purge_expired(self) -> int:
        async with self._lock:
            return self._purge_expired_locked()

    def _purge_expired_locked(self) -> int:
        removed = 0
        for entry in self._ttl_heap.pop_due():
            record = self._records.get(entry.key)
            if record is None:
                continue
            if record.expires_at != entry.expires_at or record.expiry_version != entry.version:
                continue
            self._records.pop(entry.key, None)
            self._lru.remove(entry.key)
            removed += 1
        return removed

    def _evict_if_needed_locked(self) -> None:
        while len(self._records) > self._max_keys:
            evicted_key = self._lru.pop_lru()
            if evicted_key is None:
                return
            self._records.pop(evicted_key, None)
