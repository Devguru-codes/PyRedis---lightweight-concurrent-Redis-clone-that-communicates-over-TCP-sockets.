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
        self._metrics = {
            "read_hits": 0,
            "read_misses": 0,
            "expired_keys": 0,
            "evicted_keys": 0,
            "total_reads": 0,
            "total_writes": 0,
            "snapshot_saves": 0,
            "snapshot_loads": 0,
        }

    @property
    def size(self) -> int:
        return len(self._records)

    async def get(self, key: str) -> str | None:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += 1
            record = self._records.get(key)
            if record is None:
                self._metrics["read_misses"] += 1
                return None
            if record.kind != "string":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            self._lru.touch(key)
            self._metrics["read_hits"] += 1
            return str(record.value)

    async def set(self, key: str, value: str, ex: int | None = None) -> None:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += 1
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
            self._metrics["total_writes"] += 1
            for key in keys:
                if self._records.pop(key, None) is not None:
                    self._lru.remove(key)
                    deleted += 1
        return deleted

    async def exists(self, *keys: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += 1
            return sum(1 for key in keys if key in self._records)

    async def expire(self, key: str, seconds: int) -> bool:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += 1
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
            self._metrics["total_writes"] += 1
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
            self._metrics["total_reads"] += 1
            record = self._records.get(key)
            if record is None:
                return -2
            if record.expires_at is None:
                return -1
            return max(int(record.expires_at - time.time()), 0)

    async def incr(self, key: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += 1
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
            self._metrics["total_writes"] += 1

    async def info(self) -> dict[str, str]:
        async with self._lock:
            self._purge_expired_locked()
            return {
                "keys": str(len(self._records)),
                "max_keys": str(self._max_keys),
                "expiring_keys": str(sum(1 for record in self._records.values() if record.expires_at is not None)),
                **{key: str(value) for key, value in self._metrics.items()},
            }

    async def dbsize(self) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += 1
            return len(self._records)

    async def type_of(self, key: str) -> str:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += 1
            record = self._records.get(key)
            if record is None:
                return "none"
            self._lru.touch(key)
            return record.kind

    async def mget(self, keys: list[str]) -> list[str | None]:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += len(keys)
            values: list[str | None] = []
            for key in keys:
                record = self._records.get(key)
                if record is None:
                    self._metrics["read_misses"] += 1
                    values.append(None)
                    continue
                if record.kind != "string":
                    raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
                self._lru.touch(key)
                self._metrics["read_hits"] += 1
                values.append(str(record.value))
            return values

    async def mset(self, pairs: list[tuple[str, str]]) -> None:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += len(pairs)
            for key, value in pairs:
                existing = self._records.get(key)
                version = 0 if existing is None else existing.expiry_version + 1
                self._records[key] = Record("string", value, None, version)
                self._lru.touch(key)
            self._evict_if_needed_locked()

    async def setnx(self, key: str, value: str) -> bool:
        async with self._lock:
            self._purge_expired_locked()
            if key in self._records:
                return False
            self._metrics["total_writes"] += 1
            self._records[key] = Record("string", value)
            self._lru.touch(key)
            self._evict_if_needed_locked()
            return True

    async def append(self, key: str, suffix: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += 1
            record = self._records.get(key)
            if record is None:
                record = Record("string", "")
                self._records[key] = record
            if record.kind != "string":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            record.value = str(record.value) + suffix
            self._lru.touch(key)
            self._evict_if_needed_locked()
            return len(str(record.value))

    async def strlen(self, key: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += 1
            record = self._records.get(key)
            if record is None:
                self._metrics["read_misses"] += 1
                return 0
            if record.kind != "string":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            self._lru.touch(key)
            self._metrics["read_hits"] += 1
            return len(str(record.value))

    async def incrby(self, key: str, amount: int) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += 1
            record = self._records.get(key)
            if record is None:
                record = Record("string", "0")
                self._records[key] = record
            if record.kind != "string":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            try:
                next_value = int(record.value) + amount
            except ValueError as exc:
                raise CommandError("ERR value is not an integer or out of range") from exc
            record.value = str(next_value)
            self._lru.touch(key)
            self._evict_if_needed_locked()
            return next_value

    async def zadd(self, key: str, pairs: list[tuple[float, str]]) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += len(pairs)
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
            self._metrics["total_reads"] += 1
            record = self._records.get(key)
            if record is None:
                self._metrics["read_misses"] += 1
                return []
            if record.kind != "zset":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            self._lru.touch(key)
            self._metrics["read_hits"] += 1
            return record.value.range(start, stop)

    async def zcard(self, key: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += 1
            record = self._records.get(key)
            if record is None:
                self._metrics["read_misses"] += 1
                return 0
            if record.kind != "zset":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            self._lru.touch(key)
            self._metrics["read_hits"] += 1
            return record.value.card()

    async def zscore(self, key: str, member: str) -> float | None:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_reads"] += 1
            record = self._records.get(key)
            if record is None:
                self._metrics["read_misses"] += 1
                return None
            if record.kind != "zset":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            self._lru.touch(key)
            score = record.value.score(member)
            if score is None:
                self._metrics["read_misses"] += 1
            else:
                self._metrics["read_hits"] += 1
            return score

    async def zrem(self, key: str, *members: str) -> int:
        async with self._lock:
            self._purge_expired_locked()
            self._metrics["total_writes"] += len(members)
            record = self._records.get(key)
            if record is None:
                return 0
            if record.kind != "zset":
                raise WrongTypeError("WRONGTYPE Operation against a key holding the wrong kind of value")
            removed = 0
            for member in members:
                removed += 1 if record.value.remove(member) else 0
            self._lru.touch(key)
            return removed

    async def export_state(self) -> dict[str, Any]:
        async with self._lock:
            self._purge_expired_locked()
            records: dict[str, dict[str, Any]] = {}
            for key, record in self._records.items():
                if record.kind == "string":
                    value: Any = str(record.value)
                elif record.kind == "zset":
                    value = [{"member": member, "score": score} for member, score in record.value.items()]
                else:
                    continue
                records[key] = {
                    "kind": record.kind,
                    "value": value,
                    "expires_at": record.expires_at,
                    "expiry_version": record.expiry_version,
                }
            self._metrics["snapshot_saves"] += 1
            return {"records": records}

    async def import_state(self, payload: dict[str, Any]) -> None:
        async with self._lock:
            self._records.clear()
            self._lru = LRUTracker()
            self._ttl_heap = TTLHeap()
            for key, raw in payload.get("records", {}).items():
                kind = raw["kind"]
                expires_at = raw.get("expires_at")
                expiry_version = raw.get("expiry_version", 0)
                if kind == "string":
                    record = Record(kind, raw["value"], expires_at, expiry_version)
                elif kind == "zset":
                    zset = SortedSet()
                    for item in raw.get("value", []):
                        zset.add(float(item["score"]), item["member"])
                    record = Record(kind, zset, expires_at, expiry_version)
                else:
                    continue
                if expires_at is not None and expires_at <= time.time():
                    continue
                self._records[key] = record
                self._lru.touch(key)
                if expires_at is not None:
                    self._ttl_heap.schedule(key, expires_at, expiry_version)
            self._metrics["snapshot_loads"] += 1

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
            self._metrics["expired_keys"] += 1
        return removed

    def _evict_if_needed_locked(self) -> None:
        while len(self._records) > self._max_keys:
            evicted_key = self._lru.pop_lru()
            if evicted_key is None:
                return
            self._records.pop(evicted_key, None)
            self._metrics["evicted_keys"] += 1
