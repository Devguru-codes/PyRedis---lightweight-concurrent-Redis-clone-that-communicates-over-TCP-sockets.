"""Snapshot persistence helpers for PyRedis."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
import time
from typing import Any

from .datastore import DataStore


class SnapshotManager:
    def __init__(self, path: str) -> None:
        self.path = Path(path)
        self._save_task: asyncio.Task | None = None

    async def save(self, datastore: DataStore) -> None:
        payload = await datastore.export_state()
        await asyncio.to_thread(self._write_snapshot, payload)

    async def bgsave(self, datastore: DataStore) -> bool:
        if self._save_task is not None and not self._save_task.done():
            return False
        self._save_task = asyncio.create_task(self.save(datastore))
        return True

    async def wait(self) -> None:
        if self._save_task is not None:
            await self._save_task

    async def load(self, datastore: DataStore) -> bool:
        if not self.path.exists():
            return False
        payload = await asyncio.to_thread(self._read_snapshot)
        await datastore.import_state(payload)
        return True

    def _write_snapshot(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        temp_path.replace(self.path)

    def _read_snapshot(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))


class AppendOnlyManager:
    def __init__(self, path: str, *, fsync_always: bool = False) -> None:
        self.path = Path(path)
        self.fsync_always = fsync_always
        self._lock = asyncio.Lock()

    async def append(self, command_parts: list[str]) -> None:
        async with self._lock:
            await asyncio.to_thread(self._append_sync, command_parts)

    async def replay(self, datastore: DataStore) -> bool:
        if not self.path.exists():
            return False
        commands = await asyncio.to_thread(self._read_all)
        for command_parts in commands:
            await self._apply(datastore, command_parts)
        return True

    async def rewrite_from_snapshot(self, datastore: DataStore) -> None:
        payload = await datastore.export_state()
        commands: list[list[str]] = []
        for key, record in payload.get("records", {}).items():
            kind = record["kind"]
            if kind == "string":
                commands.append(["SET", key, str(record["value"])])
                if record.get("expires_at") is not None:
                    ttl_ms = int(max((record["expires_at"] - time.time()) * 1000, 1))
                    commands.append(["PEXPIRE", key, str(ttl_ms)])
            elif kind == "zset":
                zadd_command = ["ZADD", key]
                for item in record.get("value", []):
                    zadd_command.extend([str(item["score"]), item["member"]])
                if len(zadd_command) > 2:
                    commands.append(zadd_command)
        async with self._lock:
            await asyncio.to_thread(self._rewrite_sync, commands)

    def _append_sync(self, command_parts: list[str]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(command_parts, ensure_ascii=True)
        with self.path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(line)
            handle.write("\n")
            handle.flush()
            if self.fsync_always:
                import os

                os.fsync(handle.fileno())

    def _read_all(self) -> list[list[str]]:
        commands: list[list[str]] = []
        for line in self.path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            commands.append(json.loads(line))
        return commands

    def _rewrite_sync(self, commands: list[list[str]]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = self.path.with_suffix(f"{self.path.suffix}.tmp")
        with temp_path.open("w", encoding="utf-8", newline="\n") as handle:
            for command_parts in commands:
                handle.write(json.dumps(command_parts, ensure_ascii=True))
                handle.write("\n")
            handle.flush()
        temp_path.replace(self.path)

    async def _apply(self, datastore: DataStore, command_parts: list[str]) -> None:
        name = command_parts[0].upper()
        args = command_parts[1:]
        if name == "SET":
            ex = None
            if len(args) == 4 and args[2].upper() == "EX":
                ex = int(args[3])
                args = args[:2]
            await datastore.set(args[0], args[1], ex=ex)
        elif name == "DEL":
            await datastore.delete(*args)
        elif name == "EXPIRE":
            await datastore.expire(args[0], int(args[1]))
        elif name == "PEXPIRE":
            await datastore.pexpire(args[0], int(args[1]))
        elif name == "PERSIST":
            await datastore.persist(args[0])
        elif name == "INCR":
            await datastore.incr(args[0])
        elif name == "INCRBY":
            await datastore.incrby(args[0], int(args[1]))
        elif name == "DECR":
            await datastore.incrby(args[0], -1)
        elif name == "DECRBY":
            await datastore.incrby(args[0], -int(args[1]))
        elif name == "MSET":
            await datastore.mset([(args[index], args[index + 1]) for index in range(0, len(args), 2)])
        elif name == "SETNX":
            await datastore.setnx(args[0], args[1])
        elif name == "APPEND":
            await datastore.append(args[0], args[1])
        elif name == "GETSET":
            await datastore.getset(args[0], args[1])
        elif name == "FLUSHALL":
            await datastore.flushall()
        elif name == "ZADD":
            pairs = [(float(args[index]), args[index + 1]) for index in range(1, len(args), 2)]
            await datastore.zadd(args[0], pairs)
        elif name == "ZREM":
            await datastore.zrem(args[0], *args[1:])
        elif name == "RENAME":
            await datastore.rename(args[0], args[1])
