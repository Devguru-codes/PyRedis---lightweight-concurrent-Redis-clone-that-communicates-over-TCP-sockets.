from __future__ import annotations

import asyncio
import time
from pathlib import Path

import pytest

from pyredis.datastore import DataStore
from pyredis.persistence import AppendOnlyManager, SnapshotManager


@pytest.mark.asyncio
async def test_aof_rewrite_preserves_writes_during_compaction(tmp_path: Path, monkeypatch):
    store = DataStore(max_keys=20)
    manager = AppendOnlyManager(str(tmp_path / "appendonly.aof"))
    await store.set("base", "1")
    await manager.append(["SET", "base", "1"])

    original_rewrite = manager._rewrite_sync

    def slow_rewrite(commands: list[list[str]]) -> None:
        time.sleep(0.05)
        original_rewrite(commands)

    monkeypatch.setattr(manager, "_rewrite_sync", slow_rewrite)

    rewrite_task = asyncio.create_task(manager.rewrite_from_snapshot(store))
    await asyncio.sleep(0.01)
    await store.set("late", "2")
    await manager.append(["SET", "late", "2"])
    await rewrite_task

    restored = DataStore(max_keys=20)
    assert await manager.replay(restored) is True
    assert await restored.get("base") == "1"
    assert await restored.get("late") == "2"


@pytest.mark.asyncio
async def test_bgsave_rejects_overlap(tmp_path: Path, monkeypatch):
    store = DataStore(max_keys=10)
    await store.set("name", "pyredis")
    manager = SnapshotManager(str(tmp_path / "dump.json"))

    original_write = manager._write_snapshot

    def slow_write(payload):
        time.sleep(0.05)
        original_write(payload)

    monkeypatch.setattr(manager, "_write_snapshot", slow_write)

    assert await manager.bgsave(store) is True
    assert await manager.bgsave(store) is False
    await manager.wait()
