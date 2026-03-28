"""Snapshot persistence helpers for PyRedis."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from .datastore import DataStore


class SnapshotManager:
    def __init__(self, path: str) -> None:
        self.path = Path(path)

    async def save(self, datastore: DataStore) -> None:
        payload = await datastore.export_state()
        await asyncio.to_thread(self._write_snapshot, payload)

    async def load(self, datastore: DataStore) -> bool:
        if not self.path.exists():
            return False
        payload = await asyncio.to_thread(self._read_snapshot)
        await datastore.import_state(payload)
        return True

    def _write_snapshot(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    def _read_snapshot(self) -> dict[str, Any]:
        return json.loads(self.path.read_text(encoding="utf-8"))
