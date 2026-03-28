from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from pyredis.config import ServerConfig
from pyredis.server import PyRedisServer


@pytest.fixture
async def redis_server(tmp_path: Path):
    server = PyRedisServer(
        ServerConfig(
            host="127.0.0.1",
            port=0,
            max_keys=3,
            ttl_check_interval=0.05,
            snapshot_path=str(tmp_path / "dump.json"),
        )
    )
    await server.start()
    host, port = server.address
    try:
        yield host, port, server
    finally:
        await server.close()


@pytest.fixture
async def redis_client(redis_server):
    host, port, _server = redis_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        yield reader, writer
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.fixture
async def auth_server(tmp_path: Path):
    server = PyRedisServer(
        ServerConfig(
            host="127.0.0.1",
            port=0,
            max_keys=10,
            ttl_check_interval=0.05,
            require_password="secret",
            snapshot_path=str(tmp_path / "auth-dump.json"),
        )
    )
    await server.start()
    host, port = server.address
    try:
        yield host, port, server
    finally:
        await server.close()
