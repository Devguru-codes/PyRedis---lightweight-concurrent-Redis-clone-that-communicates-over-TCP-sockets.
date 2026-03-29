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


@pytest.fixture
async def durable_server(tmp_path: Path):
    server = PyRedisServer(
        ServerConfig(
            host="127.0.0.1",
            port=0,
            max_keys=10,
            ttl_check_interval=0.05,
            snapshot_path=str(tmp_path / "durable-dump.json"),
            appendonly_enabled=True,
            appendonly_path=str(tmp_path / "appendonly.aof"),
            metrics_enabled=True,
            metrics_host="127.0.0.1",
            metrics_port=0,
            snapshot_interval_seconds=0.2,
        )
    )
    await server.start()
    host, port = server.address
    metrics_host, metrics_port = server.metrics_address
    try:
        yield host, port, metrics_host, metrics_port, server
    finally:
        await server.close()
