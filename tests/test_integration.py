from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from .helpers import send_command


@pytest.mark.asyncio
async def test_ping_and_set_get(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "PING") == b"+PONG\r\n"
    assert await send_command(writer, reader, "SET", "name", "pyredis") == b"+OK\r\n"
    assert await send_command(writer, reader, "GET", "name") == b"$7\r\npyredis\r\n"


@pytest.mark.asyncio
async def test_expire_and_ttl(redis_client):
    reader, writer = redis_client
    await send_command(writer, reader, "SET", "temp", "42")
    assert await send_command(writer, reader, "EXPIRE", "temp", "1") == b":1\r\n"
    ttl_response = await send_command(writer, reader, "TTL", "temp")
    ttl_value = int(ttl_response[1:-2])
    assert ttl_value in {0, 1}
    await asyncio.sleep(1.2)
    assert await send_command(writer, reader, "GET", "temp") == b"$-1\r\n"


@pytest.mark.asyncio
async def test_lru_eviction_over_socket(redis_client):
    reader, writer = redis_client
    await send_command(writer, reader, "SET", "a", "1")
    await send_command(writer, reader, "SET", "b", "2")
    await send_command(writer, reader, "GET", "a")
    await send_command(writer, reader, "SET", "c", "3")
    await send_command(writer, reader, "SET", "d", "4")
    assert await send_command(writer, reader, "GET", "b") == b"$-1\r\n"


@pytest.mark.asyncio
async def test_sorted_set_commands(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "ZADD", "leaders", "10", "alice", "5", "bob") == b":2\r\n"
    assert (
        await send_command(writer, reader, "ZRANGE", "leaders", "0", "-1")
        == b"*2\r\n$3\r\nbob\r\n$5\r\nalice\r\n"
    )


@pytest.mark.asyncio
async def test_mset_mget_dbsize_type_and_persist(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "MSET", "a", "1", "b", "2") == b"+OK\r\n"
    assert await send_command(writer, reader, "MGET", "a", "b", "missing") == (
        b"*3\r\n$1\r\n1\r\n$1\r\n2\r\n$-1\r\n"
    )
    assert await send_command(writer, reader, "DBSIZE") == b":2\r\n"
    assert await send_command(writer, reader, "TYPE", "a") == b"+STRING\r\n"
    await send_command(writer, reader, "SET", "temp", "9", "EX", "10")
    assert await send_command(writer, reader, "PERSIST", "temp") == b":1\r\n"
    assert await send_command(writer, reader, "TTL", "temp") == b":-1\r\n"


@pytest.mark.asyncio
async def test_extended_string_and_zset_commands(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "SETNX", "name", "py") == b":1\r\n"
    assert await send_command(writer, reader, "SETNX", "name", "ignored") == b":0\r\n"
    assert await send_command(writer, reader, "APPEND", "name", "redis") == b":7\r\n"
    assert await send_command(writer, reader, "STRLEN", "name") == b":7\r\n"
    assert await send_command(writer, reader, "INCRBY", "counter", "5") == b":5\r\n"
    assert await send_command(writer, reader, "DECR", "counter") == b":4\r\n"
    assert await send_command(writer, reader, "DECRBY", "counter", "3") == b":1\r\n"
    assert await send_command(writer, reader, "ZADD", "leaders", "1", "alice", "2", "bob") == b":2\r\n"
    assert await send_command(writer, reader, "ZCARD", "leaders") == b":2\r\n"
    assert await send_command(writer, reader, "ZSCORE", "leaders", "alice") == b"$1\r\n1\r\n"
    assert await send_command(writer, reader, "ZREM", "leaders", "alice", "missing") == b":1\r\n"
    assert await send_command(writer, reader, "ZCARD", "leaders") == b":1\r\n"


@pytest.mark.asyncio
async def test_additional_redis_commands(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "SET", "alpha", "1") == b"+OK\r\n"
    assert await send_command(writer, reader, "GETSET", "alpha", "2") == b"$1\r\n1\r\n"
    assert await send_command(writer, reader, "GET", "alpha") == b"$1\r\n2\r\n"
    assert await send_command(writer, reader, "PEXPIRE", "alpha", "1500") == b":1\r\n"
    pttl_response = await send_command(writer, reader, "PTTL", "alpha")
    assert int(pttl_response[1:-2]) > 0
    assert await send_command(writer, reader, "RENAME", "alpha", "beta") == b"+OK\r\n"
    assert await send_command(writer, reader, "KEYS", "b*") == b"*1\r\n$4\r\nbeta\r\n"
    assert await send_command(writer, reader, "RENAMENX", "beta", "gamma") == b":1\r\n"
    assert await send_command(writer, reader, "UNLINK", "gamma") == b":1\r\n"


@pytest.mark.asyncio
async def test_scan_zrank_and_zrange_withscores(redis_client):
    reader, writer = redis_client
    await send_command(writer, reader, "MSET", "item:1", "a", "item:2", "b", "note", "c")
    scan_response = await send_command(writer, reader, "SCAN", "0", "MATCH", "item:*", "COUNT", "10")
    assert scan_response.startswith(b"*2\r\n")
    assert b"item:1" in scan_response and b"item:2" in scan_response
    await send_command(writer, reader, "ZADD", "leaders", "1", "alice", "2", "bob")
    assert await send_command(writer, reader, "ZRANK", "leaders", "bob") == b"$1\r\n1\r\n"
    assert (
        await send_command(writer, reader, "ZRANGE", "leaders", "0", "-1", "WITHSCORES")
        == b"*4\r\n$5\r\nalice\r\n$1\r\n1\r\n$3\r\nbob\r\n$1\r\n2\r\n"
    )


@pytest.mark.asyncio
async def test_wrong_type_and_invalid_argument_errors(redis_client):
    reader, writer = redis_client
    await send_command(writer, reader, "ZADD", "leaders", "1", "alice")
    assert (
        await send_command(writer, reader, "GET", "leaders")
        == b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
    )
    assert await send_command(writer, reader, "EXPIRE", "leaders", "abc") == (
        b"-ERR invalid expire time in 'EXPIRE'\r\n"
    )
    assert await send_command(writer, reader, "ZADD", "leaders", "nanx", "bob") == (
        b"-ERR value for 'ZADD' must be a float\r\n"
    )


@pytest.mark.asyncio
async def test_info_reports_metrics(redis_client):
    reader, writer = redis_client
    await send_command(writer, reader, "PING")
    await send_command(writer, reader, "BOGUS")
    payload = await send_command(writer, reader, "INFO")
    assert b"commands_processed:" in payload
    assert b"command_errors:" in payload
    assert b"active_connections:" in payload
    assert b"total_connections:" in payload
    assert b"command_errors:1" in payload
    assert b"total_reads:" in payload
    assert b"total_writes:" in payload
    assert b"read_hits:" in payload
    assert b"read_misses:" in payload


@pytest.mark.asyncio
async def test_protocol_error_closes_connection(redis_server):
    host, port, _server = redis_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(b"*200\r\n")
        await writer.drain()
        response = await reader.readuntil(b"\r\n")
        assert response == b"-ERR protocol error: too many command arguments\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_inline_command_support(redis_server):
    host, port, _server = redis_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        writer.write(b"PING inline\r\n")
        await writer.drain()
        assert await reader.readuntil(b"\r\n") == b"$6\r\n"
        assert await reader.readexactly(8) == b"inline\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_unknown_command_returns_error(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "DOESNOTEXIST") == b"-ERR unknown command 'DOESNOTEXIST'\r\n"


@pytest.mark.asyncio
async def test_persist_missing_and_type_none(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "PERSIST", "missing") == b":0\r\n"
    assert await send_command(writer, reader, "TYPE", "missing") == b"+NONE\r\n"


@pytest.mark.asyncio
async def test_auth_required(auth_server):
    host, port, _server = auth_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        assert await send_command(writer, reader, "GET", "name") == b"-NOAUTH Authentication required\r\n"
        assert await send_command(writer, reader, "PING", "hello") == b"$5\r\nhello\r\n"
        assert await send_command(writer, reader, "AUTH", "wrong") == b"-ERR invalid password\r\n"
        assert await send_command(writer, reader, "AUTH", "secret") == b"+OK\r\n"
        assert await send_command(writer, reader, "SET", "name", "secure") == b"+OK\r\n"
        assert await send_command(writer, reader, "GET", "name") == b"$6\r\nsecure\r\n"
    finally:
        writer.close()
        await writer.wait_closed()


@pytest.mark.asyncio
async def test_save_and_reload_snapshot(redis_server, tmp_path: Path):
    host, port, server = redis_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        await send_command(writer, reader, "SET", "persisted", "value")
        await send_command(writer, reader, "SET", "ttl-key", "temp", "EX", "60")
        await send_command(writer, reader, "ZADD", "leaders", "1", "alice")
        assert await send_command(writer, reader, "SAVE") == b"+OK\r\n"
    finally:
        writer.close()
        await writer.wait_closed()
        await server.close()

    snapshot_path = tmp_path / "dump.json"
    snapshot_data = json.loads(snapshot_path.read_text(encoding="utf-8"))
    assert snapshot_data["records"]["persisted"]["value"] == "value"
    assert snapshot_data["records"]["leaders"]["kind"] == "zset"
    assert snapshot_data["records"]["ttl-key"]["expires_at"] is not None

    from pyredis.config import ServerConfig
    from pyredis.server import PyRedisServer

    reloaded = PyRedisServer(
        ServerConfig(
            host="127.0.0.1",
            port=0,
            max_keys=10,
            ttl_check_interval=0.05,
            snapshot_path=str(tmp_path / "dump.json"),
            load_snapshot_on_startup=True,
        )
    )
    await reloaded.start()
    host2, port2 = reloaded.address
    reader2, writer2 = await asyncio.open_connection(host2, port2)
    try:
        assert await send_command(writer2, reader2, "GET", "persisted") == b"$5\r\nvalue\r\n"
        ttl_response = await send_command(writer2, reader2, "TTL", "ttl-key")
        assert int(ttl_response[1:-2]) >= 0
        assert await send_command(writer2, reader2, "ZRANGE", "leaders", "0", "-1") == b"*1\r\n$5\r\nalice\r\n"
    finally:
        writer2.close()
        await writer2.wait_closed()
        await reloaded.close()


@pytest.mark.asyncio
async def test_multi_exec_and_discard(redis_client):
    reader, writer = redis_client
    assert await send_command(writer, reader, "MULTI") == b"+OK\r\n"
    assert await send_command(writer, reader, "SET", "trans:key", "1") == b"+QUEUED\r\n"
    assert await send_command(writer, reader, "INCRBY", "trans:key", "4") == b"+QUEUED\r\n"
    assert (
        await send_command(writer, reader, "EXEC")
        == b"*2\r\n+OK\r\n:5\r\n"
    )
    assert await send_command(writer, reader, "GET", "trans:key") == b"$1\r\n5\r\n"

    assert await send_command(writer, reader, "MULTI") == b"+OK\r\n"
    assert await send_command(writer, reader, "SET", "trans:key", "9") == b"+QUEUED\r\n"
    assert await send_command(writer, reader, "DISCARD") == b"+OK\r\n"
    assert await send_command(writer, reader, "GET", "trans:key") == b"$1\r\n5\r\n"


@pytest.mark.asyncio
async def test_append_only_replay_and_metrics_endpoint(durable_server, tmp_path: Path):
    host, port, metrics_host, metrics_port, server = durable_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        assert await send_command(writer, reader, "SET", "persist:aof", "value") == b"+OK\r\n"
        assert await send_command(writer, reader, "MULTI") == b"+OK\r\n"
        assert await send_command(writer, reader, "INCRBY", "txn", "2") == b"+QUEUED\r\n"
        assert await send_command(writer, reader, "EXEC") == b"*1\r\n:2\r\n"
        assert await send_command(writer, reader, "BGSAVE") == b"+Background saving started\r\n"
        await asyncio.sleep(0.3)
    finally:
        writer.close()
        await writer.wait_closed()
        await server.close()

    from pyredis.config import ServerConfig
    from pyredis.server import PyRedisServer

    reloaded = PyRedisServer(
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
        )
    )
    await reloaded.start()
    host2, port2 = reloaded.address
    metrics_host2, metrics_port2 = reloaded.metrics_address
    reader2, writer2 = await asyncio.open_connection(host2, port2)
    metrics_reader, metrics_writer = await asyncio.open_connection(metrics_host2, metrics_port2)
    try:
        assert await send_command(writer2, reader2, "GET", "persist:aof") == b"$5\r\nvalue\r\n"
        assert await send_command(writer2, reader2, "GET", "txn") == b"$1\r\n2\r\n"
        metrics_writer.write(b"GET /metrics HTTP/1.1\r\nHost: localhost\r\n\r\n")
        await metrics_writer.drain()
        metrics_payload = await metrics_reader.read()
        assert b"pyredis_commands_processed" in metrics_payload
        assert b'pyredis_command_count{command="get"}' in metrics_payload
        assert b'pyredis_command_latency_us_bucket{command="get",le="100"}' in metrics_payload
    finally:
        writer2.close()
        metrics_writer.close()
        await writer2.wait_closed()
        await metrics_writer.wait_closed()
        await reloaded.close()


@pytest.mark.asyncio
async def test_concurrent_clients(redis_server):
    host, port, _server = redis_server

    async def worker(index: int) -> bytes:
        reader, writer = await asyncio.open_connection(host, port)
        try:
            await send_command(writer, reader, "SET", f"worker:{index}", str(index))
            return await send_command(writer, reader, "GET", f"worker:{index}")
        finally:
            writer.close()
            await writer.wait_closed()

    results = await asyncio.gather(*(worker(index) for index in range(3)))
    assert results == [b"$1\r\n0\r\n", b"$1\r\n1\r\n", b"$1\r\n2\r\n"]


@pytest.mark.asyncio
async def test_pipelined_requests(redis_server):
    host, port, _server = redis_server
    reader, writer = await asyncio.open_connection(host, port)
    try:
        payload = (
            b"*3\r\n$3\r\nSET\r\n$4\r\npipe\r\n$1\r\n1\r\n"
            b"*2\r\n$3\r\nGET\r\n$4\r\npipe\r\n"
            b"*3\r\n$6\r\nINCRBY\r\n$4\r\npipe\r\n$1\r\n4\r\n"
            b"*2\r\n$3\r\nGET\r\n$4\r\npipe\r\n"
        )
        writer.write(payload)
        await writer.drain()
        assert await reader.readuntil(b"\r\n") == b"+OK\r\n"
        assert await reader.readuntil(b"\r\n") == b"$1\r\n"
        assert await reader.readexactly(3) == b"1\r\n"
        assert await reader.readuntil(b"\r\n") == b":5\r\n"
        assert await reader.readuntil(b"\r\n") == b"$1\r\n"
        assert await reader.readexactly(3) == b"5\r\n"
    finally:
        writer.close()
        await writer.wait_closed()
