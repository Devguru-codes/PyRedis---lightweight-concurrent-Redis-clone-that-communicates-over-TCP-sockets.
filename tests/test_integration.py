from __future__ import annotations

import asyncio

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
    assert ttl_response.startswith(b":")
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
