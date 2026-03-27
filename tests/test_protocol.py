from __future__ import annotations

import asyncio

import pytest

from pyredis.errors import ProtocolError
from pyredis.protocol import read_command


async def _make_reader(payload: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(payload)
    reader.feed_eof()
    return reader


@pytest.mark.asyncio
async def test_read_resp_array_command():
    reader = await _make_reader(b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n")
    command = await read_command(reader, max_command_parts=8, max_bulk_length=64)
    assert command == ["PING", "hello"]


@pytest.mark.asyncio
async def test_read_inline_command():
    reader = await _make_reader(b"SET demo value\r\n")
    command = await read_command(reader, max_command_parts=8, max_bulk_length=64)
    assert command == ["SET", "demo", "value"]


@pytest.mark.asyncio
async def test_reject_too_many_command_parts():
    reader = await _make_reader(b"*3\r\n$4\r\nPING\r\n$1\r\na\r\n$1\r\nb\r\n")
    with pytest.raises(ProtocolError, match="too many command arguments"):
        await read_command(reader, max_command_parts=2, max_bulk_length=64)


@pytest.mark.asyncio
async def test_reject_null_bulk_string():
    reader = await _make_reader(b"*2\r\n$4\r\nPING\r\n$-1\r\n")
    with pytest.raises(ProtocolError, match="null bulk strings are not supported"):
        await read_command(reader, max_command_parts=8, max_bulk_length=64)


@pytest.mark.asyncio
async def test_reject_bulk_string_over_limit():
    reader = await _make_reader(b"*1\r\n$5\r\nhello\r\n")
    with pytest.raises(ProtocolError, match="bulk string exceeds configured maximum size"):
        await read_command(reader, max_command_parts=8, max_bulk_length=4)


@pytest.mark.asyncio
async def test_reject_invalid_utf8_inline_command():
    reader = await _make_reader(b"\xff\xfe\r\n")
    with pytest.raises(ProtocolError, match="inline command is not valid UTF-8"):
        await read_command(reader, max_command_parts=8, max_bulk_length=64)

