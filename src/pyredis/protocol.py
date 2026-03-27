"""RESP parsing and serialization helpers."""

from __future__ import annotations

from collections.abc import Sequence

from .errors import ProtocolError


async def read_command(reader, *, max_command_parts: int, max_bulk_length: int) -> list[str]:
    first = await reader.readline()
    if not first:
        raise EOFError
    if first.startswith(b"*"):
        count = _parse_int(first[1:].strip())
        if count <= 0:
            raise ProtocolError("empty array is not a valid command")
        if count > max_command_parts:
            raise ProtocolError("too many command arguments")
        values: list[str] = []
        for _ in range(count):
            bulk_header = await reader.readline()
            if not bulk_header.startswith(b"$"):
                raise ProtocolError("expected bulk string")
            length = _parse_int(bulk_header[1:].strip())
            if length < 0:
                raise ProtocolError("null bulk strings are not supported in commands")
            if length > max_bulk_length:
                raise ProtocolError("bulk string exceeds configured maximum size")
            data = await reader.readexactly(length)
            trailer = await reader.readexactly(2)
            if trailer != b"\r\n":
                raise ProtocolError("invalid bulk string terminator")
            try:
                values.append(data.decode("utf-8"))
            except UnicodeDecodeError as exc:
                raise ProtocolError("command payload is not valid UTF-8") from exc
        return values
    try:
        values = first.decode("utf-8").strip().split()
    except UnicodeDecodeError as exc:
        raise ProtocolError("inline command is not valid UTF-8") from exc
    if not values:
        raise ProtocolError("empty inline command")
    if len(values) > max_command_parts:
        raise ProtocolError("too many command arguments")
    return values


def encode_simple(message: str) -> bytes:
    return f"+{message}\r\n".encode("utf-8")


def encode_error(message: str) -> bytes:
    return f"-{message}\r\n".encode("utf-8")


def encode_integer(value: int) -> bytes:
    return f":{value}\r\n".encode("utf-8")


def encode_bulk(value: str | None) -> bytes:
    if value is None:
        return b"$-1\r\n"
    payload = value.encode("utf-8")
    return f"${len(payload)}\r\n".encode("utf-8") + payload + b"\r\n"


def encode_array(values: Sequence[str]) -> bytes:
    parts = [f"*{len(values)}\r\n".encode("utf-8")]
    for value in values:
        parts.append(encode_bulk(value))
    return b"".join(parts)


def _parse_int(data: bytes) -> int:
    try:
        return int(data.decode("utf-8"))
    except ValueError as exc:
        raise ProtocolError("invalid integer") from exc
