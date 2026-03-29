from __future__ import annotations


def resp_array(*values: str) -> bytes:
    parts = [f"*{len(values)}\r\n".encode()]
    for value in values:
        encoded = value.encode()
        parts.append(f"${len(encoded)}\r\n".encode())
        parts.append(encoded + b"\r\n")
    return b"".join(parts)


async def send_command(writer, reader, *values: str) -> bytes:
    writer.write(resp_array(*values))
    await writer.drain()
    return await _read_response(reader)


async def _read_response(reader) -> bytes:
    first = await reader.readuntil(b"\r\n")
    prefix = first[:1]
    if prefix in (b"+", b"-", b":"):
        return first
    if prefix == b"$":
        length = int(first[1:-2])
        if length == -1:
            return first
        payload = await reader.readexactly(length + 2)
        return first + payload
    if prefix == b"*":
        count = int(first[1:-2])
        chunks = [first]
        for _ in range(count):
            chunks.append(await _read_response(reader))
        return b"".join(chunks)
    raise AssertionError(f"Unknown RESP prefix: {prefix!r}")
