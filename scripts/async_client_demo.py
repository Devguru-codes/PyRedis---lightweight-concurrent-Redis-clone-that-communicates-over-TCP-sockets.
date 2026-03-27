from __future__ import annotations

import argparse
import asyncio


def resp_array(*values: str) -> bytes:
    chunks = [f"*{len(values)}\r\n".encode()]
    for value in values:
        encoded = value.encode()
        chunks.append(f"${len(encoded)}\r\n".encode())
        chunks.append(encoded + b"\r\n")
    return b"".join(chunks)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    args = parser.parse_args()

    reader, writer = await asyncio.open_connection(args.host, args.port)
    for command in [
        ("PING",),
        ("SET", "demo", "hello"),
        ("GET", "demo"),
    ]:
        writer.write(resp_array(*command))
        await writer.drain()
        response = await reader.readuntil(b"\r\n")
        if response.startswith(b"$") and response != b"$-1\r\n":
            length = int(response[1:-2])
            response += await reader.readexactly(length + 2)
        print(command, response.decode().rstrip())
    writer.close()
    await writer.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())

