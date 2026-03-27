"""CLI entrypoint for PyRedis."""

from __future__ import annotations

import argparse
import asyncio

from .config import ServerConfig
from .server import PyRedisServer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PyRedis server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6380)
    parser.add_argument("--max-keys", type=int, default=1024)
    parser.add_argument("--ttl-check-interval", type=float, default=0.25)
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    server = PyRedisServer(
        ServerConfig(
            host=args.host,
            port=args.port,
            max_keys=args.max_keys,
            ttl_check_interval=args.ttl_check_interval,
        )
    )
    try:
        await server.serve_forever()
    finally:
        await server.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
