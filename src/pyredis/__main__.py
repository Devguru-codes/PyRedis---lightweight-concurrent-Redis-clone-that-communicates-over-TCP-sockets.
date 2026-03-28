"""CLI entrypoint for PyRedis."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace

from .config import load_config
from .server import PyRedisServer


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PyRedis server")
    parser.add_argument("--config", default=None)
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--max-keys", type=int, default=None)
    parser.add_argument("--ttl-check-interval", type=float, default=None)
    parser.add_argument("--require-password", default=None)
    parser.add_argument("--snapshot-path", default=None)
    parser.add_argument("--snapshot-on-shutdown", action="store_true")
    parser.add_argument("--disable-snapshot-load", action="store_true")
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    config = load_config(args.config)
    overrides = {
        "host": args.host,
        "port": args.port,
        "max_keys": args.max_keys,
        "ttl_check_interval": args.ttl_check_interval,
        "require_password": args.require_password,
        "snapshot_path": args.snapshot_path,
    }
    for key, value in overrides.items():
        if value is not None:
            setattr(config, key, value)
    if args.snapshot_on_shutdown:
        config.snapshot_on_shutdown = True
    if args.disable_snapshot_load:
        config.load_snapshot_on_startup = False
    server = PyRedisServer(replace(config))
    try:
        await server.serve_forever()
    finally:
        await server.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
