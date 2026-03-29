"""CLI entrypoint for PyRedis."""

from __future__ import annotations

import argparse
import asyncio
import logging
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
    parser.add_argument("--snapshot-interval-seconds", type=float, default=None)
    parser.add_argument("--appendonly-enabled", action="store_true")
    parser.add_argument("--appendonly-path", default=None)
    parser.add_argument("--appendfsync-always", action="store_true")
    parser.add_argument("--metrics-enabled", action="store_true")
    parser.add_argument("--metrics-host", default=None)
    parser.add_argument("--metrics-port", type=int, default=None)
    parser.add_argument("--log-level", default=None)
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
        "snapshot_interval_seconds": args.snapshot_interval_seconds,
        "appendonly_path": args.appendonly_path,
        "metrics_host": args.metrics_host,
        "metrics_port": args.metrics_port,
        "log_level": args.log_level,
    }
    for key, value in overrides.items():
        if value is not None:
            setattr(config, key, value)
    if args.snapshot_on_shutdown:
        config.snapshot_on_shutdown = True
    if args.disable_snapshot_load:
        config.load_snapshot_on_startup = False
    if args.appendonly_enabled:
        config.appendonly_enabled = True
    if args.appendfsync_always:
        config.appendfsync_always = True
    if args.metrics_enabled:
        config.metrics_enabled = True
    logging.basicConfig(level=getattr(logging, config.log_level.upper(), logging.INFO))
    server = PyRedisServer(replace(config))
    try:
        await server.serve_forever()
    finally:
        await server.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
