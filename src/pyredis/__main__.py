"""CLI entrypoint for PyRedis."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
from dataclasses import replace
import json

from .config import load_config, validate_config
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
    parser.add_argument("--log-format", default=None)
    return parser.parse_args()


class JsonLogFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        return json.dumps(payload, ensure_ascii=True)


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
        "log_format": args.log_format,
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
    config = validate_config(config)
    handler = logging.StreamHandler()
    if config.log_format.lower() == "json":
        handler.setFormatter(JsonLogFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(levelname)s %(name)s %(message)s"))
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
    server = PyRedisServer(replace(config))
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        stop_event.set()

    for sig in (getattr(signal, "SIGINT", None), getattr(signal, "SIGTERM", None)):
        if sig is None:
            continue
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown)
    try:
        await server.start()
        await stop_event.wait()
    finally:
        await server.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
