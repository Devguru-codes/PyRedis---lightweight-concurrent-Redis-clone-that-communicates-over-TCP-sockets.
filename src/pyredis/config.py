"""Configuration values for PyRedis."""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
import tomllib


@dataclass(slots=True)
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = 6380
    max_keys: int = 1024
    ttl_check_interval: float = 0.25
    max_command_parts: int = 128
    max_bulk_length: int = 1024 * 1024
    client_idle_timeout: float = 30.0
    require_password: str | None = None
    snapshot_path: str = "data/dump.json"
    snapshot_on_shutdown: bool = False
    load_snapshot_on_startup: bool = True
    snapshot_interval_seconds: float = 0.0
    appendonly_enabled: bool = False
    appendonly_path: str = "data/appendonly.aof"
    appendfsync_always: bool = False
    metrics_enabled: bool = False
    metrics_host: str = "127.0.0.1"
    metrics_port: int = 9101
    log_level: str = "INFO"


def load_config(config_path: str | None) -> ServerConfig:
    if not config_path:
        return ServerConfig()
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    server_data = data.get("server", data)
    valid_fields = {field.name for field in fields(ServerConfig)}
    filtered = {key: value for key, value in server_data.items() if key in valid_fields}
    return ServerConfig(**filtered)
